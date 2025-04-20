from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
import logging
from dataclasses import asdict
from app.db.database import get_db, get_dw_db
from app.db.dw_connection import get_dw_session
from app.services.chat_service import ChatService
from app.services.schema_service import SchemaService
from app.rag.dw_context_service import DWContextService
from app.schemas.chat import ChatRequest, ChatResponse
from app.core.config import settings
from app.db.schema_manager import SchemaManager
from app.llm.openai_adapter import OpenAIAdapter
from app.rag.debug_service import DebugStep, DebugService
import traceback
import sys
import os
import json
import asyncio
from fastapi.responses import JSONResponse
import uuid
from datetime import datetime, date
import decimal
from typing import Optional, Dict, Any
from fastapi import APIRouter

# Ensure log directory exists
log_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, 'app.log')

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Add file handler
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Get logger
logger = logging.getLogger(__name__)
logger.addHandler(file_handler)

# Initialize FastAPI app
app = FastAPI(
    title="Tourism Analytics API",
    description="API for tourism analytics and insights",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global chat service instance
chat_service = None
debug_service = None


@app.on_event("startup")
async def startup_event():
    global chat_service, debug_service
    try:
        logger.info("Starting up chat service...")

        # Get database session for initialization
        dw_db = next(get_dw_db())
        logger.info("Database session created")

        # Initialize schema service (fetches live schema)
        schema_service = SchemaService()
        logger.info("Schema service initialized")

        # Initialize DW context service
        dw_context_service = DWContextService(dw_db=dw_db)
        logger.info("DW context service initialized")

        # Initialize OpenAI adapter
        llm_adapter = OpenAIAdapter()
        logger.info("OpenAI adapter initialized")

        # Initialize debug service
        debug_service = DebugService()
        logger.info("Debug service initialized")

        # Initialize chat service with the updated constructor signature
        chat_service = ChatService(
            schema_service=schema_service,
            dw_context_service=dw_context_service,
            llm_adapter=llm_adapter
        )

        logger.info("Chat service initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing chat service: {str(e)}")
        logger.error(traceback.format_exc())
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global chat_service
    if chat_service:
        try:
            if hasattr(chat_service, 'close'):
                chat_service.close()
                logger.info("Chat service closed")
            else:
                logger.warning("Chat service does not have a close method")
        except Exception as e:
            logger.error(f"Error closing chat service: {str(e)}")
            logger.error(traceback.format_exc())

# Dependency functions for FastAPI


def get_chat_service() -> ChatService:
    """Dependency that returns the global chat service instance."""
    if chat_service is None:
        logger.error("Chat service not initialized")
        raise HTTPException(
            status_code=503,
            detail="Chat service not initialized")
    return chat_service


def get_debug_service() -> DebugService:
    """Dependency that returns the global debug service instance."""
    if debug_service is None:
        logger.error("Debug service not initialized")
        raise HTTPException(status_code=503,
                            detail="Debug service not initialized")
    return debug_service


def prepare_debug_info(debug_info):
    """Prepare debug info for JSON serialization by converting DebugStep objects to dictionaries."""
    if not debug_info:
        return debug_info

    # If there's a steps key, ensure all steps are dictionaries, not DebugStep
    # objects
    if "steps" in debug_info:
        # Handle case where steps is a single DebugStep object
        if isinstance(debug_info["steps"], DebugStep):
            debug_info["steps"] = [asdict(debug_info["steps"])]
        # Handle case where steps is a list of objects
        elif isinstance(debug_info["steps"], list):
            # Convert each step to a dictionary if it's not already
            for i, step in enumerate(debug_info["steps"]):
                if isinstance(step, DebugStep):
                    # If the step is a DebugStep object, convert it to dict
                    # using asdict
                    debug_info["steps"][i] = asdict(step)

    return debug_info


@app.post("/chat/stream")
async def stream_chat(
    request: ChatRequest,
    dw_db: Session = Depends(get_dw_db),
    chat_service: ChatService = Depends(get_chat_service),
    debug_service: DebugService = Depends(get_debug_service)
) -> StreamingResponse:
    """Stream chat responses"""
    async def generate():
        try:
            # Start with a simple message
            yield "data: " + json.dumps({"type": "start"}) + "\n\n"
            yield "data: " + json.dumps({"type": "content", "content": "Processing your request..."}) + "\n\n"

            # Process the actual chat stream
            async for chunk in chat_service.process_chat_stream(
                message=request.message,
                session_id=request.session_id,
                is_direct_query=getattr(request, 'is_direct_query', False),
                dw_db=dw_db
            ):
                yield "data: " + json.dumps(chunk) + "\n\n"

        except Exception as e:
            logger.error(f"Error in stream generation: {str(e)}")
            logger.error(traceback.format_exc())
            yield "data: " + json.dumps({"type": "error", "error": str(e)}) + "\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/chat")
async def chat(
    request: ChatRequest,
    dw_db: Session = Depends(get_dw_db)
) -> JSONResponse:
    """Process chat messages"""
    try:
        logger.info(f"Received chat request: {request.message}")

        if not chat_service:
            logger.error("Chat service not initialized")
            raise HTTPException(
                status_code=503,
                detail="Chat service not initialized")

        # For now, create a simplified response with just the message
        response = {
            "message_id": str(
                uuid.uuid4()),
            "message": request.message,
            "content": "This is a simplified response while we debug the issue.",
            "status": "success"}

        return JSONResponse(content=response)

    except Exception as e:
        logger.error(f"Error processing chat request: {str(e)}")
        logger.error(traceback.format_exc())
        error_response = {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "status": "error"
        }
        return JSONResponse(content=error_response, status_code=500)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Welcome to Tourism Analytics API"}


@app.get("/test")
async def test():
    return {"message": "Test endpoint working"}


@app.get("/test-app")
async def test_app():
    return {"message": "App endpoint working"}


@app.get("/api/v1/health")
async def health():
    return {"status": "ok"}


@app.get("/health")
async def root_health_check():
    """Root health check endpoint"""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
