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
from app.routers.test_router import router as test_router
from app.routers.analysis import router as analysis
from app.routers.visualization_router import router as visualization_router
from app.services.visualization_service import VisualizationService
from app.services.response_generation_service import ResponseGenerationService
from app.services.sql_generation_service import SQLGenerationService
from decimal import Decimal
import time

# Helper for JSON serialization of Decimal and Datetime
def custom_json_serializer(obj):
    """Convert Decimal, date, and datetime objects for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # Let the default json encoder handle other types or raise errors
    try:
        return json.JSONEncoder.default(None, obj) 
    except TypeError:
         raise TypeError(f"Type {type(obj)} not serializable")

# Custom JSON encoder to handle datetime objects
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        # Handle any other items that might not be JSON serializable
        try:
            return super().default(obj)
        except TypeError:
            # Fallback for any other non-serializable objects
            return str(obj)

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

        # Initialize DW context service with a direct database session, not a generator
        dw_context_service = DWContextService(dw_db=dw_db)
        logger.info("DW context service initialized")

        # Initialize OpenAI adapter
        llm_adapter = OpenAIAdapter()
        logger.info("OpenAI adapter initialized")

        # Initialize debug service
        debug_service = DebugService()
        logger.info("Debug service initialized")

        # Initialize visualization service
        visualization_service = VisualizationService(debug_service=debug_service, db=get_dw_session())
        logger.info("Visualization service initialized")

        # Initialize response generation service
        response_generation_service = ResponseGenerationService(llm_adapter=llm_adapter, debug_service=debug_service)
        logger.info("Response generation service initialized")

        # Initialize SQL generation service
        sql_generation_service = SQLGenerationService(llm_adapter=llm_adapter, debug_service=debug_service)
        logger.info("SQL generation service initialized")

        # Import the SQLExecutionService to avoid circular imports
        from app.services.sql_execution_service import SQLExecutionService
        sql_execution_service = SQLExecutionService(debug_service=debug_service)
        logger.info("SQL execution service initialized")

        # Initialize chat service with the updated constructor signature
        chat_service = ChatService(
            schema_service=schema_service,
            dw_context_service=dw_context_service,
            llm_adapter=llm_adapter,
            sql_execution_service=sql_execution_service,
            visualization_service=visualization_service,
            response_generation_service=response_generation_service,
            debug_service=debug_service
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
async def stream_chat(request: ChatRequest, 
                      chat_service: ChatService = Depends(get_chat_service), 
                      db: Session = Depends(get_dw_db)) -> StreamingResponse:
    """Handles streaming chat requests."""
    
    async def generate():
        start_time = time.time()
        logger.info(f"Received streaming chat request: {request.message}")
        try:
            async for chunk in chat_service.process_chat_stream(
                    message=request.message, 
                    session_id=request.session_id or "default_session",
                    is_direct_query=request.is_direct_query,
                    dw_db=db):
                try:
                    # Use the new custom serializer
                    yield "data: " + json.dumps(chunk, default=custom_json_serializer) + "\n\n"
                except TypeError as json_err:
                    logger.error(f"JSON serialization error for chunk: {json_err} - Chunk: {chunk}")
                    # Yield an error chunk if serialization fails
                    error_chunk = {
                        "type": "error",
                        "content": f"Serialization error: {str(json_err)}",
                        "message_id": chunk.get("message_id", "N/A") 
                    }
                    yield "data: " + json.dumps(error_chunk) + "\n\n"
        except Exception as e:
            logger.error(f"Error in stream generation: {str(e)}", exc_info=True)
            # Yield a final error chunk if the stream fails
            error_chunk = {
                "type": "error",
                "content": f"Stream error: {str(e)}",
                "message_id": "N/A" # Cannot get message_id if stream fails early
            }
            try:
                yield "data: " + json.dumps(error_chunk) + "\n\n"
            except Exception as final_err:
                 logger.error(f"Failed to yield final error chunk: {final_err}")
        finally:
            end_time = time.time()
            logger.info(f"Stream ended for request: {request.message}. Duration: {end_time - start_time:.2f}s")
            # Ensure the generator finishes properly
            yield "event: end\ndata: {}\n\n"

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no" # Useful for Nginx buffering issues
    }
    return StreamingResponse(generate(), media_type="text/event-stream", headers=headers)


@app.post("/chat")
async def chat(
    request: ChatRequest,
    dw_db: Session = Depends(get_dw_db),
    chat_service: ChatService = Depends(get_chat_service),
    debug_service: DebugService = Depends(get_debug_service)
) -> JSONResponse:
    """Process chat messages"""
    try:
        logger.info(f"Received chat request: {request.message}")

        if not chat_service:
            logger.error("Chat service not initialized")
            raise HTTPException(
                status_code=503,
                detail="Chat service not initialized")

        # Process the chat message using the chat service
        try:
            # Start debug tracking
            if debug_service:
                # Pass the session_id to debug_service.start_flow()
                debug_service.start_flow(session_id=request.session_id)
                debug_service.start_step("process_chat")

            # Process the message
            result = await chat_service.process_chat(
                message=request.message,
                session_id=request.session_id,  # Pass session_id to process_chat
                debug_service=debug_service,
                dw_db=dw_db
            )
            
            # Return the response with custom JSON encoder
            serialized_content = json.dumps(result, cls=CustomJSONEncoder)
            return JSONResponse(
                content=json.loads(serialized_content), 
                status_code=200,
                media_type="application/json",
            )
            
        except Exception as e:
            logger.error(f"Error processing chat: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Provide a friendly error response
            error_response = {
                "message_id": str(uuid.uuid4()),
                "message": request.message,
                "content": "I encountered an error while processing your request. Please try again.",
                "status": "error",
                "error": str(e)
            }
            serialized_error = json.dumps(error_response, cls=CustomJSONEncoder)
            return JSONResponse(
                content=json.loads(serialized_error)
            )

    except Exception as e:
        logger.error(f"Error processing chat request: {str(e)}")
        logger.error(traceback.format_exc())
        error_response = {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "status": "error"
        }
        serialized_error = json.dumps(error_response, cls=CustomJSONEncoder)
        return JSONResponse(
            content=json.loads(serialized_error), 
            status_code=500
        )


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

# Include routers
app.include_router(test_router)
app.include_router(analysis)
app.include_router(visualization_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
