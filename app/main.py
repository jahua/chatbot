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
from app.rag.debug_service import DebugStep
import traceback
import sys
import os
import json
import asyncio
from fastapi.responses import JSONResponse
import uuid

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
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

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

@app.on_event("startup")
async def startup_event():
    global chat_service
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

def prepare_debug_info(debug_info):
    """Prepare debug info for JSON serialization by converting DebugStep objects to dictionaries."""
    if not debug_info:
        return debug_info
        
    # If there's a steps key, ensure all steps are dictionaries, not DebugStep objects
    if "steps" in debug_info:
        # Handle case where steps is a single DebugStep object
        if isinstance(debug_info["steps"], DebugStep):
            debug_info["steps"] = [asdict(debug_info["steps"])]
        # Handle case where steps is a list of objects
        elif isinstance(debug_info["steps"], list):
            # Convert each step to a dictionary if it's not already
            for i, step in enumerate(debug_info["steps"]):
                if isinstance(step, DebugStep):
                    # If the step is a DebugStep object, convert it to dict using asdict
                    debug_info["steps"][i] = asdict(step)
    
    return debug_info

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
            raise HTTPException(status_code=503, detail="Chat service not initialized")
        
        # For now, create a simplified response with just the message
        response = {
            "message_id": str(uuid.uuid4()),
            "message": request.message,
            "content": "This is a simplified response while we debug the issue.",
            "status": "success"
        }
        
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

@app.post("/chat/stream")
async def stream_chat(
    request: ChatRequest,
    dw_db: Session = Depends(get_dw_db)
):
    """Process chat messages with streaming response"""
    try:
        logger.info(f"Received streaming chat request: {request.message}")
        
        if not chat_service:
            logger.error("Chat service not initialized")
            raise HTTPException(status_code=503, detail="Chat service not initialized")
        
        async def generate():
            try:
                # Initial response with message ID
                message_id = None  # Will be generated by process_chat_stream
                yield f"data: {json.dumps({'type': 'start'})}\n\n"
                
                # Set a flag to indicate this is a direct API call
                is_direct_query = request.is_direct_query
                
                # Start processing the message
                response_started = False
                
                # Process the chat request with streaming
                async for chunk in chat_service.process_chat_stream(
                    message=request.message,
                    session_id=request.session_id,
                    is_direct_query=is_direct_query,
                    message_id=message_id,
                    dw_db=dw_db
                ):
                    # If we receive a message_id, store it
                    if "message_id" in chunk:
                        message_id = chunk["message_id"]
                        yield f"data: {json.dumps({'type': 'message_id', 'message_id': message_id})}\n\n"
                        
                    # Handle content start event
                    if "type" in chunk and chunk["type"] == "content_start":
                        yield f"data: {json.dumps({'type': 'content_start', 'message_id': message_id})}\n\n"
                        response_started = True
                        
                    # Stream different parts of the response as they become available
                    if "content_chunk" in chunk:
                        yield f"data: {json.dumps({'type': 'content', 'content': chunk['content_chunk']})}\n\n"
                    # Also handle the "content" field from ChatService
                    elif "content" in chunk:
                        yield f"data: {json.dumps({'type': 'content', 'content': chunk['content']})}\n\n"
                    
                    if "sql_query" in chunk and chunk["sql_query"]:
                        yield f"data: {json.dumps({'type': 'sql_query', 'sql_query': chunk['sql_query']})}\n\n"
                    
                    if "visualization" in chunk and chunk["visualization"]:
                        yield f"data: {json.dumps({'type': 'visualization', 'visualization': chunk['visualization']})}\n\n"
                        
                    if "debug_info" in chunk and chunk["debug_info"]:
                        debug_info = prepare_debug_info(chunk["debug_info"])
                        yield f"data: {json.dumps({'type': 'debug_info', 'debug_info': debug_info})}\n\n"
                    
                    # Add a small delay to simulate a more natural typing effect
                    await asyncio.sleep(0.05)
                
                # Signal that the stream is complete
                yield f"data: {json.dumps({'type': 'end', 'message_id': message_id})}\n\n"
                
            except Exception as e:
                logger.error(f"Error in stream generation: {str(e)}")
                logger.error(traceback.format_exc())
                # Send error message
                yield f"data: {json.dumps({'type': 'error', 'error': str(e)})}\n\n"
        
        return StreamingResponse(
            generate(), 
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering
                "Content-Type": "text/event-stream"  # Ensure correct content type
            }
        )
        
    except Exception as e:
        logger.error(f"Error processing streaming chat request: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.get("/")
async def root():
    return {
        "message": "Welcome to the Tourism Analytics API",
        "docs": "/docs",
        "redoc": "/redoc"
    } 