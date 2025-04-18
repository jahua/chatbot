from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
import logging
from app.db.database import get_db, get_dw_db
from app.db.dw_connection import get_dw_session
from app.services.chat_service import ChatService
from app.services.schema_service import SchemaService
from app.rag.dw_context_service import DWContextService
from app.schemas.chat import ChatRequest, ChatResponse
from app.core.config import settings
from app.db.schema_manager import SchemaManager
from app.llm.openai_adapter import OpenAIAdapter
import traceback
import sys
import os
import json
import asyncio

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
        
        # Get database session
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
        
        # Initialize chat service
        chat_service = ChatService(
            dw_db=dw_db,
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

@app.post("/chat")
async def chat(
    request: ChatRequest,
    dw_db: Session = Depends(get_dw_db)
) -> ChatResponse:
    """Process chat messages"""
    try:
        logger.info(f"Received chat request: {request.message}")
        
        if not chat_service:
            logger.error("Chat service not initialized")
            raise HTTPException(status_code=503, detail="Chat service not initialized")
        
        # Get the is_direct_query flag from the request, default to False
        is_direct_query = request.is_direct_query if hasattr(request, 'is_direct_query') else False
        
        # Use process_chat_stream but collect all the results into a single response
        response_parts = {}
        async for chunk in chat_service.process_chat_stream(
            message=request.message,
            is_direct_query=is_direct_query
        ):
            # Collect relevant parts of the response
            if "message_id" in chunk:
                response_parts["message_id"] = chunk["message_id"]
            if "content_chunk" in chunk:
                if "content" not in response_parts:
                    response_parts["content"] = ""
                response_parts["content"] += chunk["content_chunk"]
            if "sql_query" in chunk:
                response_parts["sql_query"] = chunk["sql_query"]
            if "visualization" in chunk:
                response_parts["visualization"] = chunk["visualization"]
            if "result" in chunk:
                response_parts["result"] = chunk["result"]
            if "debug_info" in chunk:
                response_parts["debug_info"] = chunk["debug_info"]
        
        # Ensure required fields are present
        if "message_id" not in response_parts:
            response_parts["message_id"] = "generated-id"
        if "content" not in response_parts:
            response_parts["content"] = "No content generated"
        
        # Add message to response
        response_parts["message"] = request.message
        
        logger.info(f"Chat response generated with {len(response_parts)} parts")
        
        # Return the response
        return response_parts
        
    except Exception as e:
        logger.error(f"Error processing chat request: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

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
                yield json.dumps({"type": "start"}) + "\n"
                
                # Set a flag to indicate this is a direct API call
                is_direct_query = request.is_direct_query
                
                # Start processing the message
                response_started = False
                
                # Process the chat request with streaming
                async for chunk in chat_service.process_chat_stream(
                    message=request.message,
                    is_direct_query=is_direct_query,
                    message_id=message_id
                ):
                    # If we receive a message_id, store it
                    if "message_id" in chunk:
                        message_id = chunk["message_id"]
                        yield json.dumps({"type": "message_id", "message_id": message_id}) + "\n"
                        
                    if not response_started and chunk.get("content_chunk"):
                        yield json.dumps({"type": "content_start", "message_id": message_id}) + "\n"
                        response_started = True
                        
                    # Stream different parts of the response as they become available
                    if "content_chunk" in chunk:
                        yield json.dumps({
                            "type": "content", 
                            "content": chunk["content_chunk"]
                        }) + "\n"
                    
                    if "sql_query" in chunk and chunk["sql_query"]:
                        yield json.dumps({
                            "type": "sql_query", 
                            "sql_query": chunk["sql_query"]
                        }) + "\n"
                    
                    if "visualization" in chunk and chunk["visualization"]:
                        yield json.dumps({
                            "type": "visualization", 
                            "visualization": chunk["visualization"]
                        }) + "\n"
                        
                    if "debug_info" in chunk and chunk["debug_info"]:
                        yield json.dumps({
                            "type": "debug_info", 
                            "debug_info": chunk["debug_info"]
                        }) + "\n"
                    
                    # Add a small delay to simulate a more natural typing effect
                    await asyncio.sleep(0.05)
                
                # Signal that the stream is complete
                yield json.dumps({"type": "end", "message_id": message_id}) + "\n"
                
            except Exception as e:
                logger.error(f"Error in stream generation: {str(e)}")
                logger.error(traceback.format_exc())
                # Send error message
                yield json.dumps({
                    "type": "error", 
                    "error": str(e)
                }) + "\n"
        
        return StreamingResponse(
            generate(), 
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no"  # Disable nginx buffering
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