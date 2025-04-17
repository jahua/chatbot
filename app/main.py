from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import logging
from app.db.database import get_db, get_dw_db
from app.db.dw_connection import get_dw_session
from app.services.chat_service import ChatService
from app.services.schema_service import SchemaService
from app.schemas.chat import ChatRequest, ChatResponse
from app.core.config import settings
from app.db.schema_manager import SchemaManager
from app.llm.openai_adapter import OpenAIAdapter
import traceback
import sys
import os

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
        
        # Initialize schema manager
        schema_manager = SchemaManager()
        logger.info("Schema manager initialized")
        
        # Initialize OpenAI adapter
        llm_adapter = OpenAIAdapter()
        logger.info("OpenAI adapter initialized")
        
        # Initialize chat service
        chat_service = ChatService(
            dw_db=dw_db,
            schema_manager=schema_manager,
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
        chat_service.close()
        logger.info("Chat service closed")

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
        
        # Set a flag to indicate this is a direct API call (not validation)
        is_direct_query = True
            
        # Process the chat request
        response = await chat_service.process_chat(
            message=request.message,
            is_direct_query=is_direct_query
        )
        
        logger.info(f"Chat response generated: {response}")
        
        # Return the response
        return response
        
    except Exception as e:
        logger.error(f"Error processing chat request: {str(e)}")
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