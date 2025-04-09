from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.schemas.chat import ChatRequest, ChatResponse
from app.services.chat_service import ChatService
from app.db.database import DatabaseService
from app.llm.openai_adapter import OpenAIAdapter
from app.db.schema_manager import schema_manager
from dotenv import load_dotenv
import logging
import os
import uuid

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Tourism Data Analysis Chatbot")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
db_service = DatabaseService()
try:
    openai_adapter = OpenAIAdapter(
        api_key=os.getenv("OPENAI_API_KEY"),
        api_base=os.getenv("OPENAI_API_BASE")
    )
    logger.info("OpenAI adapter initialized successfully")
except Exception as e:
    logger.error(f"Error initializing OpenAI adapter: {str(e)}")
    openai_adapter = None  # Set to None so we can check if it's available

# Initialize chat service with or without OpenAI adapter
chat_service = ChatService(
    db_service=db_service, 
    schema_manager=schema_manager,
    llm_adapter=openai_adapter  # Changed from openai_adapter to llm_adapter
)

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    try:
        logger.info("Services initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing services: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup services on shutdown"""
    try:
        if chat_service:
            chat_service.close()
            logger.info("Chat service closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")

@app.post("/chat")
async def chat(request: ChatRequest) -> ChatResponse:
    """Process chat messages"""
    try:
        if not chat_service:
            raise HTTPException(status_code=503, detail="Chat service not initialized")
        
        # Set a flag to indicate this is a direct API call (not validation)
        is_direct_query = True
            
        # Process the chat request
        response = await chat_service.process_chat(
            message=request.message,
            is_direct_query=is_direct_query
        )
        
        # Return the response
        return response
        
    except Exception as e:
        logger.error(f"Error processing chat request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"} 