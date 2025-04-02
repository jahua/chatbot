from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from app.services.chat_service import ChatService
from app.core.config import settings
from app.schemas.chat import ChatMessage, ChatResponse
from app.db.database import DatabaseService, SessionLocal, get_db
from sqlalchemy.orm import Session
import logging
from typing import Dict, Any
import traceback
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
chat_service = None

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global chat_service
    try:
        chat_service = ChatService()
        logger.info("Services initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing services: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup services on shutdown"""
    global chat_service
    if chat_service:
        try:
            chat_service.close()
            logger.info("Services cleaned up successfully")
        except Exception as e:
            logger.error(f"Error cleaning up services: {str(e)}")

@app.get("/")
async def root():
    return {"message": "Welcome to the Tourism Data Chatbot API"}

@app.post("/api/chat/message", response_model=ChatResponse)
async def chat_message(message: ChatMessage) -> Dict[str, Any]:
    """Handle chat messages"""
    try:
        if not chat_service:
            raise HTTPException(status_code=500, detail="Chat service not initialized")
        
        response = await chat_service.process_message(message.content)
        return response
    except Exception as e:
        logger.error(f"Error processing chat message: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.get("/api/schema")
async def get_schema():
    """Get database schema information"""
    try:
        if not chat_service:
            raise HTTPException(status_code=500, detail="Chat service not initialized")
        
        schema_info = await chat_service.get_schema_info()
        return {"success": True, "schema": schema_info}
    except Exception as e:
        logger.error(f"Error getting schema: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 