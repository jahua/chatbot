from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from app.services.chat_service import ChatService
from app.core.config import settings
from app.models.chat import ChatMessage, ChatResponse
from app.db.database import get_db, DatabaseService
from sqlalchemy.orm import Session
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.DEBUG)
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

def get_chat_service(db: Session = Depends(get_db)) -> ChatService:
    """Get ChatService instance with database session"""
    try:
        chat_service = ChatService(db)
        return chat_service
    except Exception as e:
        logger.error(f"Error initializing ChatService: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error initializing chat service: {str(e)}")

@app.get("/")
async def root():
    return {"message": "Welcome to the Tourism Data Chatbot API"}

@app.post("/api/chat/message")
async def process_message(
    message: Dict[str, Any],
    chat_service: ChatService = Depends(get_chat_service)
) -> Dict[str, Any]:
    """Process a chat message"""
    try:
        # Extract content from message
        content = message.get("content", message.get("message", ""))
        if not content:
            raise HTTPException(status_code=400, detail="Message content is required")
            
        response = await chat_service.process_message(content)
        return response
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.get("/api/schema")
async def get_schema(
    chat_service: ChatService = Depends(get_chat_service)
):
    try:
        schema_info = chat_service._load_schema_summary()
        return {"success": True, "schema": schema_info}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 