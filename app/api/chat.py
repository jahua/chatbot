from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, Dict, Any
from app.services.chat_service import chat_service

router = APIRouter()

class ChatMessage(BaseModel):
    message: str
    session_id: Optional[str] = None

class ChatResponse(BaseModel):
    success: bool
    sql_query: Optional[str] = None
    response: Optional[str] = None
    schema_context: Optional[str] = None
    error: Optional[str] = None

@router.post("/message", response_model=ChatResponse)
async def process_message(message: ChatMessage):
    """Process a chat message and return SQL query and response"""
    try:
        # Generate a session ID if none was provided
        session_id = message.session_id or "default-session"
        result = await chat_service.process_message(message.message, session_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/schema", response_model=Dict[str, Any])
async def get_schema():
    """Get database schema summary"""
    try:
        return chat_service.get_schema_summary()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 