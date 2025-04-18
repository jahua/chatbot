from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.services.chat_service import ChatService
from app.db.database import get_dw_db

router = APIRouter(prefix="/chat", tags=["chat"])

class ChatRequest(BaseModel):
    message: str
    region_id: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

class ChatResponse(BaseModel):
    message: str
    response: str
    analysis: dict

def get_chat_service(dw_db: Session = Depends(get_dw_db)) -> ChatService:
    return ChatService(dw_db=dw_db)

@router.post("/message", response_model=ChatResponse)
async def process_message(
    request: ChatRequest,
    chat_service: ChatService = Depends(get_chat_service)
) -> ChatResponse:
    """Process a chat message and return analysis"""
    try:
        result = await chat_service.process_message(
            message=request.message,
            region_id=request.region_id,
            start_date=request.start_date,
            end_date=request.end_date
        )
        return ChatResponse(**result)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing message: {str(e)}"
        ) 