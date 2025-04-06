from pydantic import BaseModel
from typing import Optional

class ChatRequest(BaseModel):
    """Request model for chat endpoint"""
    message: str
    session_id: Optional[str] = None 