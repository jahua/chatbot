from pydantic import BaseModel
from typing import Dict, Any, Optional, List
from datetime import datetime

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    is_direct_query: bool = False

class ChatResponse(BaseModel):
    message_id: str
    content: str
    sql_query: Optional[str] = None
    visualization: Optional[str] = None
    status: str = "success"
    response: Optional[str] = ""

    class Config:
        from_attributes = True 