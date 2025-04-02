from pydantic import BaseModel
from typing import Dict, Any, Optional, List
from datetime import datetime

class ChatMessage(BaseModel):
    content: str
    role: Optional[str] = "user"
    model: Optional[str] = "openai"
    session_id: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    data: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    sql_query: Optional[str] = None
    visualization: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True 