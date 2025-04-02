from pydantic import BaseModel
from typing import Dict, Any, Optional, List
from datetime import datetime

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    message: str

class ChatResponse(BaseModel):
    response: str
    visualization: Optional[str] = None
    query: Optional[str] = None
    success: bool = True

    class Config:
        from_attributes = True 