from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime

class ChatMessageBase(BaseModel):
    content: str
    role: str
    model: Optional[str] = "claude"

class ChatMessageCreate(ChatMessageBase):
    pass

class ChatMessageResponse(ChatMessageBase):
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    sql_query: Optional[str] = None
    results: Optional[list] = None
    visualization: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True 