from pydantic import BaseModel
from typing import Optional, Dict, Any

class ChatMessage(BaseModel):
    message: str
    session_id: str

class ChatResponse(BaseModel):
    success: bool
    sql_query: Optional[str] = None
    response: str
    schema_context: Optional[str] = None 