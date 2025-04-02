from sqlalchemy import Column, Integer, String, DateTime, JSON, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel

Base = declarative_base()

class ConversationHistory(Base):
    __tablename__ = "conversation_history"
    
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, index=True)
    prompt = Column(Text)
    sql_query = Column(Text)
    response = Column(Text)
    schema_context = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    query_metadata = Column(JSON)
    vector_embedding = Column(JSON)

class ChatMessage(BaseModel):
    message: str
    session_id: str

class ChatResponse(BaseModel):
    message: str
    sql_query: Optional[str] = None
    results: Optional[Dict[str, Any]] = None
    type: Optional[str] = None
    plot: Optional[Dict[str, Any]] = None
    statistics: Optional[Dict[str, Any]] = None
    success: Optional[bool] = True
    response: Optional[str] = None 