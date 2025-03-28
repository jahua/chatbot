from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.llm.gemini_adapter import GeminiAdapter
from app.models.chat import ChatMessage, ChatResponse
from sqlalchemy import text
import pandas as pd
import logging
import traceback
import asyncio
from app.services.conversation_service import ConversationService
import json

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self):
        self.llm = GeminiAdapter()
        self.db = SessionLocal()
        self.conversation_service = ConversationService(self.db)
        
    def get_schema_summary(self):
        """Get database schema summary"""
        # This would typically come from your database
        return """Database Schema:

Table: data_lake.aoi_days_raw
  - source_system: character varying
  - dwelltimes: jsonb
  - ingestion_timestamp: timestamp without time zone
  - top_swiss_municipalities: jsonb
  - top_last_municipalities: jsonb
  - aoi_date: date
  - top_swiss_cantons: jsonb
  - id: integer
  - raw_content: jsonb
  - visitors: jsonb
  - top_last_cantons: jsonb
  - overnights_from_yesterday: jsonb
  - aoi_id: character varying
  - load_date: date
  - top_foreign_countries: jsonb
  - demographics: jsonb"""
        
    async def process_message(self, message: str, session_id: str):
        """Process a chat message"""
        try:
            # First, check for similar conversations
            similar_conversations = self.conversation_service.find_similar_conversations(message)
            
            # If we have similar conversations, use their SQL queries as examples
            schema_context = self.get_schema_summary()
            if similar_conversations:
                example_queries = "\n".join([
                    f"Example query {i+1}:\n{c.sql_query}"
                    for i, c in enumerate(similar_conversations[:3])
                ])
                schema_context += f"\n\nPrevious similar queries:\n{example_queries}"
            
            # Generate SQL query
            sql_query = await self.llm.generate_sql(message, schema_context)
            if not sql_query:
                return {
                    "success": False,
                    "sql_query": None,
                    "response": "Failed to generate SQL query."
                }
            
            # Execute SQL query and get results
            # This would typically be done through your database connection
            # For now, we'll return a placeholder response
            data = "Sample data from database query"
            
            # Generate natural language response
            response = await self.llm.generate_response(message, sql_query, data)
            
            # Extract metadata from the query and response
            query_metadata = {
                "query_type": "visitor_data" if "visitor" in message.lower() else "other",
                "has_date_range": "between" in sql_query.lower(),
                "has_region": "region" in sql_query.lower(),
                "has_aoi": "aoi" in sql_query.lower()
            }
            
            # Save conversation to history
            self.conversation_service.save_conversation(
                session_id=session_id,
                prompt=message,
                sql_query=sql_query,
                response=response,
                schema_context=schema_context,
                metadata=query_metadata
            )
            
            return {
                "success": True,
                "sql_query": sql_query,
                "response": response
            }
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "sql_query": None,
                "response": f"Error: {str(e)}"
            }
        finally:
            self.db.close() 

# Initialize chat service
chat_service = ChatService() 