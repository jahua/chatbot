from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from typing import Dict, Any, Optional
import pandas as pd
import logging
import traceback
import asyncio

# Create database URL from individual settings
DATABASE_URL = f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class DatabaseService:
    def __init__(self, db):
        # Set up logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('chatbot.log', mode='a'),  # Append mode to preserve logs
                logging.StreamHandler()  # Also log to console
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing DatabaseService...")
        
        self.db = db
        self.logger.debug("DatabaseService initialized successfully")
    
    async def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        self.logger.debug(f"Executing query: {query}")
        try:
            result = self.db.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()
            self.logger.debug(f"Query returned {len(rows)} rows with columns: {columns}")
            
            df = pd.DataFrame(rows, columns=columns)
            return df
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error executing query: {error_msg}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    async def save_conversation(
        self,
        session_id: str,
        user_message: str,
        sql_query: Optional[str] = None,
        response: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> None:
        """Save conversation to database"""
        self.logger.debug(f"Saving conversation for session {session_id}")
        try:
            # Convert response to JSON string if present
            response_json = None
            if response:
                response_json = str(response)  # Simple string conversion for now
            
            # Create SQL query
            query = text("""
                INSERT INTO conversations (
                    session_id,
                    user_message,
                    sql_query,
                    response,
                    error,
                    created_at
                ) VALUES (
                    :session_id,
                    :user_message,
                    :sql_query,
                    :response,
                    :error,
                    NOW()
                )
            """)
            
            # Execute query
            self.db.execute(
                query,
                {
                    "session_id": session_id,
                    "user_message": user_message,
                    "sql_query": sql_query,
                    "response": response_json,
                    "error": error
                }
            )
            self.db.commit()
            self.logger.debug("Conversation saved successfully")
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error saving conversation: {error_msg}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            self.db.rollback()
            raise

__all__ = ['engine', 'SessionLocal', 'get_db', 'DatabaseService'] 