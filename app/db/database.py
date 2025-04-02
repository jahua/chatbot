from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
from typing import Dict, Any, Optional
import pandas as pd
import logging
import traceback
import asyncio

logger = logging.getLogger(__name__)

# Create SQLAlchemy engine with logging
logger.info(f"Connecting to database at {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}")
engine = create_engine(
    settings.DATABASE_URL,
    echo=True,  # Enable SQL query logging
    pool_pre_ping=True  # Enable connection health checks
)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create Base class
Base = declarative_base()

def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        logger.info("Database session created successfully")
        yield db
    finally:
        db.close()
        logger.info("Database session closed")

class DatabaseService:
    def __init__(self, db):
        self.db = db
        logger.info("Initializing DatabaseService...")
        try:
            # Test the connection using text()
            self.db.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
        except Exception as e:
            logger.error(f"Database connection test failed: {str(e)}")
            raise
        logger.info("DatabaseService initialized successfully")
    
    async def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame"""
        logger.info(f"Executing query: {query}")
        try:
            # Convert query string to SQLAlchemy text object
            sql = text(query)
            result = self.db.execute(sql)
            rows = result.fetchall()
            columns = result.keys()
            logger.info(f"Query returned {len(rows)} rows with columns: {columns}")
            
            df = pd.DataFrame(rows, columns=columns)
            return df
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error executing query: {error_msg}")
            logger.error(f"Traceback: {traceback.format_exc()}")
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
        logger.info(f"Saving conversation for session {session_id}")
        try:
            # Convert response to JSON string if present
            response_json = None
            if response:
                response_json = str(response)  # Simple string conversion for now
            
            # Create SQL query using text()
            query = text("""
            INSERT INTO conversations (conversation_id, user_message, assistant_message, metadata)
            VALUES (:conversation_id, :user_message, :assistant_message, :metadata)
            """)
            
            # Execute query
            await self.db.execute(
                query,
                {
                    "conversation_id": session_id,
                    "user_message": user_message,
                    "assistant_message": response_json,
                    "metadata": {
                        "sql_query": sql_query,
                        "error": error
                    }
                }
            )
            await self.db.commit()
            logger.info("Conversation saved successfully")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error saving conversation: {error_msg}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.db.rollback()
            raise

__all__ = ['engine', 'SessionLocal', 'get_db', 'DatabaseService'] 