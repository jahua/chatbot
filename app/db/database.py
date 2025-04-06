from sqlalchemy import create_engine, text, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from app.core.config import settings
from typing import Dict, Any, Optional, List, Generator
import pandas as pd
import logging
import traceback
import asyncio
import os

# Set up detailed logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create SQLAlchemy engine with connection pooling
logger.debug(f"Creating database engine with URL: {settings.DATABASE_URL}")
engine = create_engine(
    settings.DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
    echo=True  # Enable SQL query logging
)

# Add event listeners for connection lifecycle
@event.listens_for(engine, 'connect')
def on_connect(dbapi_connection, connection_record):
    logger.debug("New database connection established")

@event.listens_for(engine, 'checkout')
def on_checkout(dbapi_connection, connection_record, connection_proxy):
    logger.debug("Connection checked out from pool")

@event.listens_for(engine, 'checkin')
def on_checkin(dbapi_connection, connection_record):
    logger.debug("Connection returned to pool")

# Create SessionLocal class with proper session management
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    expire_on_commit=False
)

# Create Base class
Base = declarative_base()

def get_db():
    """Get database session"""
    logger.debug("Creating new database session")
    db = SessionLocal()
    try:
        logger.debug("Database session created successfully")
        yield db
    except Exception as e:
        logger.error(f"Error in database session: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        logger.debug("Closing database session")
        db.close()
        logger.debug("Database session closed")

class DatabaseService:
    def __init__(self):
        """Initialize database service"""
        logger.debug("Initializing DatabaseService")
        try:
            # Create database engine with timeout settings
            self.engine = create_engine(
                'postgresql://postgres:336699@3.76.40.121:5432/trip_dw',
                connect_args={
                    'connect_timeout': 10,
                    'options': '-c statement_timeout=15000'  # 15 seconds timeout
                }
            )
            logger.debug("Created database engine")
            
            # Create session factory
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            logger.debug("Created session factory")
            
        except Exception as e:
            logger.error(f"Error initializing database service: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        try:
            with self.engine.connect() as connection:
                # Set statement timeout for this connection
                connection.execute(text("SET statement_timeout = 15000"))  # 15 seconds
                
                # Execute query with timeout
                result = connection.execute(text(query))
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result]
                
        except Exception as e:
            if "canceling statement due to statement timeout" in str(e):
                logger.warning("Query execution timed out after 15 seconds")
                raise TimeoutError("Query execution timed out. Please try a more specific query or add filters.")
            logger.error(f"Error executing query: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def close(self):
        """Close database connection"""
        logger.debug("Closing DatabaseService")
        try:
            if hasattr(self, 'session'):
                logger.debug("Closing session in DatabaseService")
                self.session.close()
            if hasattr(self, 'engine'):
                logger.debug("Disposing engine in DatabaseService")
                self.engine.dispose()
            logger.debug("DatabaseService closed successfully")
        except Exception as e:
            logger.error(f"Error closing DatabaseService: {str(e)}")
            logger.error(traceback.format_exc())
            raise

# Create a single instance of DatabaseService
_db_service = None

def get_db() -> DatabaseService:
    """Get database service instance"""
    global _db_service
    if _db_service is None:
        _db_service = DatabaseService()
    return _db_service

__all__ = ['engine', 'SessionLocal', 'get_db', 'DatabaseService'] 