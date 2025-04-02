from sqlalchemy import create_engine, text, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from app.core.config import settings
from typing import Dict, Any, Optional, List
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
            self.engine = engine
            self.session = SessionLocal()
            logger.debug("Database session created successfully in DatabaseService")
        except Exception as e:
            logger.error(f"Error creating database session in DatabaseService: {str(e)}")
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

    async def execute_query(self, query: str) -> list:
        """Execute a SQL query and return the results"""
        logger.debug(f"Executing query: {query}")
        try:
            # Execute the query using SQLAlchemy text()
            result = self.session.execute(text(query))
            logger.debug("Query executed successfully")
            
            # Convert the result to a list of dictionaries
            columns = result.keys()
            data = []
            for row in result:
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = row[i]
                data.append(row_dict)
            
            logger.debug(f"Query returned {len(data)} rows")
            return data
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        finally:
            logger.debug("Committing session after query execution")
            self.session.commit()

__all__ = ['engine', 'SessionLocal', 'get_db', 'DatabaseService'] 