from sqlalchemy import create_engine, text, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from ..core.config import settings
from typing import Dict, Any, Optional, List, Generator, Union, Tuple
import pandas as pd
import logging
import traceback
import asyncio
import os
import time
from datetime import datetime

# Set up detailed logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Database URL configuration
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "336699")  # Default for development
DB_HOST = os.getenv("DB_HOST", "3.76.40.121")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "trip_dw")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create SQLAlchemy engine with connection pooling
logger.debug(f"Creating database engine with URL: {DATABASE_URL}")
engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=True,  # Enable SQL query logging
    connect_args={"options": "-c statement_timeout=15000"}  # 15 seconds timeout
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

# Make sure the database is available
try:
    logger.debug("Initializing DatabaseService")
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    logger.debug("Created database engine")
except Exception as e:
    logger.error(f"Database connection error: {str(e)}")
    raise

logger.debug("Created session factory")

def get_db():
    """Dependency for getting a database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class DatabaseService:
    """Service for database operations"""
    
    def __init__(self):
        """Initialize database service"""
        self.engine = engine
        
    def close(self):
        """Close database connections"""
        # No need to explicitly close with SQLAlchemy connection pooling
        pass
        
    def execute_query(self, query: str, params: Optional[Union[Dict, List, Tuple]] = None) -> List[Dict]:
        """Execute a SQL query and return results as a list of dictionaries."""
        try:
            logger.debug(f"Executing query: {query}")
            logger.debug(f"With parameters: {params}")
            
            with self.engine.connect() as connection:
                # Convert params to the correct format for SQLAlchemy
                if params is not None:
                    if isinstance(params, (list, tuple)):
                        # Convert list/tuple to dict with numbered parameters
                        params = {f"param_{i}": val for i, val in enumerate(params)}
                        # Replace %s with :param_N in the query
                        for i in range(len(params)):
                            query = query.replace("%s", f":param_{i}")
                    elif not isinstance(params, dict):
                        params = {"param": params}
                        query = query.replace("%s", ":param")
                
                # Execute query with proper parameter binding
                result = connection.execute(text(query), params or {})
                
                # Convert result rows to dictionaries safely
                return [row._asdict() for row in result]
                
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Parameters: {params}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
            
    def validate_query(self, query: str):
        """Validate a SQL query without executing it"""
        try:
            with SessionLocal() as session:
                # Add EXPLAIN to analyze the query without executing
                explain_query = f"EXPLAIN {query}"
                session.execute(text(explain_query))
                session.commit()  # Commit the transaction to avoid rollback
                return True
        except Exception as e:
            logger.error(f"Error validating query: {str(e)}")
            return False

__all__ = ['engine', 'SessionLocal', 'get_db', 'DatabaseService'] 