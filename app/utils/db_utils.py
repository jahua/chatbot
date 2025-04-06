import logging
from typing import Dict, Any, List, Optional
import pandas as pd
from ..db.database import get_db
from sqlalchemy.orm import Session
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@contextmanager
def get_connection():
    """Get a database connection using environment variables"""
    conn = None
    try:
        # Use direct connection string with password
        conn = psycopg2.connect(
            "dbname=trip_dw user=postgres password=336699 host=3.76.40.121 port=5432"
        )
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

async def execute_query(query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Execute a SQL query and return results as a list of dictionaries"""
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params or {})
                results = cur.fetchall()
                conn.commit()  # Explicitly commit the transaction
                return [dict(row) for row in results]
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        raise

async def validate_query(query: str) -> bool:
    """Validate a SQL query without executing it"""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN {query}")
                conn.commit()  # Explicitly commit to avoid transaction rollback
                return True
    except Exception as e:
        logger.error(f"Invalid query: {str(e)}")
        return False

async def get_table_info(table_name: str) -> List[Dict[str, Any]]:
    """Get information about a table's structure"""
    query = """
    SELECT 
        column_name, 
        data_type, 
        is_nullable,
        column_default
    FROM 
        information_schema.columns
    WHERE 
        table_name = %s
    ORDER BY 
        ordinal_position;
    """
    try:
        return await execute_query(query, {"table_name": table_name})
    except Exception as e:
        logger.error(f"Error getting table info: {str(e)}")
        return []

async def get_schema_info() -> Dict[str, List[Dict[str, Any]]]:
    """Get information about all tables in the database"""
    query = """
    SELECT 
        table_name
    FROM 
        information_schema.tables
    WHERE 
        table_schema = 'public'
    ORDER BY 
        table_name;
    """
    try:
        tables = await execute_query(query)
        schema_info = {}
        for table in tables:
            table_name = table["table_name"]
            schema_info[table_name] = await get_table_info(table_name)
        return schema_info
    except Exception as e:
        logger.error(f"Error getting schema info: {str(e)}")
        return {} 