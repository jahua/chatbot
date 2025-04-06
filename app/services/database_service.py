import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, List, Optional
import logging
import os
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseService:
    def __init__(self):
        # Use direct connection string with password
        self.connection_string = "dbname=trip_dw user=postgres password=336699 host=3.76.40.121 port=5432"
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        """
        conn = None
        try:
            conn = psycopg2.connect(self.connection_string)
            yield conn
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
        finally:
            if conn is not None:
                conn.close()
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query)
                    results = cur.fetchall()
                    return [dict(row) for row in results]
        except psycopg2.Error as e:
            logger.error(f"Database error executing query: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    async def validate_query(self, query: str) -> bool:
        """
        Validate a SQL query without executing it
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("EXPLAIN " + query)
                    return True
        except psycopg2.Error as e:
            logger.error(f"Invalid query: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error validating query: {str(e)}")
            return False
    
    async def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a table's structure
        """
        try:
            query = """
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position;
            """
            
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (table_name,))
                    columns = cur.fetchall()
                    
                    if not columns:
                        return None
                    
                    return {
                        'table_name': table_name,
                        'columns': [dict(col) for col in columns]
                    }
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            return None
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """
        Get information about all tables in the database
        """
        try:
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
            
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query)
                    tables = cur.fetchall()
                    
                    schema_info = []
                    for table in tables:
                        table_name = table['table_name']
                        table_info = await self.get_table_info(table_name)
                        if table_info:
                            schema_info.append(table_info)
                    
                    return schema_info
        except Exception as e:
            logger.error(f"Error getting schema info: {str(e)}")
            return [] 
            
    def close(self):
        """Clean up any resources"""
        # Nothing to do here since we're using connection context managers
        # But this method is needed to maintain API compatibility
        pass 