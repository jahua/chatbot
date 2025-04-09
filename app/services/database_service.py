import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, Any, List, Optional
import logging
import os
from contextlib import contextmanager
import signal

logger = logging.getLogger(__name__)

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("Query execution timed out")

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
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries
        """
        # Use different timeout for different types of queries
        timeout_seconds = 15  # Default timeout
        
        # For "highest spending industry" query, use a more focused approach with shorter timeout
        if "master_card" in query:
            # This is a simplified industry query - should be fast
            if "SELECT industry, SUM(txn_amt) as total_spending" in query and "GROUP BY industry" in query and "ORDER BY total_spending DESC" in query:
                timeout_seconds = 6  # Very focused query
                logger.info(f"Using optimized timeout ({timeout_seconds}s) for highest spending industry query")
            # Other master_card queries need careful timing
            else:
                timeout_seconds = 10
                logger.info(f"Setting shorter timeout ({timeout_seconds}s) for spending analysis query")
                
        try:
            # Set up timeout
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
            
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # For spending queries, set statement timeout at database level too
                    if "master_card" in query:
                        # Set slightly shorter timeout at DB level
                        cur.execute(f"SET statement_timeout = {(timeout_seconds - 1) * 1000}")
                    
                    if params:
                        cur.execute(query, params)
                    else:
                        cur.execute(query)
                    results = cur.fetchall()
                    conn.commit()  # Explicitly commit the transaction
                    # Cancel the alarm
                    signal.alarm(0)
                    return [dict(row) for row in results]
        except TimeoutException:
            logger.warning(f"Query execution timed out after {timeout_seconds} seconds")
            error_msg = "Query execution timed out. "
            if "highest spending industry" in query.lower() or ("industry" in query.lower() and "ORDER BY total_spending DESC" in query):
                error_msg += "For industry spending, try specifying a smaller time period like 'in the last month' or 'Q1 2023'."
            else:
                error_msg += "Please try a more specific query or add filters."
            raise TimeoutError(error_msg)
        except psycopg2.Error as e:
            logger.error(f"Database error executing query: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
        finally:
            # Ensure alarm is canceled even if an exception occurs
            signal.alarm(0)
    
    def validate_query(self, query: str) -> bool:
        """
        Validate a SQL query without executing it
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("EXPLAIN " + query)
                    conn.commit()  # Explicitly commit the transaction
                    return True
        except psycopg2.Error as e:
            logger.error(f"Invalid query: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error validating query: {str(e)}")
            return False
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
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
    
    def get_schema_info(self) -> List[Dict[str, Any]]:
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
                        table_info = self.get_table_info(table_name)
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