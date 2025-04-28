import logging
import traceback
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.rag.debug_service import DebugService
from app.db.database import get_dw_db

# Set up logging
logger = logging.getLogger(__name__)

class SQLExecutionService:
    """Service for executing SQL queries against the DW database"""
    
    def __init__(self, debug_service: Optional[DebugService] = None):
        """Initialize the SQL execution service.
        
        Args:
            debug_service: Optional debug service for tracking query execution.
        """
        self.debug_service = debug_service

    async def execute_query(self, sql_query: str, dw_db: Session = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return the results.
        
        Args:
            sql_query: The SQL query to execute.
            dw_db: SQLAlchemy session for the DW database.
            
        Returns:
            List of dictionaries representing the query results.
        
        Raises:
            Exception: If there's an error executing the query.
        """
        # Start debug tracking if available
        if self.debug_service:
            self.debug_service.start_step("sql_execution", {"sql_query": sql_query})
        
        try:
            logger.info(f"Executing SQL query: {sql_query}")
            
            # Use provided db session, or get a new one
            db_session = dw_db
            if db_session is None:
                logger.debug("Creating new database session")
                db_session = next(get_dw_db())
                session_provided = False
            else:
                session_provided = True
            
            # Execute the query
            result = db_session.execute(text(sql_query))
            
            # Extract column names from result
            columns = result.keys()
            
            # Convert result to list of dictionaries
            rows = []
            for row in result:
                row_dict = {}
                for i, column in enumerate(columns):
                    # Handle None values
                    row_dict[column] = row[i]
                rows.append(row_dict)
            
            # Log and debug
            result_count = len(rows)
            logger.info(f"Query executed successfully. Returned {result_count} rows.")
            
            # End debug tracking with success
            if self.debug_service:
                self.debug_service.end_step(success=True, details={
                    "result_count": result_count,
                    "columns": list(columns)
                })
            
            # Close session if we created it
            if not session_provided:
                db_session.close()
            
            return rows
            
        except Exception as e:
            # Log error and provide debug info
            error_msg = f"Error executing SQL query: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            
            # End debug tracking with error
            if self.debug_service:
                self.debug_service.end_step(success=False, error=str(e))
            
            # Re-raise the exception
            raise Exception(f"SQL Execution Error: {str(e)}")
            
    def validate_query(self, sql_query: str) -> bool:
        """Validate that a SQL query is safe to execute.
        
        Args:
            sql_query: The SQL query to validate.
            
        Returns:
            True if the query is valid and safe to execute, False otherwise.
        """
        # Convert to lowercase for easier checking
        query_lower = sql_query.lower().strip()
        
        # Check that query is read-only (SELECT, WITH, EXPLAIN)
        valid_prefixes = ["select ", "with ", "explain "]
        is_valid = any(query_lower.startswith(prefix) for prefix in valid_prefixes)
        
        # Check for dangerous operations
        dangerous_keywords = [
            "insert ", "update ", "delete ", "drop ", "create ", "alter ", 
            "truncate ", "grant ", "revoke ", ";", "-- ", "/*", "*/"
        ]
        
        is_dangerous = any(keyword in query_lower for keyword in dangerous_keywords)
        
        return is_valid and not is_dangerous 