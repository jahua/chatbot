from typing import Dict, Any
from langchain.schema.runnable import Runnable
from app.db.database import DatabaseService
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class QueryExecutorNode(Runnable):
    def __init__(self, db: DatabaseService):
        self.db = db

    def invoke(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute SQL query and return results"""
        try:
            sql_query = input_data.get("sql_query")
            if not sql_query:
                raise ValueError("No SQL query provided in input")

            # Execute query
            results = self.db.execute_query(sql_query)
            df = pd.DataFrame(results)
            
            logger.info(f"Query executed successfully, returned {len(results)} rows")

            return {
                **input_data,  # Pass through previous data
                "query_results": results,
                "dataframe": df,
                "execution_error": None
            }

        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return {
                **input_data,
                "query_results": None,
                "dataframe": None,
                "execution_error": str(e)
            } 