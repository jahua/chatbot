import vanna
from app.core.config import settings
from typing import Dict, Any
import json

class VannaAdapter:
    def __init__(self):
        self.vn = vanna.Vanna()
        self.vn.set_model('local')  # Using local model instead of GPT-4
        self.vn.set_database('postgres')
        
        # Initialize database connection
        self.vn.connect_to_postgres(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            database=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD
        )
        
        # Set the schema
        self.vn.set_schema(settings.DB_SCHEMA)
        
        # Train Vanna on the schema
        self.vn.train()
    
    async def generate_sql(self, schema_context: str, user_query: str) -> Dict[str, Any]:
        """Generate SQL query from natural language using Vanna"""
        try:
            # Generate SQL using Vanna
            sql_query = self.vn.generate_sql(user_query)
            
            # Add schema prefix if not present
            if "data_lake." not in sql_query:
                sql_query = sql_query.replace("regions", "data_lake.regions")
                sql_query = sql_query.replace("visits", "data_lake.visits")
                sql_query = sql_query.replace("visitors", "data_lake.visitors")
                sql_query = sql_query.replace("time_periods", "data_lake.time_periods")
                sql_query = sql_query.replace("visit_types", "data_lake.visit_types")
                sql_query = sql_query.replace("demographics", "data_lake.demographics")
            
            return {
                "sql_query": sql_query
            }
        except Exception as e:
            raise Exception(f"Error generating SQL with Vanna: {str(e)}")
    
    async def generate_response(
        self,
        sql_query: str,
        query_result: Any,
        user_query: str
    ) -> str:
        """Generate natural language response from query results"""
        try:
            # Use Vanna's natural language generation
            response = self.vn.generate_natural_language_response(
                sql=sql_query,
                results=query_result,
                question=user_query
            )
            return response
        except Exception as e:
            raise Exception(f"Error generating response with Vanna: {str(e)}")

# Initialize Vanna adapter
vanna_adapter = VannaAdapter() 