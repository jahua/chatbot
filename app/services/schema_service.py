from typing import Optional
from app.db.database import get_db
from sqlalchemy import text

class SchemaService:
    def __init__(self):
        self.schema_context = None
        
    async def get_schema_context(self) -> str:
        """Get database schema context"""
        if self.schema_context:
            return self.schema_context
            
        try:
            db = next(get_db())
            result = db.execute(text("""
                SELECT 
                    table_name,
                    column_name,
                    data_type
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = 'dw'
                ORDER BY 
                    table_name, ordinal_position;
            """))
            
            # Build schema context
            schema_info = {}
            for row in result:
                table_name = row[0]
                if table_name not in schema_info:
                    schema_info[table_name] = []
                schema_info[table_name].append(f"  - {row[1]}: {row[2]}")
            
            # Format schema context
            schema_context = "Database Schema (dw):\n\n"
            for table_name, columns in schema_info.items():
                schema_context += f"Table: dw.{table_name}\n"
                schema_context += "\n".join(columns)
                schema_context += "\n\n"
            
            self.schema_context = schema_context
            return schema_context
            
        except Exception as e:
            print(f"Error getting schema context: {str(e)}")
            return "Error retrieving schema information" 