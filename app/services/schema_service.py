from typing import Optional, Dict, List
from app.db.database import get_db
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

class SchemaService:
    def __init__(self):
        self.schema_context = None
        self.table_relationships = {}
        self.table_metadata = {}
        
    async def initialize(self):
        """Initialize the schema service by pre-loading schema context"""
        if not self.schema_context:
            await self.get_schema_context()
            await self.detect_relationships()
            
    async def get_schema_context(self) -> str:
        """Get database schema context with enhanced metadata"""
        if self.schema_context:
            return self.schema_context
            
        try:
            db = next(get_db())
            # Get column information
            result = db.execute(text("""
                SELECT 
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM 
                    information_schema.columns
                WHERE 
                    table_schema = 'dw'
                ORDER BY 
                    table_name, ordinal_position;
            """))
            
            # Build schema context with enhanced information
            schema_info = {}
            for row in result:
                table_name = row[0]
                if table_name not in schema_info:
                    schema_info[table_name] = []
                nullable = "NULL" if row[3] == 'YES' else "NOT NULL"
                default = f" DEFAULT {row[4]}" if row[4] else ""
                schema_info[table_name].append(f"  - {row[1]}: {row[2]} {nullable}{default}")
                
                # Store metadata for each table
                if table_name not in self.table_metadata:
                    self.table_metadata[table_name] = []
                self.table_metadata[table_name].append({
                    "column_name": row[1],
                    "data_type": row[2],
                    "is_nullable": row[3] == 'YES',
                    "default": row[4]
                })
            
            # Get additional table metadata
            table_counts = await self._get_table_row_counts()
            
            # Format schema context with enhanced information
            schema_context = "Database Schema (dw):\n\n"
            for table_name, columns in schema_info.items():
                row_count = table_counts.get(table_name, "unknown")
                schema_context += f"Table: dw.{table_name} (approx. {row_count} rows)\n"
                schema_context += "\n".join(columns)
                schema_context += "\n\n"
            
            self.schema_context = schema_context
            logger.info(f"Schema context loaded with {len(schema_info)} tables")
            return schema_context
            
        except Exception as e:
            logger.error(f"Error getting schema context: {str(e)}")
            return "Error retrieving schema information"
            
    async def detect_relationships(self):
        """Detect relationships between tables based on column names"""
        try:
            # Simple heuristic: look for columns that match "<table_name>_id" pattern
            for table_name, columns in self.table_metadata.items():
                for column in columns:
                    col_name = column["column_name"]
                    for target_table in self.table_metadata.keys():
                        # Check if column follows the pattern "table_id"
                        if col_name == f"{target_table}_id" or col_name == f"{target_table}id":
                            if table_name not in self.table_relationships:
                                self.table_relationships[table_name] = []
                            self.table_relationships[table_name].append({
                                "target_table": target_table,
                                "source_column": col_name,
                                "relationship_type": "many-to-one"
                            })
            
            logger.info(f"Detected {sum(len(rels) for rels in self.table_relationships.values())} relationships between tables")
        except Exception as e:
            logger.error(f"Error detecting relationships: {str(e)}")
    
    async def _get_table_row_counts(self) -> Dict[str, int]:
        """Get approximate row counts for each table"""
        table_counts = {}
        try:
            db = next(get_db())
            # Get list of tables
            tables_result = db.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'dw'
            """))
            
            tables = [row[0] for row in tables_result]
            
            # Get row count for each table
            for table in tables:
                try:
                    count_result = db.execute(text(f"""
                        SELECT count(*) FROM dw.{table}
                    """))
                    count = next(count_result)[0]
                    table_counts[table] = count
                except Exception as e:
                    logger.warning(f"Could not get count for table {table}: {str(e)}")
            
            return table_counts
        except Exception as e:
            logger.error(f"Error getting table row counts: {str(e)}")
            return {}
            
    async def get_visualization_compatible_tables(self) -> List[Dict]:
        """Get a list of tables that are suitable for visualization"""
        viz_tables = []
        try:
            for table_name, columns in self.table_metadata.items():
                # Check if table has date/time columns and numeric columns
                has_date = any(col["data_type"] in ("date", "timestamp", "timestamp with time zone") for col in columns)
                has_numeric = any(col["data_type"] in ("integer", "numeric", "double precision", "bigint") for col in columns)
                has_categorical = any(col["data_type"] in ("character varying", "text", "character") for col in columns)
                
                if (has_date and has_numeric) or (has_categorical and has_numeric):
                    viz_tables.append({
                        "table_name": table_name,
                        "has_date": has_date,
                        "has_numeric": has_numeric,
                        "has_categorical": has_categorical,
                        "column_count": len(columns)
                    })
            
            return viz_tables
        except Exception as e:
            logger.error(f"Error getting visualization compatible tables: {str(e)}")
            return [] 