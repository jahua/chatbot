from typing import Dict, Any, List, Optional, Set
import logging

logger = logging.getLogger(__name__)

class SchemaContextManager:
    """
    Centralized manager for database schema information to support intelligent SQL generation.
    Maintains detailed table structure, JSON field mappings, and relationships between tables.
    """
    
    def __init__(self):
        """Initialize schema context with detailed table and field information"""
        self.tables = {
            "aoi_days_raw": {
                "schema": "data_lake",
                "primary_keys": ["aoi_id", "aoi_date"],
                "date_columns": ["aoi_date"],
                "description": "Contains daily visitor data for areas of interest",
                "columns": {
                    "aoi_id": {"type": "text", "description": "Unique identifier for area of interest"},
                    "aoi_date": {"type": "date", "description": "Date of the visitor data"},
                    "visitors": {
                        "type": "jsonb", 
                        "description": "JSON object containing visitor counts by type",
                        "fields": {
                            "swissTourist": {"type": "numeric", "description": "Number of Swiss tourists"},
                            "foreignTourist": {"type": "numeric", "description": "Number of foreign tourists"},
                            "swissLocal": {"type": "numeric", "description": "Number of Swiss local residents"},
                            "foreignWorker": {"type": "numeric", "description": "Number of foreign workers"},
                            "swissCommuter": {"type": "numeric", "description": "Number of Swiss commuters"}
                        },
                        "access_pattern": "visitors->>'field_name'",
                        "cast_pattern": "(visitors->>'field_name')::numeric"
                    },
                    "dwelltimes": {
                        "type": "jsonb",
                        "description": "Tourist dwell time distribution in hourly buckets",
                        "fields": {
                            "0.5-1h": {"type": "numeric", "description": "Visitors staying 0.5-1 hours"},
                            "1-2h": {"type": "numeric", "description": "Visitors staying 1-2 hours"},
                            "2-3h": {"type": "numeric", "description": "Visitors staying 2-3 hours"},
                            "3-4h": {"type": "numeric", "description": "Visitors staying 3-4 hours"},
                            "4-5h": {"type": "numeric", "description": "Visitors staying 4-5 hours"},
                            "5-6h": {"type": "numeric", "description": "Visitors staying 5-6 hours"},
                            "6-7h": {"type": "numeric", "description": "Visitors staying 6-7 hours"},
                            "7-8h": {"type": "numeric", "description": "Visitors staying 7-8 hours"},
                            "8-24h": {"type": "numeric", "description": "Visitors staying 8-24 hours"}
                        }
                    }
                }
            },
            "master_card": {
                "schema": "data_lake",
                "primary_keys": ["id"],
                "date_columns": ["txn_date"],
                "description": "Contains transaction data from credit card usage",
                "columns": {
                    "id": {"type": "integer", "description": "Primary key"},
                    "txn_date": {"type": "date", "description": "Date of the transaction"},
                    "industry": {"type": "text", "description": "Industry sector of the transaction"},
                    "segment": {"type": "text", "description": "Market segment (overall, domestic, international)"},
                    "txn_amt": {"type": "numeric", "description": "Total transaction amount (indexed to 2018)"},
                    "txn_cnt": {"type": "numeric", "description": "Number of transactions"},
                    "acct_cnt": {"type": "numeric", "description": "Number of distinct cards"},
                    "avg_ticket": {"type": "numeric", "description": "Average spend per transaction"},
                    "avg_freq": {"type": "numeric", "description": "Average transactions per card"},
                    "avg_spend": {"type": "numeric", "description": "Average spend per card"},
                    "geo_type": {"type": "text", "description": "Geographic type"},
                    "geo_name": {"type": "text", "description": "Geographic location name"},
                    "central_latitude": {"type": "numeric", "description": "Tile center latitude"},
                    "central_longitude": {"type": "numeric", "description": "Tile center longitude"}
                }
            }
        }
        
        # Define table relationships
        self.relationships = {
            "aoi_days_raw": {
                "master_card": {
                    "type": "spatial-temporal",
                    "join_conditions": [
                        "aoi_days_raw.aoi_date = master_card.txn_date",
                        "ST_DWithin(ST_MakePoint(master_card.central_longitude, master_card.central_latitude), ST_MakePoint(aoi_days_raw.longitude, aoi_days_raw.latitude), 0.01)"
                    ],
                    "description": "Relates visitor data to transaction data based on date and spatial proximity"
                }
            }
        }
        
        # Define common query patterns
        self.query_patterns = {
            "visitor_count": {
                "description": "Count visitors by type (Swiss vs. foreign)",
                "template": """
                    SELECT 
                        {date_grouping} as {date_alias},
                        SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                        SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                        SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) as total_visitors
                    FROM data_lake.aoi_days_raw
                    {where_clause}
                    GROUP BY {date_alias}
                    ORDER BY {order_clause};
                """,
                "parameters": {
                    "date_grouping": ["aoi_date", "DATE_TRUNC('day', aoi_date)", "DATE_TRUNC('week', aoi_date)", "DATE_TRUNC('month', aoi_date)"],
                    "date_alias": ["date", "day", "week", "month"],
                    "where_clause": "WHERE aoi_date BETWEEN '{start_date}' AND '{end_date}'",
                    "order_clause": ["{date_alias}", "total_visitors DESC"]
                }
            },
            "spending_analysis": {
                "description": "Analyze spending patterns by industry and location",
                "template": """
                    SELECT 
                        {date_grouping} as {date_alias},
                        industry,
                        geo_name,
                        SUM(txn_amt) as total_spend,
                        SUM(txn_cnt) as transaction_count,
                        AVG(avg_ticket) as avg_transaction_value
                    FROM data_lake.master_card
                    {where_clause}
                    GROUP BY {date_alias}, industry, geo_name
                    ORDER BY {order_clause};
                """,
                "parameters": {
                    "date_grouping": ["txn_date", "DATE_TRUNC('day', txn_date)", "DATE_TRUNC('week', txn_date)", "DATE_TRUNC('month', txn_date)"],
                    "date_alias": ["date", "day", "week", "month"],
                    "where_clause": "WHERE txn_date BETWEEN '{start_date}' AND '{end_date}'",
                    "order_clause": ["total_spend DESC", "transaction_count DESC"]
                }
            },
            "combined_analysis": {
                "description": "Combine visitor and spending data for comprehensive analysis",
                "template": """
                    WITH visitor_data AS (
                        SELECT 
                            aoi_date,
                            SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                            SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists
                        FROM data_lake.aoi_days_raw
                        {visitor_where_clause}
                        GROUP BY aoi_date
                    ),
                    spending_data AS (
                        SELECT 
                            txn_date,
                            industry,
                            SUM(txn_amt) as total_spend,
                            SUM(txn_cnt) as transaction_count
                        FROM data_lake.master_card
                        {spending_where_clause}
                        GROUP BY txn_date, industry
                    )
                    SELECT 
                        v.aoi_date,
                        v.swiss_tourists,
                        v.foreign_tourists,
                        s.industry,
                        s.total_spend,
                        s.transaction_count
                    FROM visitor_data v
                    LEFT JOIN spending_data s ON v.aoi_date = s.txn_date
                    ORDER BY v.aoi_date DESC, s.total_spend DESC;
                """,
                "parameters": {
                    "visitor_where_clause": "WHERE aoi_date BETWEEN '{start_date}' AND '{end_date}'",
                    "spending_where_clause": "WHERE txn_date BETWEEN '{start_date}' AND '{end_date}'"
                }
            }
        }
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific table"""
        return self.tables.get(table_name, {})
    
    def get_column_info(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific column"""
        table_info = self.get_table_info(table_name)
        return table_info.get("columns", {}).get(column_name, {})
    
    def get_json_field_info(self, table_name: str, column_name: str, field_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific JSON field"""
        column_info = self.get_column_info(table_name, column_name)
        return column_info.get("fields", {}).get(field_name, {})
    
    def get_relationship(self, source_table: str, target_table: str) -> Dict[str, Any]:
        """Get relationship information between two tables"""
        return self.relationships.get(source_table, {}).get(target_table, {})
    
    def get_query_pattern(self, pattern_name: str) -> Dict[str, Any]:
        """Get a specific query pattern template and parameters"""
        return self.query_patterns.get(pattern_name, {})
    
    def get_schema_context(self, question: str) -> str:
        """Generate a schema context string based on the user's question"""
        # Analyze question to determine relevant tables and fields
        relevant_tables = self._get_relevant_tables(question)
        
        # Build context string
        context = []
        
        for table_name in relevant_tables:
            table_info = self.get_table_info(table_name)
            if not table_info:
                continue
                
            full_table_name = f"{table_info['schema']}.{table_name}"
            context.append(f"Table: {full_table_name}")
            context.append(f"Description: {table_info.get('description', '')}")
            context.append("Columns:")
            
            for col_name, col_info in table_info.get("columns", {}).items():
                context.append(f"  - {col_name}: {col_info.get('type')} - {col_info.get('description', '')}")
                
                # Add JSON field information if applicable
                if col_info.get("type") == "jsonb" and "fields" in col_info:
                    context.append("    JSON fields:")
                    for field_name, field_info in col_info["fields"].items():
                        context.append(f"      * {field_name}: {field_info.get('type')} - {field_info.get('description', '')}")
                    
                    if "access_pattern" in col_info:
                        context.append(f"    Access pattern: {col_info['access_pattern']}")
                    if "cast_pattern" in col_info:
                        context.append(f"    Cast pattern: {col_info['cast_pattern']}")
            
            context.append("")
        
        # Add relationship information
        context.append("Relationships:")
        for source in relevant_tables:
            for target in relevant_tables:
                if source != target:
                    relationship = self.get_relationship(source, target)
                    if relationship:
                        context.append(f"  - {source} to {target}: {relationship.get('type', '')}")
                        context.append(f"    Join conditions: {', '.join(relationship.get('join_conditions', []))}")
                        context.append(f"    Description: {relationship.get('description', '')}")
                        context.append("")
        
        return "\n".join(context)
    
    def _get_relevant_tables(self, question: str) -> List[str]:
        """Determine which tables are relevant to the user's question"""
        question = question.lower()
        relevant_tables = set()
        
        # Check for visitor-related keywords
        visitor_keywords = ["visitor", "tourist", "swiss", "foreign", "domestic", "international"]
        for keyword in visitor_keywords:
            if keyword in question:
                relevant_tables.add("aoi_days_raw")
                break
        
        # Check for spending-related keywords
        spending_keywords = ["spend", "transaction", "purchase", "industry", "payment"]
        for keyword in spending_keywords:
            if keyword in question:
                relevant_tables.add("master_card")
                break
        
        # If no tables are found to be relevant, include both as a fallback
        if not relevant_tables:
            relevant_tables = {"aoi_days_raw", "master_card"}
        
        return list(relevant_tables) 