from typing import Dict, Any, List, Optional, Union
import logging
import re
from datetime import datetime
from .intent_parser import IntentParser, QueryIntent, TimeGranularity
from .schema_context_manager import SchemaContextManager

logger = logging.getLogger(__name__)

class QueryRouter:
    """
    Routes and processes queries to the appropriate tables based on intent detection.
    Supports multi-table queries by identifying relationships and joining tables.
    """
    
    def __init__(self, sql_generator):
        self.sql_generator = sql_generator
        
        # Define known tables and their relationships
        self.tables = {
            "visitor_data": {
                "primary_table": "data_lake.aoi_days_raw",
                "join_tables": [],
                "keywords": ["visitor", "tourist", "attendance", "demographic", "age", "gender", "origin", "country"]
            },
            "transaction_data": {
                "primary_table": "data_lake.master_card",
                "join_tables": [],
                "keywords": ["spend", "transaction", "purchase", "industry", "ticket", "payment"]
            },
            "combined_analysis": {
                "primary_table": "data_lake.aoi_days_raw",
                "join_tables": ["data_lake.master_card"],
                "join_conditions": {
                    "data_lake.master_card": "aoi_days_raw.aoi_date = master_card.txn_date"
                },
                "keywords": ["correlation", "impact", "relation", "compare spending", "visitor spending", "expenditure by tourists"]
            }
        }
    
    def detect_query_type(self, message: str) -> Dict[str, Any]:
        """
        Detect the type of query based on message content
        Returns information needed to route the query
        """
        message = message.lower()
        
        # Initialize result with default values
        result = {
            "primary_table": "data_lake.master_card",  # Default to master_card for spending queries
            "secondary_tables": [],
            "join_conditions": {},
            "is_multi_table": False
        }
        
        # Check for multi-table queries first
        for query_type, info in self.tables.items():
            if query_type == "combined_analysis":
                # Look for keywords indicating a relationship between visitors and transactions
                related_keywords = 0
                for keyword in info["keywords"]:
                    if keyword.lower() in message:
                        related_keywords += 1
                
                # If enough related keywords are found, it's likely a multi-table query
                if related_keywords >= 2:
                    result["primary_table"] = info["primary_table"]
                    result["secondary_tables"] = info["join_tables"]
                    result["join_conditions"] = info["join_conditions"]
                    result["is_multi_table"] = True
                    return result
            
            # Check for single-table queries
            keyword_matches = sum(1 for keyword in info["keywords"] if keyword.lower() in message)
            if keyword_matches > 0:
                result["primary_table"] = info["primary_table"]
                return result
        
        # If no specific table is detected but the message contains spending-related terms
        spending_terms = ["spending", "spent", "expense", "cost", "amount", "revenue", "income"]
        if any(term in message for term in spending_terms):
            result["primary_table"] = "data_lake.master_card"
        
        return result

class SQLGenerator:
    """
    Generates SQL queries based on parsed user intents and schema context.
    """
    
    def __init__(self):
        """Initialize the SQL generator with intent parser and schema context"""
        self.intent_parser = IntentParser()
        self.schema_context = SchemaContextManager()
    
    def generate_sql_query(self, user_message: str) -> Dict[str, Any]:
        """
        Generate a SQL query based on the user's message
        Returns a dictionary containing the query and metadata
        """
        try:
            # Parse the user's intent
            parsed_intent = self.intent_parser.parse_query_intent(user_message)
            
            if parsed_intent.get("error"):
                logger.error(f"Intent parsing error: {parsed_intent.get('error')}")
                return {"error": parsed_intent["error"]}
            
            logger.debug(f"Parsed intent: {parsed_intent}")
            
            # Generate our own SQL components based on the parsed intent
            sql_components = self._generate_sql_components(parsed_intent)
            
            # Build the complete SQL query
            query_parts = []
            
            # Add each component in the correct order
            if "select_clause" in sql_components:
                query_parts.append(sql_components["select_clause"].strip())
            if "from_clause" in sql_components and sql_components["from_clause"]:
                query_parts.append(sql_components["from_clause"].strip())
            if "where_clause" in sql_components and sql_components["where_clause"]:
                query_parts.append(sql_components["where_clause"].strip())
            if "group_by_clause" in sql_components and sql_components["group_by_clause"]:
                query_parts.append(sql_components["group_by_clause"].strip())
            if "order_by_clause" in sql_components and sql_components["order_by_clause"]:
                query_parts.append(sql_components["order_by_clause"].strip())
            if "limit_clause" in sql_components and sql_components["limit_clause"]:
                query_parts.append(sql_components["limit_clause"].strip())
            
            # Join all parts with newlines
            complete_query = "\n".join(query_parts)
            
            # Determine the table name safely
            table = "unknown"
            if "from_clause" in sql_components and sql_components["from_clause"]:
                from_parts = sql_components["from_clause"].split()
                if len(from_parts) > 0:
                    table = from_parts[-1]
            elif parsed_intent["intent"] == QueryIntent.PEAK_PERIOD or parsed_intent["intent"] == QueryIntent.VISITOR_COUNT:
                # For peak period or visitor count, we know it's using aoi_days_raw
                table = "data_lake.aoi_days_raw"
            elif parsed_intent["intent"] == QueryIntent.SPENDING_ANALYSIS:
                # For spending analysis, we know it's using master_card
                table = "data_lake.master_card"
            
            # Convert Enum objects to strings for serialization
            intent_str = parsed_intent["intent"].value if hasattr(parsed_intent["intent"], "value") else str(parsed_intent["intent"])
            granularity_str = parsed_intent["granularity"].value if hasattr(parsed_intent["granularity"], "value") else str(parsed_intent["granularity"])
            
            result = {
                "query": complete_query,
                "intent": intent_str,
                "metadata": {
                    "time_range": parsed_intent["time_range"],
                    "granularity": granularity_str,
                    "table": table
                }
            }
            
            if parsed_intent.get("comparison_type"):
                result["metadata"]["comparison_type"] = parsed_intent["comparison_type"]
                
            logger.debug(f"Generated query result: {result}")
            return result
            
        except IndexError as e:
            logger.error(f"Index error in generate_sql_query: {str(e)}")
            # Get the traceback information
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"error": f"Index error: {str(e)}"}
        except Exception as e:
            logger.error(f"Error generating SQL query: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"error": str(e)}
    
    def validate_query(self, query_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate the generated query against schema context
        Returns the query info with any validation errors
        """
        try:
            if "error" in query_info:
                return query_info
            
            # Get table name from metadata
            table_name = query_info.get("metadata", {}).get("table", "unknown")
            
            if table_name == "unknown":
                return {"error": "Unable to determine table name for validation"}
            
            table_info = self.schema_context.get_table_info(table_name)
            
            if not table_info:
                return {"error": f"Unknown table: {table_name}"}
            
            # Add validation info to query metadata
            if "metadata" not in query_info:
                query_info["metadata"] = {}
                
            query_info["metadata"]["validated"] = True
            query_info["metadata"]["table_info"] = table_info
            
            return query_info
        except Exception as e:
            logger.error(f"Error validating query: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"error": str(e)}
    
    def format_query(self, query: str) -> str:
        """Format the SQL query with proper indentation and line breaks"""
        try:
            # Simple formatting - split on keywords and rejoin with proper spacing
            keywords = ["SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"]
            lines = query.split("\n")
            formatted_lines = []
            
            for line in lines:
                line = line.strip()
                # Add proper indentation for keywords
                for keyword in keywords:
                    if keyword in line.upper():
                        if keyword == "SELECT":
                            formatted_lines.append(line)
                        else:
                            formatted_lines.append(f"\n{line}")
                        break
                else:
                    # Indent non-keyword lines
                    formatted_lines.append(f"    {line}")
            
            return "\n".join(formatted_lines)
        except Exception as e:
            logger.error(f"Error formatting query: {str(e)}")
            return query  # Return original query if formatting fails

    def _generate_sql_components(self, parsed_intent: Dict[str, Any]) -> Dict[str, str]:
        """Generate SQL components based on parsed intent"""
        components = {}
        where_clause = self._build_where_clause(parsed_intent)
        
        if parsed_intent["intent"] == QueryIntent.SPENDING_ANALYSIS:
            components["select_clause"] = """
                WITH spending_data AS (
                    SELECT 
                        industry,
                        SUM(txn_amt) as total_spending,
                        SUM(txn_cnt) as transaction_count,
                        CAST(AVG(txn_amt / NULLIF(txn_cnt, 0)) AS DECIMAL(10,2)) as average_transaction
                    FROM data_lake.master_card
                    """ + where_clause.replace("aoi_date", "txn_date") + """
                    GROUP BY industry
                )
                SELECT 
                    industry,
                    total_spending,
                    transaction_count,
                    average_transaction,
                    CAST(100.0 * total_spending / (SELECT SUM(total_spending) FROM spending_data) AS DECIMAL(5,2)) as percentage_of_total
                FROM spending_data
                ORDER BY total_spending DESC
                LIMIT 10
            """
            components["from_clause"] = ""  # Already included in CTE
            components["where_clause"] = ""  # Already included in CTE
            components["group_by_clause"] = ""  # Already included in CTE
            components["order_by_clause"] = ""  # Already included in CTE
            components["limit_clause"] = ""  # Already included in CTE
        
        elif parsed_intent["intent"] == QueryIntent.VISITOR_COUNT:
            # Generate query for visitor count analysis
            date_format = self._get_date_format_for_granularity(parsed_intent.get("granularity", TimeGranularity.DAY))
            
            components["select_clause"] = f"""
                SELECT 
                    {date_format} as date,
                    SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                    SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                    SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) as total_visitors
            """
            components["from_clause"] = "FROM data_lake.aoi_days_raw"
            components["where_clause"] = where_clause
            components["group_by_clause"] = "GROUP BY date"
            components["order_by_clause"] = "ORDER BY date"
            components["limit_clause"] = "LIMIT 100"
            
        elif parsed_intent["intent"] == QueryIntent.PEAK_PERIOD:
            # Generate query to find peak periods
            date_format = self._get_date_format_for_granularity(parsed_intent.get("granularity", TimeGranularity.DAY))
            
            components["select_clause"] = f"""
                WITH visitor_stats AS (
                    SELECT 
                        {date_format}::date as date,
                        SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                        SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                        SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) as total_visitors
                    FROM data_lake.aoi_days_raw
                    {where_clause}
                    GROUP BY date
                )
                SELECT 
                    date,
                    swiss_tourists,
                    foreign_tourists,
                    total_visitors,
                    RANK() OVER (ORDER BY total_visitors DESC) as visitor_rank
                FROM visitor_stats
                ORDER BY visitor_rank
                LIMIT 10
            """
            components["from_clause"] = ""  # Already included in CTE
            components["where_clause"] = ""  # Already included in CTE
            components["group_by_clause"] = ""  # Already included in CTE
            components["order_by_clause"] = ""  # Already included in CTE
            components["limit_clause"] = ""  # Already included in CTE
            
        else:  # Default to visitor trend analysis
            date_format = self._get_date_format_for_granularity(parsed_intent.get("granularity", TimeGranularity.DAY))
            
            components["select_clause"] = f"""
                SELECT 
                    {date_format} as date,
                    SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                    SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                    SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) as total_visitors
            """
            components["from_clause"] = "FROM data_lake.aoi_days_raw"
            components["where_clause"] = where_clause
            components["group_by_clause"] = "GROUP BY date"
            components["order_by_clause"] = "ORDER BY date"
            components["limit_clause"] = "LIMIT 100"
        
        return components
    
    def _get_date_format_for_granularity(self, granularity: TimeGranularity) -> str:
        """Get the appropriate date format expression for the specified granularity"""
        if granularity == TimeGranularity.DAY:
            return "aoi_date"
        elif granularity == TimeGranularity.WEEK:
            return "DATE_TRUNC('week', aoi_date)"
        elif granularity == TimeGranularity.MONTH:
            return "DATE_TRUNC('month', aoi_date)"
        elif granularity == TimeGranularity.SEASON:
            return "DATE_TRUNC('month', aoi_date)"  # Approximation for season
        elif granularity == TimeGranularity.YEAR:
            return "DATE_TRUNC('year', aoi_date)"
        else:
            return "aoi_date"
    
    def _build_where_clause(self, parsed_intent: Dict[str, Any]) -> str:
        """Build the WHERE clause based on the parsed intent"""
        where_parts = []
        
        # Add time range conditions if available
        time_range = parsed_intent.get("time_range", {})
        if isinstance(time_range, dict):
            if time_range.get("start_date"):
                where_parts.append(f"aoi_date >= '{time_range['start_date']}'")
            if time_range.get("end_date"):
                where_parts.append(f"aoi_date < '{time_range['end_date']}'")
        
        # Build the complete WHERE clause
        if where_parts:
            return "WHERE " + " AND ".join(where_parts)
        return "" 