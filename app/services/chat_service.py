from typing import Dict, Any, Optional, List, AsyncGenerator, Tuple
from sqlalchemy.orm import Session
from ..db.database import SessionLocal, DatabaseService, get_db
from ..llm.openai_adapter import OpenAIAdapter
from ..schemas.chat import ChatMessage, ChatResponse
from ..db.schema_manager import SchemaManager
from sqlalchemy import text, Result
import pandas as pd
import logging
import traceback
import asyncio
from .conversation_service import ConversationService
import json
import datetime
import time
import decimal  # Add this import for decimal handling
from fastapi import HTTPException, Depends
from ..core.config import settings
from decimal import Decimal
import uuid
from ..utils.sql_utils import extract_sql_query, clean_sql_query
from ..utils.sql_formatter import format_sql
import re
import psycopg2
import plotly.graph_objects as go
import plotly.express as px
from ..utils.visualization import generate_visualization, DateTimeEncoder
from ..utils.sql_generator import SQLGenerator
from ..utils.db_utils import execute_query
from ..utils.analysis_generator import generate_analysis_summary
from ..utils.intent_parser import QueryIntent
from .database_service import DatabaseService
from ..utils.intent_parser import IntentParser
from ..models.chat_request import ChatRequest
from functools import lru_cache
from .geo_insights_service import GeoInsightsService
from .geo_visualization_service import GeoVisualizationService
from ..utils.hybrid_intent_parser import HybridIntentParser
from app.db.database import get_dw_db
from app.rag.dw_context_service import DWContextService
from app.agents.agent_service import DWAnalyticsAgent
from app.rag.debug_service import DebugService
from app.services.tourism_region_service import TourismRegionService
from app.services.sql_generation_service import SQLGenerationService
from app.services.visualization_service import VisualizationService
from app.services.schema_service import SchemaService
from app.services.response_generation_service import ResponseGenerationService
# Import VisitorAnalysisService if it exists
# from .visitor_analysis_service import VisitorAnalysisService

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ChatService:
    def __init__(
        self,
        schema_service: Optional[SchemaService] = None,
        dw_context_service: Optional[DWContextService] = None,
        llm_adapter: Optional[OpenAIAdapter] = None
    ):
        """Initialize ChatService with required dependencies"""
        self.schema_service = schema_service or SchemaService()
        self.dw_context_service = dw_context_service
        self.llm_adapter = llm_adapter

        # Initialize debug service
        self.debug_service = DebugService()

        # Initialize DatabaseService (no params needed)
        self.db_service = DatabaseService()

        # Initialize SQL formatter
        self.sql_formatter = None

        # Initialize modular services for LangChain-style flow
        self.sql_generation_service = SQLGenerationService(
            llm_adapter=self.llm_adapter, debug_service=self.debug_service)
        self.visualization_service = VisualizationService(self.debug_service)
        self.response_generation_service = ResponseGenerationService(
            llm_adapter=self.llm_adapter, debug_service=self.debug_service)

        # Initialize other supporting services
        self.tourism_region_service = TourismRegionService()

        # Set up cache for query results
        self.query_cache = {}
        self.query_cache_ttl = 3600  # Cache results for 1 hour

        logger.info("ChatService initialized successfully")

    async def process_chat_stream(
        self,
        message: str,
        session_id: str,
        is_direct_query: bool = False,
        message_id: Optional[str] = None,
        dw_db: Session = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Process a chat message and return a streaming response.
        """
        current_step_name = "initialization"
        try:
            # Initialize flow
            if message_id is None:
                message_id = self.debug_service.start_flow(session_id)
            else:
                self.debug_service.start_flow(session_id, message_id=message_id)

            # Start streaming
            yield {"type": "start"}
            yield {"type": "message_id", "message_id": message_id}
            yield {"type": "content_start"}
            yield {"type": "content", "content": "Analyzing your question..."}

            # 1. Get context
            current_step_name = "context_retrieval"
            self.debug_service.start_step(current_step_name)
            schema_context = await self.schema_service.get_schema_context()
            self.debug_service.end_step(current_step_name, success=True)

            # 2. Generate SQL
            current_step_name = "sql_generation"
            self.debug_service.start_step(current_step_name)
            sql_query = await self.sql_generation_service.generate_query(message, schema_context)
            yield {"type": "sql_query", "sql_query": sql_query}
            self.debug_service.end_step(current_step_name, success=True)

            # 3. Execute SQL
            current_step_name = "sql_execution"
            self.debug_service.start_step(current_step_name)
            sql_results = await self.db_service.execute_query_async(sql_query)
            processed_results = self._process_sql_results(sql_results)
            yield {"type": "sql_results", "sql_results": processed_results}
            self.debug_service.end_step(current_step_name, success=True)

            # 4. Generate visualization
            current_step_name = "visualization"
            self.debug_service.start_step(current_step_name)
            visualization = self.visualization_service.create_visualization(processed_results, message)
            if visualization:
                if visualization.get('type') == 'plotly_json':
                    yield {"type": "plotly_json", "data": visualization.get('data', {})}
                else:
                    yield {"type": "visualization", "visualization": visualization}
            self.debug_service.end_step(current_step_name, success=True)

            # 5. Generate response
            current_step_name = "response_generation"
            self.debug_service.start_step(current_step_name)
            response = await self.response_generation_service.generate_response(
                query=message,
                sql_query=sql_query,
                sql_results=processed_results,
                visualization_info=visualization,
                context={"schema_context": schema_context}
            )
            yield {"type": "content", "content": response}
            self.debug_service.end_step(current_step_name, success=True)

            # 6. Add debug info
            debug_info = self.debug_service.get_flow_info()
            yield {"type": "debug_info", "debug_info": debug_info}

            # End stream
            yield {"type": "end"}

        except Exception as e:
            logger.error(f"Error in chat stream: {str(e)}")
            logger.error(traceback.format_exc())
            self.debug_service.end_step(current_step_name, success=False, error=str(e))
            yield {"type": "error", "error": str(e)}
            yield {"type": "end"}
        finally:
            self.debug_service.end_flow()

    def is_conversational_message(self, message: str) -> bool:
        """Detect if a message is conversational rather than a data query"""
        # Clean and normalize the message
        message = message.strip().lower()

        # Define greetings that should trigger conversational response
        pure_greetings = [
            "hi",
            "hello",
            "hey",
            "greetings",
            "hi there",
            "hello there",
            "thanks",
            "thank you",
            "goodbye",
            "bye",
            "good morning",
            "good afternoon",
            "good evening"]

        # Check if the message is EXACTLY a greeting
        if message in pure_greetings:
            return True

        # Check if it looks like a question about data (not just a greeting)
        question_words = [
            "what",
            "which",
            "where",
            "how",
            "when",
            "who",
            "show",
            "list",
            "find",
            "tell",
            "give",
            "display",
            "query",
            "analyze",
            "get",
            "calculate"]

        data_related_terms = [
            "industry",
            "visitor",
            "spending",
            "tourism",
            "busiest",
            "week",
            "day",
            "month",
            "year",
            "quarter",
            "pattern",
            "trend",
            "statistics",
            "data",
            "amount",
            "total",
            "average",
            "count",
            "transaction",
            "swiss",
            "foreign",
            "domestic",
            "international",
            "spring",
            "summer",
            "winter",
            "fall",
            "autumn",
            "region",
            "location",
            "period",
            "season",
            "txn",
            "amt"]

        # If it contains question words AND data terms, it's a data query, not
        # just conversation
        for word in question_words:
            if word in message.split():
                for term in data_related_terms:
                    if term in message:
                        return False  # It's a data query, not just conversation

        # If very short and not obviously a data query, treat as conversation
        if len(message.split()) < 3:
            return True

        # Check if it's asking about the bot itself rather than data
        bot_references = ["you", "your", "yourself",
                          "chatbot", "bot", "assistant", "ai"]
        bot_reference_count = sum(
            1 for ref in bot_references if ref in message.split())
        if bot_reference_count > 0 and len(message.split()) < 6:
            return True

        return False

    def is_schema_inquiry(self, message: str) -> bool:
        """Detect if the message is asking about available data or schema"""
        # Clean the message
        cleaned_message = message.lower().strip()

        # Keywords related to schema inquiries
        schema_keywords = [
            "schema",
            "columns",
            "tables",
            "fields",
            "structure",
            "data model",
            "what data",
            "available data",
            "what information",
            "what tables",
            "database schema",
            "field names",
            "column names",
            "available tables",
            "what columns",
            "show me the data",
            "data structure",
            "metadata",
            "what can i ask",
            "what can you tell me about",
            "show me what data"]

        # Check if the message contains schema inquiry keywords
        for keyword in schema_keywords:
            if keyword in cleaned_message:
                return True

        return False

    def get_schema_summary(self) -> str:
        """Generate a user-friendly summary of the database schema"""
        try:
            # This is a simplified implementation - you may need to implement
            # the actual schema retrieval logic based on your schema_manager

            schema_summary = (
                "Here's the data I have available:\n\n"
                "**Visitor Data**\n"
                "- Daily visitor counts for Swiss and foreign tourists\n"
                "- Data available from 2022-2023\n\n"
                "**Transaction Data**\n"
                "- Spending information by industry sector\n"
                "- Geographic spending patterns\n"
                "- Seasonal transaction trends\n\n"
                "You can ask questions like:\n"
                "- What was the busiest week in spring 2023?\n"
                "- Which industry had the highest spending?\n"
                "- How do visitor patterns differ between Swiss and foreign tourists?")

            return schema_summary
        except Exception as e:
            logger.error(f"Error generating schema summary: {str(e)}")
            logger.error(traceback.format_exc())
            return "I can help you analyze tourism data including visitor statistics and transaction data. Please ask a specific question about tourism patterns."

    def _split_into_chunks(self, text: str, chunk_size: int = 1000):
        """Yield successive chunk_size chunks from text."""
        if not text:
            return

        # Log the full response text for debugging
        logger.info(f"Response to be chunked (length {len(text)}): {text}")

        for i in range(0, len(text), chunk_size):
            yield text[i:i + chunk_size]
    
    def close(self):
        """Close any open resources"""
        # Log the closing operation
        logger.info("Closing ChatService resources")

        # Close any other resources that need closing
        if hasattr(self, 'db_service'):
            self.db_service.close()

        logger.info("ChatService resources closed successfully")

    def _process_sql_results(self, result: Any) -> List[Dict[str, Any]]:
        """Convert database query results to a list of JSON-serializable dictionaries.
        Handles both SQLAlchemy Result objects and lists of dictionaries."""
        processed_results = []
        if not result:
            return processed_results

        try:
            # If result is already a list
            if isinstance(result, list):
                if not result:
                    return []

                # Handle list of tuples or other non-dictionary objects
                if not isinstance(result[0], dict):
                    logger.warning(
                        f"Result contains non-dictionary items: {type(result[0])}. Converting...")
                    # Try to convert to dictionaries if possible
                    if isinstance(result[0], (list, tuple)):
                        # For lists or tuples, create dictionaries with
                        # numbered keys
                        for row in result:
                            row_dict = {
                                f"col_{i}": value for i,
                                value in enumerate(row)}
                            processed_results.append(
                                self._process_dict_values(row_dict))
                        return processed_results
                    else:
                        # For other types, wrap each item in a dictionary
                        return [{"value": item} for item in result]

                # If items in the list are already dictionaries, just process
                # their values
                for row in result:
                    processed_results.append(self._process_dict_values(row))
                return processed_results

            # If result is a SQLAlchemy Result object (from db.database)
            if hasattr(result, 'keys'):
                keys = result.keys()
                for row in result.mappings(
                ):  # Iterate through rows as dictionaries
                    row_dict = {}
                    for key in keys:
                        value = row[key]
                        row_dict[key] = value
                    processed_results.append(
                        self._process_dict_values(row_dict))
                return processed_results

            # If we got here, we don't know how to process this result format
            logger.warning(
                f"Unknown result format: {type(result)}. Converting to string representation.")
            # Convert to string and wrap in a dictionary
            return [{"result": str(result)}]

        except Exception as e:
            logger.error(
                f"Error processing SQL results: {str(e)}",
                exc_info=True)
            # Return with error information
            return [{"error": f"Error processing results: {str(e)}"}]

    def _process_dict_values(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Process dictionary values to ensure they are JSON-serializable."""
        processed_dict = {}
        for key, value in row_dict.items():
            if isinstance(value, (datetime.date, datetime.datetime)):
                processed_dict[key] = value.isoformat()
            elif isinstance(value, decimal.Decimal):
                processed_dict[key] = float(value)
            else:
                processed_dict[key] = value
        return processed_dict

    def _get_visualization(
            self, results: List[Dict[str, Any]], query: str) -> Optional[Dict[str, Any]]:
        """Generate visualization based on query results, handling potential errors."""
        try:
            if not results or not self.visualization_service:
                logger.info(
                    "No results or visualization service available to generate visualization.")
                return None

            # Directly use the main visualization service method with built-in
            # error handling
            visualization_data = self.visualization_service.create_visualization(
                results, query)
            if visualization_data:
                vis_type = visualization_data.get('type', 'unknown')
                logger.info(
                    f"Successfully generated visualization of type: {vis_type}")

                # Convert 'plotly' type to 'plotly_json' for the frontend
                if vis_type == 'plotly':
                        return {
                        "type": "plotly_json",
                        "data": visualization_data.get('data', {})
                    }
                return visualization_data
            else:
                logger.warning("Visualization service returned None")
                return None

        except Exception as e:
            logger.error(
                f"Unexpected error during visualization generation: {str(e)}")
            logger.error(traceback.format_exc())

            # Try one more time with simplified data
            try:
                # Simplify results - take only the first 20 rows to reduce
                # complexity
                simplified_results = results[:20] if len(
                    results) > 20 else results

                # Strip any complex nested data that might cause issues
                for row in simplified_results:
                    for key in list(row.keys()):
                        if isinstance(row[key], (dict, list)):
                            row[key] = str(row[key])

                # Use the fallback visualization method directly
                fallback_viz = self.visualization_service._create_fallback_visualization(
                    simplified_results,
                    query,
                    f"Error in primary visualization: {str(e)}"
                )

                # Also check the fallback visualization for Plotly format
                if fallback_viz and fallback_viz.get('type') == 'plotly':
                        return {
                        "type": "plotly_json",
                        "data": fallback_viz.get('data', {})
                    }
                return fallback_viz
            except Exception as fallback_e:
                logger.error(
                    f"Fallback visualization also failed: {str(fallback_e)}")

                # Last resort - return results as a table
                return {
                    "type": "table",
                    "data": results[:10]  # Limit to 10 rows for performance
                }

        return None

    def _determine_query_type(
            self,
            message: str,
            is_direct_query: bool) -> str:
        """Determine the type of query based on the message and the direct_query flag."""
        if is_direct_query:
            return "sql_direct"

        # Check if the message looks like a SQL query
        sql_keywords = [
            "select",
            "from",
            "where",
            "join",
            "group by",
            "order by",
            "having",
            "limit"]
        message_lower = message.lower().strip()

        # If message starts with SELECT and contains other SQL keywords, assume
        # it's a direct SQL query
        if message_lower.startswith("select") and any(
                keyword in message_lower for keyword in sql_keywords[1:]):
            return "sql_direct"

        # Otherwise, treat it as a natural language query
        return "natural_language"

    async def _get_context(self,
                           message: str,
                           is_natural_language: bool = True,
                           dw_db: Session = None) -> Tuple[Optional[str],
                                                           Optional[str]]:
        """Retrieve database schema context and data warehouse insights context"""
        schema_context = None
        dw_context = None
        current_step_name = "context_retrieval"

        try:
            # Get schema context from SchemaService
            self.debug_service.start_step(current_step_name)
            schema_context = None
            dw_context = None
            try:
                # Only get live context if it's a natural language query
                if is_natural_language:
                    # Corrected call: removed dw_db argument
                    schema_context = await self.schema_service.get_schema_context()
                    dw_context = await self.dw_context_service.get_dw_context(query=message) if self.dw_context_service else None

                    # Fallback mechanism if live retrieval fails
                    if not schema_context:
                        logger.warning(
                            "Live schema context retrieval failed, using fallback.")
                        schema_context = self._get_fallback_schema_context()
                    if not dw_context:
                        logger.warning(
                            "DW context retrieval failed, using fallback.")
                        dw_context = self._get_fallback_dw_context()
                else:
                    # For direct SQL, we might not need full context, but log
                    # it
                    logger.info(
                        "Skipping context retrieval for direct SQL query.")

                self.debug_service.end_step(
                    current_step_name,
                    success=True,
                    details={
                        "schema_context_retrieved": schema_context is not None,
                        "schema_length": len(schema_context) if schema_context else 0,
                        "dw_context_keys": list(
                            dw_context.keys()) if dw_context else []})
                return schema_context, dw_context  # Return tuple
            except Exception as step_e:
                logger.error(
                    f"Error retrieving context: {str(step_e)}",
                    exc_info=True)
                # Fallback if ANY error occurs during context retrieval
                logger.warning(
                    "Using fallback context due to error during retrieval.")
                schema_context = self._get_fallback_schema_context()
                dw_context = self._get_fallback_dw_context()
                self.debug_service.end_step(
                    current_step_name,
                    success=False,
                    error=f"Error retrieving context: {str(step_e)}. Used fallback.")
                return schema_context, dw_context  # Return fallback tuple
        
        except Exception as e:
            logger.error(f"General error in _get_context: {str(e)}")
            logger.error(traceback.format_exc())
            # Ensure we return at least the fallback values rather than None
            if not schema_context:
                schema_context = self._get_fallback_schema_context()
            if not dw_context:
                dw_context = self._get_fallback_dw_context()
            return schema_context, dw_context

    def _get_fallback_schema_context(self) -> str:
        """Provide fallback schema context when the actual context can't be retrieved"""
        logger.info("Using fallback schema context")
        return '''
Tables:
  data_lake.aoi_days_raw (
    aoi_id bigint, -- Area of Interest ID
    aoi_date date, -- Date of measurement
    visitor_metrics jsonb -- Visitor metrics in JSON structure
  )
  data_lake.aoi_metadata (
    aoi_id bigint PRIMARY KEY, -- Area of Interest ID
    aoi_name varchar(255), -- Name of the area
    aoi_region varchar(100), -- Region containing the area
    aoi_type varchar(50) -- Type of area (e.g., 'city', 'attraction')
  )
  data_lake.master_card (
    tile_id varchar(24), -- Unique tile identifier in format [lat]_[long]
    txn_date date, -- Date of transaction data
    origin varchar(100), -- Origin of transaction (e.g., 'domestic', 'international')
    spending_metrics jsonb -- Spending metrics in JSON structure
  )
  dw.dim_date (
    date_id bigint PRIMARY KEY, -- Date surrogate key
    full_date date, -- Calendar date
    day_of_week int, -- Day of week (1-7)
    day_name varchar(10), -- Day name (e.g., 'Monday')
    month int, -- Month number (1-12)
    month_name varchar(10), -- Month name (e.g., 'January')
    quarter int, -- Quarter (1-4)
    year int -- Year (e.g., 2023)
  )
  dw.dim_region (
    region_id bigint PRIMARY KEY, -- Region surrogate key
    region_name varchar(255), -- Name of region
    region_type varchar(50), -- Type of region (e.g., 'city', 'canton')
    parent_region_id bigint, -- Foreign key to parent region
    population int, -- Population of region
    area_sqkm float -- Area in square kilometers
  )
  dw.dim_visitor_segment (
    segment_id bigint PRIMARY KEY, -- Visitor segment surrogate key
    segment_name varchar(100), -- Name of segment (e.g., 'Domestic Day Visitor')
    is_domestic boolean, -- Whether visitors are domestic
    is_overnight boolean, -- Whether visitors stay overnight
    segment_description text -- Description of visitor segment
  )
  dw.dim_spending_industry (
    industry_id bigint PRIMARY KEY, -- Industry surrogate key
    industry_name varchar(255), -- Name of industry
    sector varchar(100) -- Broader economic sector
  )
  dw.dim_spending_category (
    category_id bigint PRIMARY KEY, -- Unique spending category identifier
    category_name varchar(255), -- Name of the category (e.g., 'Accommodation', 'Food & Beverage')
    parent_category_id bigint -- Foreign key for hierarchical categories (optional)
  )
'''

    def _get_fallback_dw_context(self) -> Dict[str, Any]:
        """Provide fallback DW context when the actual context can't be retrieved"""
        logger.info("Using fallback DW context")
        return {
            "regions": [
                "Zurich",
                "Geneva",
                "Basel",
                "Bern",
                "Lucerne"],
            "date_range": {
                "min_date": "2023-01-01",
                "max_date": "2023-12-31"},
            "industries": [
                "Retail",
                "Accommodation",
                "Food Service",
                "Transportation"],
            "common_metrics": {
                "visitor_count": "Total visitor count across all categories",
                "spending_amount": "Total spending in CHF",
                "transaction_count": "Number of transactions"}}

    async def _determine_query_intent(self, message: str) -> str:
        """Determine the intent of a query."""
        # Simple implementation for now - can be enhanced with more
        # sophisticated intent detection
        return "data_query"

    def _get_geo_insights_service(self, dw_db: Session) -> GeoInsightsService:
        """Get or create a GeoInsightsService with the provided database session"""
        return GeoInsightsService(dw_db)

    def _get_dw_analytics_agent(self, dw_db: Session) -> DWAnalyticsAgent:
        """Get or create a DWAnalyticsAgent with the provided database session"""
        return DWAnalyticsAgent(dw_db)

    def _attempt_recovery_visualization(
            self, results: List[Dict[str, Any]], message: str, sql_query: str) -> Optional[Dict[str, Any]]:
        """Attempt to recover visualization when the primary visualization fails"""
        try:
            logger.info("Attempting recovery visualization")
            # Check if we have results
            if not results or len(results) == 0:
                logger.warning("No results to visualize in recovery attempt")
                return None
                
            # Create a basic visualization type based on result structure
            visualization = None
            
            # Check if the data has numeric columns for charts
            numeric_cols = []
            for key, value in results[0].items():
                if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                    numeric_cols.append(key)
                    
            # Check if there are date/time columns
            date_cols = []
            for key, value in results[0].items():
                if key.lower() in ['date', 'day', 'month', 'year', 'time', 'datetime', 'period']:
                    date_cols.append(key)
                    
            # If we have both numeric and date columns, try a time series
            if numeric_cols and date_cols:
                logger.info("Recovery: Creating time series visualization")
                visualization = {"type": "time_series", "data": results, "x_field": date_cols[0], "y_field": numeric_cols[0]}
            # If we have multiple numeric columns, try a bar chart
            elif len(numeric_cols) > 1:
                logger.info("Recovery: Creating bar chart visualization")
                visualization = {"type": "bar", "data": results, "fields": numeric_cols[:2]}
            # If we have just one numeric column, try a pie chart
            elif numeric_cols:
                logger.info("Recovery: Creating pie chart visualization")
                non_numeric_cols = [k for k in results[0].keys() if k not in numeric_cols]
                if non_numeric_cols:
                    visualization = {"type": "pie", "data": results, "label_field": non_numeric_cols[0], "value_field": numeric_cols[0]}
            # Otherwise, just use a table
            else:
                logger.info("Recovery: Creating table visualization")
                visualization = {"type": "table", "data": results}
                
            # Check if the visualization is a Plotly chart
            if visualization and visualization.get('type') == 'plotly':
                logger.info("Converting recovery plotly visualization format for frontend")
                return {
                    "type": "plotly_json",
                    "data": visualization.get('data', {})
                }

            return visualization
        except Exception as e:
            logger.error(f"Recovery visualization failed: {str(e)}")
            return None

    def _fix_group_by_error(self, sql_query: str) -> str:
        """Attempt to fix GROUP BY errors in a SQL query"""
        fixed_query = sql_query

        # Pattern to find EXTRACT expressions in SELECT
        extract_pattern = r'EXTRACT\s*\(\s*(\w+)\s+FROM\s+([^\)]+)\)\s+AS\s+(\w+)'

        # Find all EXTRACT expressions
        extracts = re.finditer(extract_pattern, sql_query, re.IGNORECASE)

        for match in extracts:
            extract_type = match.group(1)   # year, month, etc.
            column = match.group(2)         # d.full_date
            alias = match.group(3)          # year, month, alias

            # Check if this alias is used in GROUP BY
            group_by_pattern = rf'GROUP\s+BY\s+[^;]*(^|,|\s){alias}($|,|\s)'

            # If we find the alias in a GROUP BY clause
            if re.search(
                    group_by_pattern,
                    sql_query,
                    re.IGNORECASE | re.MULTILINE):
                # Replace the alias in GROUP BY with the full EXTRACT
                # expression
                fixed_query = re.sub(
                    rf'(GROUP\s+BY\s+[^;]*)(^|,|\s){alias}($|,|\s)',
                    f'\\1\\2EXTRACT({extract_type} FROM {column})\\3',
                    fixed_query,
                    flags=re.IGNORECASE | re.MULTILINE
                )

        # Try to find and fix issues with CTEs and GROUP BY
        if "WITH" in fixed_query.upper() and "GROUP BY" in fixed_query.upper():
            # Split into CTEs and main query
            parts = re.split(
                r'SELECT',
                fixed_query,
                flags=re.IGNORECASE,
                maxsplit=1)
            if len(parts) > 1:
                cte_part = parts[0]
                main_part = "SELECT" + parts[1]

                # Check if there are EXTRACT expressions in CTEs that need
                # fixing
                cte_extracts = re.finditer(
                    extract_pattern, cte_part, re.IGNORECASE)

                for match in cte_extracts:
                    extract_type = match.group(1)
                    column = match.group(2)
                    alias = match.group(3)

                    # Look for group by with this alias in CTE part
                    cte_group_pattern = rf'GROUP\s+BY\s+[^)]*\b{alias}\b'
                    if re.search(cte_group_pattern, cte_part, re.IGNORECASE):
                        cte_part = re.sub(
                            rf'(GROUP\s+BY\s+[^)]*)(\b{alias}\b)',
                            f'\\1EXTRACT({extract_type} FROM {column})',
                            cte_part,
                            flags=re.IGNORECASE
                        )

                # Recombine the query
                fixed_query = cte_part + main_part

        return fixed_query
