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
from datetime import datetime, date
import time
import decimal
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


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class ChatService:
    def __init__(
        self,
        schema_service: Optional[SchemaService] = None,
        dw_context_service: Optional[DWContextService] = None,
        llm_adapter: Optional[OpenAIAdapter] = None,
        sql_execution_service = None,
        visualization_service = None,
        response_generation_service = None,
        debug_service: Optional[DebugService] = None
    ):
        """Initialize the chat service with required dependencies"""
        try:
            # Initialize debug service first
            self.debug_service = debug_service

            # Initialize schema service with fallback
            self.schema_service = schema_service or SchemaService()

            # Initialize other services
            self.dw_context_service = dw_context_service
            self.llm_adapter = llm_adapter or OpenAIAdapter()
            self.db_service = DatabaseService()
            self.sql_execution_service = sql_execution_service or SQLExecutionService()
            self.visualization_service = visualization_service or VisualizationService(self.debug_service)
            self.response_generation_service = response_generation_service or ResponseGenerationService(self.llm_adapter, self.debug_service)

            # Initialize modular services for LangChain-style flow
            self.sql_generation_service = SQLGenerationService(
                llm_adapter=self.llm_adapter, debug_service=self.debug_service)
            self.tourism_region_service = TourismRegionService()

            # Set up cache for query results
            self.query_cache = {}
            self.query_cache_ttl = 3600  # Cache results for 1 hour

            # Initialize lock for async initialization
            self._initialization_lock = asyncio.Lock()
            self._initialized = False

            logger.info("ChatService initialized successfully")
        except Exception as e:
            logger.error(f"Error during ChatService initialization: {str(e)}")
            raise

    async def initialize(self):
        """Initialize the chat service asynchronously"""
        if self._initialized:
            return

        async with self._initialization_lock:
            if self._initialized:
                return

            try:
                # Initialize schema service
                await self.schema_service.initialize()
                self._initialized = True
                logger.info("Chat service async initialization completed")
            except Exception as e:
                logger.error(f"Failed to initialize chat service: {str(e)}")
                raise

    async def process_chat(
        self,
        message: str,
        session_id: str,
        debug_service: Optional[DebugService] = None,
        is_direct_query: bool = False,
        dw_db: Session = None
    ) -> Dict[str, Any]:
        """
        Process a chat message for the non-streaming endpoint.
        
        Args:
            message: The message to process
            session_id: The session ID for tracking
            debug_service: Optional debug service for tracking debug information
            is_direct_query: Whether this is a direct SQL query
            dw_db: Optional database session
            
        Returns:
            A dictionary containing the response information
        """
        message_id = str(uuid.uuid4())
        self.debug_service = debug_service
        
        try:
            # Detect query type
            query_type = self._determine_query_type(message, is_direct_query)
            is_natural_language = query_type == "natural_language"
            
            # Get context for this query if it's a natural language query
            schema_context, dw_context = await self._get_context(
                message, is_natural_language, dw_db
            )
            
            # Generate SQL query
            sql_generation_step = "sql_generation"
            self.debug_service.start_step(sql_generation_step)
            
            sql_query = None
            if is_natural_language:
                sql_query = await self.sql_generation_service.generate_query(
                    message, schema_context, dw_context
                )
                
                # Attempt to fix common GROUP BY errors first
                if sql_query:
                    original_sql = sql_query
                    sql_query = self._fix_group_by_error(sql_query)
                    if sql_query != original_sql:
                         self.debug_service.add_step_details({"sql_groupby_fix_applied": True, "original_sql": original_sql, "fixed_sql_groupby": sql_query})

                # WORKAROUND: Correct specific SQL pattern for monthly tourist comparison
                problematic_pattern_1 = "SUM(f.swiss_tourists)"
                problematic_pattern_2 = "SUM(f.foreign_tourists)"
                monthly_comparison_keywords = ["month", "swiss", "international", "foreign", "tourist", "bar chart", "visualise"]
                
                if (sql_query and 
                    problematic_pattern_1 in sql_query and 
                    problematic_pattern_2 in sql_query and 
                    all(keyword in message.lower() for keyword in monthly_comparison_keywords)):
                    
                    logger.warning(f"Applying workaround for generated SQL: Replaced numeric sum with JSONB extraction for query: {message}")
                    correct_sql = """
                    SELECT d.year, d.month, d.month_name,
                           SUM((f.demographics->>'swiss_tourists')::numeric) as swiss_tourists,
                           SUM((f.demographics->>'foreign_tourists')::numeric) as foreign_tourists
                    FROM dw.fact_visitor f
                    JOIN dw.dim_date d ON f.date_id = d.date_id
                    WHERE d.year = 2023 -- Assuming 2023, adjust if year needs to be dynamic
                    GROUP BY d.year, d.month, d.month_name
                    ORDER BY d.year, d.month;
                    """
                    sql_query = correct_sql
                    self.debug_service.add_step_details({"sql_workaround_applied": True, "corrected_sql": sql_query})
                else:
                    self.debug_service.add_step_details({"sql_workaround_applied": False})

                self.debug_service.add_step_details({"generated_sql": sql_query})
            else:
                # For direct SQL queries, use the message as the query
                sql_query = message
                self.debug_service.add_step_details({"sql": "Direct SQL input"})
            
            self.debug_service.end_step(sql_generation_step)
            
            # Execute SQL query
            sql_execution_step = "sql_execution"
            self.debug_service.start_step(sql_execution_step)
            
            results = None
            try:
                results = await self.sql_execution_service.execute_query(sql_query, dw_db)
                self.debug_service.add_step_details({"row_count": len(results) if results else 0})
            except Exception as e:
                self.debug_service.end_step(sql_execution_step, success=False, error=str(e))
                raise
            
            self.debug_service.end_step(sql_execution_step)
            
            # Generate visualization
            visualization_step = "visualization"
            self.debug_service.start_step(visualization_step)
            
            visualization = None
            # Check if visualization is requested, with specific attention to bar chart requests
            visualization_requested = ("chart" in message.lower() or 
                                      "visual" in message.lower() or 
                                      "graph" in message.lower() or
                                      "bar" in message.lower() or
                                      "plot" in message.lower())
            
            if results and visualization_requested:
                # First try the specialized tourist comparison for Swiss vs. foreign tourists
                if ("swiss" in message.lower() and 
                    ("international" in message.lower() or "foreign" in message.lower()) and 
                    "tourist" in message.lower() and 
                    "month" in message.lower()):
                    
                    logger.info("Creating monthly Swiss vs international tourist comparison visualization")
                    visualization = self.visualization_service.create_monthly_tourist_comparison(results, message)
                
                # If specialized visualization didn't work, use standard visualization
                if not visualization:
                    visualization = self.visualization_service.create_visualization(results, message)
                
                # Add visualization details to debug info
                if visualization:
                    self.debug_service.add_step_details({
                        "visualization_type": visualization.get("type", "unknown"),
                        "visualization_created": True
                    })
                else:
                    self.debug_service.add_step_details({
                        "visualization_created": False,
                        "reason": "Visualization service failed to create visualization"
                    })
            else:
                self.debug_service.add_step_details({
                    "visualization_created": False,
                    "reason": "No results or visualization not requested"
                })
            
            self.debug_service.end_step(visualization_step)
            
            # Generate response
            response_step = "response_generation"
            self.debug_service.start_step(response_step)
            
            # Use the intent detection if available
            intent = await self._determine_query_intent(message) if is_natural_language else "direct_sql"
            
            # Generate content
            content = await self.response_generation_service.generate_response(
                query=message,
                sql_query=sql_query,
                sql_results=results,
                intent=intent,
                visualization_info=visualization,
                context={"schema_context": schema_context}
            )
            
            self.debug_service.end_step(response_step)
            
            # Prepare final response
            response = {
                "message_id": message_id,
                "message": message,
                "content": content,
                "sql_query": sql_query,
                "status": "success"
            }
            
            # Add visualization if available
            if visualization:
                response["visualization"] = visualization
            
            # Add debug info if available
            if self.debug_service:
                response["debug_info"] = self.debug_service.get_flow_info()
            
            return response

        except Exception as e:
            logger.error(f"Error in process_chat: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Provide error information
            error_response = {
                "message_id": message_id,
                "message": message,
                "content": "An error occurred while processing your request. Please try again.",
                "status": "error",
                "error": str(e)
            }
            
            # Add debug info if available
            if self.debug_service:
                try:
                    debug_info = self.debug_service.get_flow_info()
                    error_response["debug_info"] = debug_info
                except Exception as debug_err:
                    logger.error(f"Error getting debug info: {str(debug_err)}")
            
            return error_response

    async def process_chat_stream(self, message: str, session_id: str, dw_db: Session = None, is_direct_query: bool = False) -> AsyncGenerator[Dict[str, Any], None]:
        """Process a chat message and stream results back."""
        message_id = str(uuid.uuid4())
        # Instantiate DebugService correctly
        debug_service = DebugService()
        # Set the debug service instance for this request
        self.debug_service = debug_service
        # Start the flow with session and message IDs
        debug_service.start_flow(session_id=session_id, message_id=message_id)

        try:
            await self.initialize() # Ensure service is initialized
            debug_service.start_step("process_chat_stream")
            yield {"type": "status", "message_id": message_id, "status": "Processing started"}

            # --- Step 1: Determine Query Type & Get Context ---
            debug_service.start_step("context_retrieval")
            query_type = self._determine_query_type(message, is_direct_query) # Assuming stream is always NL
            is_natural_language = query_type == "natural_language"
            schema_context, dw_context = await self._get_context(message, is_natural_language, dw_db)
            debug_service.add_step_details({
                "query_type": query_type,
                "schema_context_retrieved": schema_context is not None,
                "dw_context_keys": list(dw_context.keys()) if dw_context else []
            })
            debug_service.end_step("context_retrieval")
            yield {"type": "status", "status": "Context retrieved"}

            # --- Step 2: Generate SQL Query ---
            debug_service.start_step("sql_generation")
            sql_query = None
            if is_natural_language:
                yield {"type": "status", "status": "Generating SQL query..."}
                sql_query = await self.sql_generation_service.generate_query(
                    message, schema_context, dw_context
                )
                
                # Attempt to fix common GROUP BY errors first
                if sql_query:
                    original_sql = sql_query
                    sql_query = self._fix_group_by_error(sql_query)
                    if sql_query != original_sql:
                         debug_service.add_step_details({"sql_groupby_fix_applied": True, "original_sql": original_sql, "fixed_sql_groupby": sql_query})

                # WORKAROUND: Correct specific SQL pattern for monthly tourist comparison
                problematic_pattern_1 = "SUM(f.swiss_tourists)"
                problematic_pattern_2 = "SUM(f.foreign_tourists)"
                monthly_comparison_keywords = ["month", "swiss", "international", "foreign", "tourist", "bar chart", "visualise"]
                if (sql_query and
                    problematic_pattern_1 in sql_query and
                    problematic_pattern_2 in sql_query and
                    all(keyword in message.lower() for keyword in monthly_comparison_keywords)):
                    logger.warning(f"[Stream] Applying workaround for generated SQL: Replaced numeric sum with JSONB extraction for query: {message}")
                    correct_sql = """
                    SELECT d.year, d.month, d.month_name,
                           SUM((f.demographics->>'swiss_tourists')::numeric) as swiss_tourists,
                           SUM((f.demographics->>'foreign_tourists')::numeric) as foreign_tourists
                    FROM dw.fact_visitor f
                    JOIN dw.dim_date d ON f.date_id = d.date_id
                    WHERE d.year = 2023 -- Assuming 2023, adjust if year needs to be dynamic
                    GROUP BY d.year, d.month, d.month_name
                    ORDER BY d.year, d.month;
                    """
                    sql_query = correct_sql
                    self.debug_service.add_step_details({"sql_workaround_applied": True, "corrected_sql": sql_query})
                else:
                    self.debug_service.add_step_details({"sql_workaround_applied": False})

                self.debug_service.add_step_details({"generated_sql": sql_query})
                yield {"type": "sql_query", "sql_query": sql_query}
            else:
                sql_query = message # Direct SQL
                yield {"type": "sql_query", "sql_query": sql_query}
            debug_service.end_step("sql_generation")

            # --- Step 3: Execute SQL Query ---
            debug_service.start_step("sql_execution")
            yield {"type": "status", "status": "Executing SQL query..."}
            results = None
            try:
                results = await self.sql_execution_service.execute_query(sql_query, dw_db)
                debug_service.add_step_details({"row_count": len(results) if results else 0})
                yield {"type": "sql_results", "results_preview": results[:5] if results else []} # Send preview
            except Exception as e:
                logger.error(f"Error executing SQL: {e}")
                debug_service.end_step("sql_execution", success=False, error=str(e))
                yield {"type": "error", "content": f"Error executing SQL: {str(e)}"}
                return
            debug_service.end_step("sql_execution")

            # --- Step 4: Generate Visualization ---
            debug_service.start_step("visualization")
            visualization = None
            visualization_requested = ("chart" in message.lower() or "visual" in message.lower() or "graph" in message.lower() or "bar" in message.lower() or "plot" in message.lower())
            if results and visualization_requested:
                yield {"type": "status", "status": "Generating visualization..."}
                # Apply same specialized logic
                if ("swiss" in message.lower() and ("international" in message.lower() or "foreign" in message.lower()) and "tourist" in message.lower() and "month" in message.lower()):
                    visualization = self.visualization_service.create_monthly_tourist_comparison(results, message)
                if not visualization:
                    visualization = self.visualization_service.create_visualization(results, message)

                if visualization:
                    debug_service.add_step_details({"visualization_type": visualization.get("type", "unknown"), "visualization_created": True})
                    yield {"type": "visualization", "visualization": visualization}
                else:
                     debug_service.add_step_details({"visualization_created": False, "reason": "Failed to create"})
            else:
                 debug_service.add_step_details({"visualization_created": False, "reason": "Not requested or no results"})
            debug_service.end_step("visualization")

            # --- Step 5: Generate Final Response ---
            debug_service.start_step("response_generation")
            yield {"type": "status", "status": "Generating final response..."}
            intent = await self._determine_query_intent(message) if is_natural_language else "direct_sql"
            content = await self.response_generation_service.generate_response(
                query=message,
                sql_query=sql_query,
                sql_results=results,
                intent=intent,
                visualization_info=visualization,
                context={"schema_context": schema_context}
            )
            debug_service.end_step("response_generation")

            # --- Final Chunk: Content & Debug Info ---
            yield {
                "type": "final_response",
                "message_id": message_id,
                "content": content,
                "sql_query": sql_query, # Send final (potentially corrected) SQL
                "visualization": visualization, # Send final viz
                "status": "completed",
                "debug_info": debug_service.get_debug_info_for_response()
            }

            debug_service.end_step("process_chat_stream")

        except Exception as e:
            logger.error(f"Error processing chat stream: {str(e)}", exc_info=True)
            yield {"type": "error", "message_id": message_id, "content": f"An error occurred: {str(e)}", "debug_info": debug_service.get_debug_info_for_response() if 'debug_service' in locals() else None}
        finally:
            if 'debug_service' in locals() and debug_service:
                # Determine success based on whether an exception 'e' exists
                flow_success = 'e' not in locals()
                debug_service.end_flow(success=flow_success)

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

    async def close(self):
        """Close the chat service and cleanup resources"""
        try:
            if hasattr(self, 'db_service'):
                self.db_service.close()
        except Exception as e:
            logger.error(f"Error closing chat service: {str(e)}")
            raise

    def _process_sql_results(
            self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process SQL results to ensure they are JSON serializable."""
        processed_results = []

        for row in results:
            processed_row = {}
            for key, value in row.items():
                if isinstance(value, (date, datetime)):
                    processed_row[key] = value.isoformat()
                elif isinstance(value, decimal.Decimal):
                    processed_row[key] = float(value)
                else:
                    processed_row[key] = value
            processed_results.append(processed_row)

        return processed_results

    def _get_visualization(
            self, results: List[Dict[str, Any]], query: str) -> Optional[Dict[str, Any]]:
        """Generate visualization for query results."""
        try:
            if not results or not self.visualization_service:
                return None

            # Check for Swiss and international tourists visualization request
            if ("swiss" in query.lower() and "tourist" in query.lower() and 
                ("international" in query.lower() or "foreign" in query.lower()) and 
                "month" in query.lower() and 
                ("bar" in query.lower() or "chart" in query.lower() or "visual" in query.lower())):
                
                logger.info("Creating Swiss and international tourist monthly comparison")
                return self.visualization_service.create_monthly_tourist_comparison(results, query)

            # Create visualization using the service
            visualization = self.visualization_service.create_visualization(
                results, query)

            if visualization:
                logger.info("Successfully generated visualization")
                return visualization

            # If visualization fails, try to create a simplified version with
            # limited rows
            try:
                df = pd.DataFrame(results)
                # Limit to 10 rows and strip complex nested data
                df = df.head(10)
                # Convert to simple types for JSON serialization
                for col in df.columns:
                    if isinstance(df[col].iloc[0], (dict, list)):
                        df[col] = df[col].astype(str)
                return {
                    "type": "table",
                    "data": json.loads(df.to_json(orient="records"))
                }
            except Exception as e:
                logger.error(
                    f"Error creating fallback visualization: {str(e)}")
                return None

        except Exception as e:
            logger.error(f"Error in visualization generation: {str(e)}")
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
  dw.fact_visitor (
    visitor_id bigint, -- Unique visitor identifier
    date_id bigint, -- Foreign key to dim_date
    region_id bigint, -- Foreign key to dim_region
    segment_id bigint, -- Foreign key to dim_visitor_segment
    visitor_count int, -- Number of visitors
    swiss_tourists int, -- Number of Swiss tourists
    foreign_tourists int, -- Number of foreign tourists
    demographics jsonb -- Demographic information
  )
  dw.fact_spending (
    spending_id bigint, -- Unique spending identifier
    date_id bigint, -- Foreign key to dim_date
    region_id bigint, -- Foreign key to dim_region
    industry_id bigint, -- Foreign key to dim_spending_industry
    category_id bigint, -- Foreign key to dim_spending_category
    total_amount decimal, -- Total spending amount
    transaction_count int, -- Number of transactions
    segment varchar(100) -- Segment information
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
                if isinstance(
                    value, (int, float)) or (
                    isinstance(
                        value, str) and value.replace(
                        '.', '', 1).isdigit()):
                    numeric_cols.append(key)

            # Check if there are date/time columns
            date_cols = []
            for key, value in results[0].items():
                if key.lower() in [
                    'date',
                    'day',
                    'month',
                    'year',
                    'time',
                    'datetime',
                        'period']:
                    date_cols.append(key)

            # If we have both numeric and date columns, try a time series
            if numeric_cols and date_cols:
                logger.info("Recovery: Creating time series visualization")
                visualization = {
                    "type": "time_series",
                    "data": results,
                    "x_field": date_cols[0],
                    "y_field": numeric_cols[0]}
            # If we have multiple numeric columns, try a bar chart
            elif len(numeric_cols) > 1:
                logger.info("Recovery: Creating bar chart visualization")
                visualization = {"type": "bar",
                                 "data": results, "fields": numeric_cols[:2]}
            # If we have just one numeric column, try a pie chart
            elif numeric_cols:
                logger.info("Recovery: Creating pie chart visualization")
                non_numeric_cols = [
                    k for k in results[0].keys() if k not in numeric_cols]
                if non_numeric_cols:
                    visualization = {
                        "type": "pie",
                        "data": results,
                        "label_field": non_numeric_cols[0],
                        "value_field": numeric_cols[0]}
            # Otherwise, just use a table
            else:
                logger.info("Recovery: Creating table visualization")
                visualization = {"type": "table", "data": results}

            # Check if the visualization is a Plotly chart
            if visualization and visualization.get('type') == 'plotly':
                logger.info(
                    "Converting recovery plotly visualization format for frontend")
                return {
                    "type": "plotly_json",
                    "data": visualization.get('data', {})
                }

            return visualization
        except Exception as e:
            logger.error(f"Recovery visualization failed: {str(e)}")
            return None

    def _fix_group_by_error(self, sql_query: str) -> str:
        """Attempt to fix GROUP BY errors in a SQL query for EXTRACT and DATE_TRUNC."""
        fixed_query = sql_query
        change_made = False

        # --- Handle EXTRACT --- 
        extract_pattern = r'EXTRACT\s*\(\s*(\w+)\s+FROM\s+([^\)]+)\)\s+AS\s+(\w+)'
        extracts = re.finditer(extract_pattern, fixed_query, re.IGNORECASE)
        for match in extracts:
            extract_type = match.group(1)
            column = match.group(2).strip()
            alias = match.group(3)
            # Pattern to find the alias within a GROUP BY clause
            group_by_pattern = rf'(GROUP\s+BY\s+[^;]*?)(\b{alias}\b)([^;]*)'
            # Check if the alias exists in GROUP BY
            if re.search(group_by_pattern, fixed_query, re.IGNORECASE | re.MULTILINE):
                 logger.info(f"Fixing GROUP BY alias '{alias}' for EXTRACT expression.")
                 replacement_expression = f'EXTRACT({extract_type} FROM {column})'
                 # Replace alias only within the GROUP BY part using captured groups
                 fixed_query = re.sub(group_by_pattern, rf'\g<1>{replacement_expression}\g<3>', fixed_query, count=1, flags=re.IGNORECASE | re.MULTILINE)
                 change_made = True

        # --- Handle DATE_TRUNC --- 
        date_trunc_pattern = r"DATE_TRUNC\s*\(\s*'([^']+)'\s*,\s*([^\)]+)\)\s+AS\s+(\w+)"
        date_truncs = re.finditer(date_trunc_pattern, fixed_query, re.IGNORECASE)
        for match in date_truncs:
            unit = match.group(1)
            column = match.group(2).strip()
            alias = match.group(3)
            # Pattern to find the alias within a GROUP BY clause
            group_by_pattern = rf'(GROUP\s+BY\s+[^;]*?)(\b{alias}\b)([^;]*)'
            # Check if the alias exists in GROUP BY
            if re.search(group_by_pattern, fixed_query, re.IGNORECASE | re.MULTILINE):
                logger.info(f"Fixing GROUP BY alias '{alias}' for DATE_TRUNC expression.")
                replacement_expression = f"DATE_TRUNC('{unit}', {column})"
                # Replace alias only within the GROUP BY part using captured groups
                fixed_query = re.sub(group_by_pattern, rf'\g<1>{replacement_expression}\g<3>', fixed_query, count=1, flags=re.IGNORECASE | re.MULTILINE)
                change_made = True

        # Log if changes were made
        if change_made:
            logger.info(f"Original query: \n{sql_query}")
            logger.info(f"Fixed query: \n{fixed_query}")
            
        return fixed_query

    # For spending patterns query
    def get_spending_patterns_query(self):
        return """
        SELECT
            d.full_date,
            r.region_name,
            SUM(s.total_amount) as total_spending
        FROM dw.fact_spending s
        JOIN dw.dim_date d ON s.date_id = d.date_id
        JOIN dw.dim_region r ON s.region_id = r.region_id
        WHERE r.country_code = 'CH'
        GROUP BY d.full_date, r.region_name
        ORDER BY d.full_date, total_spending DESC;
        """

    # For visitor density query
    def get_visitor_density_query(self):
        return """
        WITH region_stats AS (
            SELECT
                r.canton_code,
                r.region_name,
                r.population,
                SUM(f.total_visitors) AS total_visitors
            FROM dw.fact_visitor f
            JOIN dw.dim_region r ON f.region_id = r.region_id
            WHERE r.population > 0
            GROUP BY r.canton_code, r.region_name, r.population
        )
        SELECT
            canton_code,
            region_name,
            total_visitors,
            ROUND((total_visitors::float / NULLIF(population, 0))::numeric, 2) AS visitor_density
        FROM region_stats
        ORDER BY visitor_density DESC
        LIMIT 10;
        """
