from typing import Dict, Any, Optional, List, AsyncGenerator
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
# Import VisitorAnalysisService if it exists
# from .visitor_analysis_service import VisitorAnalysisService

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ChatService:
    def __init__(
        self,
        schema_service: SchemaService,
        dw_context_service: DWContextService,
        llm_adapter: Optional[OpenAIAdapter] = None
    ):
        """Initialize ChatService with required dependencies"""
        self.schema_service = schema_service
        self.dw_context_service = dw_context_service
        self.llm_adapter = llm_adapter
        
        # Initialize debug service
        self.debug_service = DebugService()
        
        # Initialize modular services for LangChain-style flow
        self.sql_generation_service = SQLGenerationService(llm_adapter=self.llm_adapter, debug_service=self.debug_service)
        self.visualization_service = VisualizationService(self.debug_service)
        
        # Initialize other supporting services
        self.tourism_region_service = TourismRegionService()
        
        # Create DatabaseService for GeoInsightsService
        self.db_service = DatabaseService()
        self.geo_insights_service = GeoInsightsService(db_service=self.db_service)
        self.dw_analytics_agent = DWAnalyticsAgent()
        
        # Set up cache for query results
        self.query_cache = {}
        self.query_cache_ttl = 3600  # Cache results for 1 hour
        
        logger.info("ChatService initialized successfully")

    async def process_chat_stream(
        self,
        message: str,
        *,
        session_id: str,
        is_direct_query: bool,
        message_id: Optional[str] = None,
        dw_db: Session
    ) -> AsyncGenerator[Dict[str, Any], None]:
        flow_succeeded = True # Overall success flag
        current_step_name = None
        current_message_id = message_id or str(uuid.uuid4())
        self.debug_service.start_flow(session_id, current_message_id)
        yield {"type": "start"}
        yield {"type": "message_id", "message_id": current_message_id}
        yield {"type": "content_start", "message_id": current_message_id}
        yield {"type": "content", "content": "Analyzing your question..."}

        processed_results = [] # Initialize to ensure it exists
        sql_query = "" # Initialize

        try:
            # --- Step: Message Processing ---
            current_step_name = "message_processing"
            self.debug_service.start_step(current_step_name)
            query_type = None # Initialize
            try:
                query_type = self._determine_query_type(message, is_direct_query)
                logger.info(f"Determined query type: {query_type}") # ADDED LOG
                self.debug_service.add_step_details({
                    "message": message,
                    "is_direct_query_flag": is_direct_query,
                    "query_type": query_type
                })
                self.debug_service.end_step()
            except Exception as step_e:
                logger.error("Error in _determine_query_type", exc_info=True) # ADDED LOG
                self.debug_service.end_step(error=str(step_e))
                raise # Re-raise to be caught by outer try/except

            # --- Step: Context Retrieval ---
            current_step_name = "context_retrieval"
            self.debug_service.start_step(current_step_name)
            schema_context, dw_context = None, None # Initialize
            try:
                schema_context, dw_context = await self._get_context(
                    message, 
                    query_type == "natural_language", 
                    dw_db=dw_db
                )
                logger.info(f"Context retrieval complete. Schema context length: {len(schema_context) if schema_context else 0}") # ADDED LOG
                self.debug_service.add_step_details({
                    "schema_context_retrieved": schema_context is not None,
                    "schema_length": len(schema_context) if schema_context else 0,
                    "dw_context_keys": list(dw_context.keys()) if dw_context else []
                })
                self.debug_service.end_step()
            except Exception as step_e:
                logger.error("Error in _get_context", exc_info=True) # ADDED LOG
                self.debug_service.end_step(error=str(step_e))
                raise

            logger.info("Proceeding to main logic branching...") # ADDED LOG
            # --- Main Logic Branching ---
            if query_type == "sql_direct":
                # --- Step: SQL Direct Execution ---
                current_step_name = "sql_execution"
                self.debug_service.start_step(current_step_name)
                try:
                    sql_query = message # Direct query is the message itself
                    yield {"type": "sql_query", "sql_query": sql_query}
                    result = dw_db.execute(text(sql_query))
                    processed_results = self._process_sql_results(result)
                    self.debug_service.add_step_details({"executed_successfully": True, "results_processed": True, "row_count": len(processed_results)})
                    self.debug_service.end_step()
                except Exception as step_e:
                    self.debug_service.end_step(error=str(step_e))
                    raise

                # --- Step: Response Generation (Direct) ---
                # No LLM response needed, just yield confirmation
                yield {"type": "content", "content": f"Executed direct query. Found {len(processed_results)} results."}

                # --- Step: Visualization (Direct) ---
                current_step_name = "visualization_creation" # Use distinct name
                self.debug_service.start_step(current_step_name)
                try:
                    visualization = self._get_visualization(processed_results, sql_query)
                    if visualization:
                        yield {"type": "visualization", "visualization": visualization}
                        self.debug_service.add_step_details({"visualization_generated": True})
                    else:
                         self.debug_service.add_step_details({"visualization_skipped": True})
                    self.debug_service.end_step()
                except Exception as step_e:
                    # Log visualization error but don't fail the flow for direct SQL
                    logger.warning(f"Visualization failed for direct query, but continuing: {str(step_e)}")
                    self.debug_service.end_step(error=f"Handled viz error: {str(step_e)}")


            elif query_type == "natural_language":
                # --- Step: SQL Generation LLM ---
                current_step_name = "sql_generation_llm"
                self.debug_service.start_step(current_step_name, details={
                    "query_text": message,
                    "schema_context_keys": ["live_schema_string", "dw_context"] if schema_context and dw_context else []
                })
                try:
                    sql_query = await self.sql_generation_service.generate_query(
                        query_text=message,
                        schema_context={
                            "live_schema_string": schema_context,
                            "dw_context": dw_context
                        }
                    )
                    if not sql_query: raise ValueError("SQL query generation by LLM returned empty.")
                    yield {"type": "sql_query", "sql_query": sql_query}
                    self.debug_service.add_step_details({"sql_generated": True, "sql_query": sql_query})
                    self.debug_service.end_step()
                except Exception as step_e:
                    self.debug_service.end_step(error=str(step_e))
                    raise

                # --- Step: SQL Execution ---
                current_step_name = "sql_execution"
                self.debug_service.start_step(current_step_name)
                try:
                    logger.info(f"Executing SQL query: {sql_query}")
                    formatted_sql = format_sql(sql_query)
                    result = dw_db.execute(text(formatted_sql))
                    processed_results = self._process_sql_results(result)
                    logger.info(f"SQL query executed. Row count: {len(processed_results)}")
                    self.debug_service.add_step_details({"executed_successfully": True, "results_processed": True, "row_count": len(processed_results)})
                    self.debug_service.end_step()
                except Exception as step_e:
                    self.debug_service.end_step(error=str(step_e))
                    raise

                # --- Step: Response Generation ---
                current_step_name = "response_generation"
                self.debug_service.start_step(current_step_name)
                response = ""
                try:
                    # Simplified response generation
                    response = f"Found {len(processed_results)} results for your query about busy periods." # Simplified
                    # TODO: Integrate real ResponseGenerationService call
                    # response = await self.response_generation_service.generate_response(...)
                    self.debug_service.add_step_details({"response_generated": True, "response_length": len(response)})
                    self.debug_service.end_step()
                except Exception as step_e:
                     self.debug_service.end_step(error=str(step_e))
                     raise # Fail flow if response generation fails

                # --- Yield Final Content ---
                # This block now has its own error handling and logging
                yield_step_name = "final_content_yield"
                self.debug_service.start_step(yield_step_name)
                try:
                    logger.info("Attempting to yield final content chunks...") # ADDED LOG
                    if not response:
                         logger.warning("Response string is empty, skipping final content yield.")
                    else:
                        for chunk in self._split_into_chunks(response):
                            yield {"type": "content", "content": chunk}
                            # logger.debug(f"Yielded content chunk: {chunk[:50]}...") # Optional: very verbose
                    logger.info("Finished yielding final content chunks.") # ADDED LOG
                    self.debug_service.end_step()
                except Exception as yield_e:
                    logger.error(f"Error yielding final content: {str(yield_e)}", exc_info=True)
                    self.debug_service.end_step(error=str(yield_e))
                    # Continue the flow, but log the yield error. Don't set flow_succeeded = False here.


                # --- Step: Visualization (NLQ) ---
                current_step_name = "visualization_creation" # Using distinct name
                self.debug_service.start_step(current_step_name, details={
                    "data_rows": len(processed_results),
                    "query_text": message
                })
                try:
                    visualization = self._get_visualization(processed_results, sql_query)
                    if visualization:
                        yield {"type": "visualization", "visualization": visualization}
                        self.debug_service.add_step_details({"visualization_generated": True})
                    else:
                         self.debug_service.add_step_details({"visualization_skipped": True})
                    self.debug_service.end_step()
                except Exception as step_e:
                     logger.warning(f"Visualization creation/yielding failed, but continuing: {str(step_e)}")
                     self.debug_service.end_step(error=f"Handled viz error: {str(step_e)}")
                     # Do not raise, allow flow to finish

            else: # Fallback for other query types
                 yield {"type": "content", "content": "Sorry, I can only process natural language queries or direct SQL for now."}


        except Exception as e:
            flow_succeeded = False # Mark overall flow as failed
            logger.error(f"Chat stream processing failed in step '{current_step_name}': {str(e)}", exc_info=True)
            # Ensure the step that raised the exception is marked as failed if not already
            # The specific step's except block should have already called end_step
            # We just yield the generic error message here
            yield {"type": "error", "error": f"Sorry, I encountered an error: {str(e)}"}

        finally:
            # Final debug info yield
            try:
                debug_info_data = self.debug_service.get_debug_info_for_response()
            except Exception as debug_err:
                logger.error(f"Error generating final debug info: {debug_err}", exc_info=True)
                debug_info_data = {"status": "error", "error": "Failed to generate debug info"}
            
            yield {"type": "debug_info", "debug_info": debug_info_data}
            
            # Final end marker
            # Ensure message ID is available
            final_message_id = self.debug_service.get_message_id() or "unknown"
            yield {"type": "end", "message_id": final_message_id}
            
            logger.info(f"Finished processing chat stream for session {session_id}. Overall Success: {flow_succeeded}")

    def is_conversational_message(self, message: str) -> bool:
        """Detect if a message is conversational rather than a data query"""
        # Clean and normalize the message
        message = message.strip().lower()
        
        # Define greetings that should trigger conversational response
        pure_greetings = ["hi", "hello", "hey", "greetings", "hi there", "hello there", 
                          "thanks", "thank you", "goodbye", "bye", "good morning", 
                          "good afternoon", "good evening"]
        
        # Check if the message is EXACTLY a greeting
        if message in pure_greetings:
            return True
            
        # Check if it looks like a question about data (not just a greeting)
        question_words = ["what", "which", "where", "how", "when", "who", "show", 
                          "list", "find", "tell", "give", "display", "query", 
                          "analyze", "get", "calculate"]
                          
        data_related_terms = ["industry", "visitor", "spending", "tourism", "busiest", 
                              "week", "day", "month", "year", "quarter", "pattern", 
                              "trend", "statistics", "data", "amount", "total", 
                              "average", "count", "transaction", "swiss", "foreign", 
                              "domestic", "international", "spring", "summer", 
                              "winter", "fall", "autumn", "region", "location", 
                              "period", "season", "txn", "amt"]
        
        # If it contains question words AND data terms, it's a data query, not just conversation
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
            "schema", "columns", "tables", "fields", "structure", "data model", 
            "what data", "available data", "what information", "what tables", 
            "database schema", "field names", "column names", "available tables",
            "what columns", "show me the data", "data structure", "metadata",
            "what can i ask", "what can you tell me about", "show me what data"
        ]
        
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
                "- How do visitor patterns differ between Swiss and foreign tourists?"
            )
            
            return schema_summary
        except Exception as e:
            logger.error(f"Error generating schema summary: {str(e)}")
            logger.error(traceback.format_exc())
            return "I can help you analyze tourism data including visitor statistics and transaction data. Please ask a specific question about tourism patterns."
    
    def _split_into_chunks(self, text: str, chunk_size: int = 50):
        """Yield successive chunk_size chunks from text."""
        if not text:
            return
        for i in range(0, len(text), chunk_size):
            yield text[i:i + chunk_size]
            
    def close(self):
        """Clean up resources when service is shutting down"""
        logger.info("Closing ChatService resources")
        # Close any resources that need to be cleaned up
        try:
            # Clean up any resources that need to be released
            # For example, if there are any background tasks or connections to close
            pass
        except Exception as e:
            logger.error(f"Error closing ChatService: {str(e)}")
            logger.error(traceback.format_exc())

    def _process_sql_results(self, result: Result) -> List[Dict[str, Any]]:
        """Convert SQLAlchemy Result to a list of JSON-serializable dictionaries."""
        processed_results = []
        if not result:
            return processed_results
            
        try:
            keys = result.keys()
            for row in result.mappings(): # Iterate through rows as dictionaries
                row_dict = {}
                for key in keys:
                    value = row[key]
                    if isinstance(value, (datetime.date, datetime.datetime)):
                        row_dict[key] = value.isoformat()
                    elif isinstance(value, decimal.Decimal):
                        row_dict[key] = float(value)
                    # Add other type conversions if needed (e.g., timedelta)
                    else:
                        row_dict[key] = value
                processed_results.append(row_dict)
        except Exception as e:
             logger.error(f"Error processing SQL results: {str(e)}", exc_info=True)
             # Decide how to handle partial processing or return empty
             return [] # Return empty list on processing error
             
        return processed_results

    def _get_visualization(self, results: List[Dict[str, Any]], query: str) -> Optional[str]:
        """Generate visualization based on query results, handling potential errors."""
        visualization_data = None # Initialize
        try:
            if results and self.visualization_service:
                # Example: Generate a bar chart for visitor data
                if "visitor" in query.lower() or "tourist" in query.lower():
                    try:
                        visualization_data = self.visualization_service.create_bar_chart(results)
                    except AttributeError:
                        logger.warning("VisualizationService has no 'create_bar_chart' method.")
                    except Exception as viz_err:
                         logger.error(f"Error in create_bar_chart: {str(viz_err)}")
                # Add similar try/except blocks for other chart types if needed
                elif "spending" in query.lower() or "industry" in query.lower():
                     try:
                         visualization_data = self.visualization_service.create_pie_chart(results)
                     except AttributeError:
                         logger.warning("VisualizationService has no 'create_pie_chart' method.")
                     except Exception as viz_err:
                         logger.error(f"Error in create_pie_chart: {str(viz_err)}")
                # ... add more specific chart types with try/except ...
                else:
                    # Fallback to a default or attempt a generic visualization
                    try:
                        # Example: Try a generic table or default plot if available
                        if hasattr(self.visualization_service, 'create_default_visualization'):
                            visualization_data = self.visualization_service.create_default_visualization(results)
                        else:
                            logger.warning("No default visualization method found.")
                    except Exception as viz_err:
                        logger.error(f"Error in default visualization: {str(viz_err)}")
            else:
                logger.info("No results or visualization service available to generate visualization.")
            
        except Exception as e:
            logger.error(f"Unexpected error during visualization generation: {str(e)}")
            logger.error(traceback.format_exc()) # Log the full traceback

        return visualization_data # Return None if any error occurred or no data

    async def _get_context(
        self, 
        query: str, 
        is_nlq: bool, 
        dw_db: Session
    ) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
        if not is_nlq:
            return None, None 

        # SchemaService doesn't expect a db_session parameter
        schema_context_str = await self.schema_service.get_schema_context() 

        # Assume DWContextService needs the session
        # Re-initialize or pass db session if needed
        # Option 1: Pass session (if service accepts it)
        # dw_context = await self.dw_context_service.get_dw_context(query=query, db_session=dw_db)
        # Option 2: Re-initialize with new session (if service takes db in init)
        dw_context_service = DWContextService(dw_db=dw_db)
        dw_context = await dw_context_service.get_dw_context(query=query)

        logger.info(f"Retrieved DW context: {list(dw_context.keys())}")
        return schema_context_str, dw_context

    def _determine_query_type(self, message: str, is_direct_query: bool) -> str:
        """Determine the type of query based on the message and the direct_query flag."""
        if is_direct_query:
            return "sql_direct"
        
        # Check if the message looks like a SQL query
        sql_keywords = ["select", "from", "where", "join", "group by", "order by", "having", "limit"]
        message_lower = message.lower().strip()
        
        # If message starts with SELECT and contains other SQL keywords, assume it's a direct SQL query
        if message_lower.startswith("select") and any(keyword in message_lower for keyword in sql_keywords[1:]):
            return "sql_direct"
        
        # Otherwise, treat it as a natural language query
        return "natural_language"
