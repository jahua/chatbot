from typing import Dict, Any, Optional, List
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
        dw_db: Session,
        schema_service: SchemaService,
        dw_context_service: DWContextService,
        llm_adapter: Optional[OpenAIAdapter] = None
    ):
        """Initialize ChatService with required dependencies"""
        self.dw_db = dw_db
        self.schema_service = schema_service
        self.dw_context_service = dw_context_service
        self.llm_adapter = llm_adapter
        
        # Initialize debug service
        self.debug_service = DebugService()
        
        # Initialize context service for RAG
        self.context_service = DWContextService(dw_db)
        self.dw_context_service = DWContextService(dw_db)
        
        # Initialize modular services for LangChain-style flow
        self.sql_generation_service = SQLGenerationService(llm_adapter=self.llm_adapter, debug_service=self.debug_service)
        self.visualization_service = VisualizationService(self.debug_service)
        
        # Initialize other supporting services
        self.tourism_region_service = TourismRegionService()
        self.geo_insights_service = GeoInsightsService(dw_db)
        self.dw_analytics_agent = DWAnalyticsAgent(dw_db)
        
        # Set up cache for query results
        self.query_cache = {}
        self.query_cache_ttl = 3600  # Cache results for 1 hour
        
        logger.info("ChatService initialized successfully")

    async def process_chat_stream(
        self,
        message: str,
        is_direct_query: bool = False,
        session_id: Optional[str] = None,
        message_id: Optional[str] = None
    ):
        """Process chat messages with streaming response using modular LangChain-style pipeline"""
        # Initialize debug service for this flow
        debug_service = self.debug_service
        debug_service.start_flow()
        debug_service.start_step("message_processing")
        debug_service.add_step_details({"message": message})

        try:
            # First yield only the message ID
            yield {"message_id": message_id or str(uuid.uuid4())}
            
            # Process as natural language query
            debug_service.add_step_details({"query_type": "natural_language" if not is_direct_query else "direct_sql"})
            
            # Check if this is a direct SQL query
            if is_direct_query:
                try:
                    debug_service.start_step("direct_sql_execution")
                    # Use the sqlalchemy text function for SQL injection protection
                    query = text(message)
                    
                    # Yield the SQL query
                    yield {"sql_query": str(query)}
                    
                    # Execute the query
                    result = self.dw_db.execute(query).fetchall()
                    
                    # Convert result to JSON-serializable format
                    result_json = []
                    for row in result:
                        row_dict = {}
                        for column, value in row._mapping.items():
                            # Handle non-serializable types
                            if isinstance(value, (datetime.date, datetime.datetime)):
                                row_dict[column] = value.isoformat()
                            elif isinstance(value, decimal.Decimal):
                                row_dict[column] = float(value)
                            else:
                                row_dict[column] = value
                        result_json.append(row_dict)
                    
                    debug_service.add_step_details({
                        "query": str(query),
                        "result_count": len(result_json)
                    })
                    debug_service.end_step()  # End direct_sql_execution
                    
                    # Yield the result summary
                    response = f"Query executed successfully. Found {len(result_json)} results."
                    for chunk in self._split_into_chunks(response):
                        yield {"content_chunk": chunk}
                    
                    # Yield the result data
                    yield {"result": result_json}
                    
                    # Generate visualization from the results
                    visualization_json = self.visualization_service.create_visualization(result_json, message)
                    if visualization_json:
                        yield {"visualization": visualization_json}
                    
                    # Yield debug info
                    debug_info = debug_service.get_debug_info_for_response()
                    if debug_info:
                        yield {"debug_info": debug_info}
                    
                except Exception as e:
                    debug_service.add_step_details({"error": str(e)})
                    debug_service.end_step(error=e)
                    logger.error(f"Error executing direct SQL query: {str(e)}")
                    logger.error(traceback.format_exc())
                    
                    # Roll back the transaction to avoid future errors
                    self.dw_db.rollback()
                    
                    # Yield error message
                    error_response = f"Error executing SQL query: {str(e)}"
                    for chunk in self._split_into_chunks(error_response):
                        yield {"content_chunk": chunk}
                    
                    # Yield debug info
                    debug_info = debug_service.get_debug_info_for_response()
                    if debug_info:
                        yield {"debug_info": debug_info}
                
                # End function here for direct SQL queries
                return
                
            # Check if the message is a simple conversational message
            if self.is_conversational_message(message):
                debug_service.add_step_details({"message_type": "conversational"})
                debug_service.end_step()  # End message_processing

                # For simple greetings, provide a welcome message with available capabilities
                response = (
                    "Hello! I'm your Tourism Analytics Assistant. I can help you with questions about:"
                    "\n- Visitor statistics and trends"
                    "\n- Spending patterns by industry or visitor type"
                    "\n- Seasonal tourism patterns"
                    "\n- Geographic visitor distribution"
                    "\n\nHow can I assist you today?"
                )

                # Stream the response by chunks
                for chunk in self._split_into_chunks(response):
                    yield {"content_chunk": chunk}

                # Yield debug info
                debug_info = debug_service.get_debug_info_for_response()
                if debug_info:
                    yield {"debug_info": debug_info}
                
                # End function here for conversational messages
                return

            # Check if it's a schema inquiry question
            if self.is_schema_inquiry(message):
                debug_service.add_step_details({"message_type": "schema_inquiry"})
                debug_service.end_step()  # End message_processing

                # Return schema information
                schema_summary = self.get_schema_summary()

                # Stream the schema summary by chunks
                for chunk in self._split_into_chunks(schema_summary):
                    yield {"content_chunk": chunk}

                # Yield debug info
                debug_info = debug_service.get_debug_info_for_response()
                if debug_info:
                    yield {"debug_info": debug_info}
                
                # End function here for schema inquiries
                return

            debug_service.end_step()  # End message_processing

            # Indicate analysis is in progress
            yield {"content_chunk": "Analyzing your question..."}

            # LangChain-style RAG flow: 
            # 1. Retrieve relevant context from the database schema and domain knowledge
            debug_service.start_step("context_retrieval")
            schema_context_str = ""
            dw_context = {}
            combined_context = {} # Initialize combined_context here
            try:
                # Get live schema structure
                schema_context_str = await self.schema_service.get_schema_context()
                logger.info(f"Retrieved dynamic schema context of length {len(schema_context_str)}")
                
                # Get rich DW context (examples, curated schema info, etc.)
                dw_context = await self.dw_context_service.get_dw_context(
                    query=message,
                    # Pass other relevant parameters if needed (region_id, dates?)
                )
                logger.info(f"Retrieved DW context: {list(dw_context.keys())}") # Log keys for info

                # Combine contexts for SQL Generation
                combined_context = {
                    "live_schema_string": schema_context_str,
                    "dw_context": dw_context 
                }

                self.debug_service.add_step_details({
                    "schema_context_retrieved": True, 
                    "schema_length": len(schema_context_str),
                    "dw_context_keys": list(dw_context.keys()) 
                })
                self.debug_service.end_step("context_retrieval")

            except Exception as e:
                logger.error(f"Error during context retrieval: {str(e)}", exc_info=True)
                self.debug_service.add_step_details({"error": f"Context retrieval failed: {str(e)}"})
                self.debug_service.end_step(error=e)
                yield {"type": "error", "content": f"Failed to retrieve necessary context: {str(e)}"}
                # Yield debug info before returning on error
                debug_info = self.debug_service.get_debug_info_for_response()
                if debug_info: yield {"debug_info": debug_info}
                return 
            
            self.debug_service.start_step("sql_generation")
            sql_query = None
            try:
                # Ensure combined_context is available here before passing
                if not combined_context:
                     raise ValueError("Combined context is empty, cannot generate SQL.")

                sql_query = await self.sql_generation_service.generate_query(
                    query_text=message,
                    schema_context=combined_context # Pass the combined context
                )
                self.debug_service.add_step_details({
                     "sql_generated": True,
                     "sql_query": sql_query
                 })
                # Yield the generated SQL query
                yield {"sql_query": sql_query}
            except Exception as e:
                logger.error(f"Error generating SQL query: {str(e)}", exc_info=True)
                self.debug_service.add_step_details({"error": str(e)})
                self.debug_service.end_step(error=e)
                yield {"type": "error", "content": f"Failed to generate SQL query: {str(e)}"}
                # Yield debug info before returning on error
                debug_info = self.debug_service.get_debug_info_for_response()
                if debug_info: yield {"debug_info": debug_info}
                return
            self.debug_service.end_step("sql_generation")

            # Execute SQL Query
            self.debug_service.start_step("sql_execution")
            try:
                if not sql_query:
                    raise ValueError("SQL query is empty, cannot execute.")
                    
                logger.info(f"Executing SQL query: {format_sql(sql_query)}")
                result = self.dw_db.execute(text(sql_query))
                self.debug_service.add_step_details({"executed_successfully": True})
                
                processed_results = self._process_sql_results(result)
                self.debug_service.add_step_details({"results_processed": True, "row_count": len(processed_results)})
                
                # Yield results
                if processed_results:
                    yield {"result": processed_results} # Yield processed results 
                
                # Generate and yield visualization if results exist
                if processed_results:
                    visualization_data = self._get_visualization(processed_results, sql_query)
                    if visualization_data:
                        yield {"visualization": visualization_data}
                        self.debug_service.add_step_details({"visualization_generated": True})
                    else:
                         self.debug_service.add_step_details({"visualization_skipped": True})
                else:
                     self.debug_service.add_step_details({"visualization_skipped_no_results": True})

            except Exception as e:
                logger.error(f"Error executing SQL query: {str(e)}", exc_info=True)
                logger.error(f"Failed SQL Query:\n{format_sql(sql_query)}")
                self.debug_service.add_step_details({"error": str(e), "failed_sql": format_sql(sql_query)})
                self.debug_service.end_step(error=e)
                # --- Start Added Error Yielding --- 
                error_message = f"Error executing SQL query: {str(e)}"
                yield {"type": "error", "content": error_message}
                # --- End Added Error Yielding --- 
                # Yield debug info before returning on error
                debug_info = self.debug_service.get_debug_info_for_response()
                if debug_info: yield {"debug_info": debug_info}
                return
            self.debug_service.end_step("sql_execution")

            # 4. Generate natural language response from the query results
            debug_service.start_step("response_generation")
            try:
                # Generate appropriate response based on query and results
                if len(processed_results) > 0:
                    # Check query type to format appropriate response
                    query_lower = message.lower()
                    
                    if "busiest" in query_lower and ("week" in query_lower or "day" in query_lower):
                        if "period_start" in processed_results[0] and "period_end" in processed_results[0] and "total_visitors" in processed_results[0]:
                            # Format the date range
                            start_date = datetime.datetime.fromisoformat(processed_results[0]["period_start"]) if isinstance(processed_results[0]["period_start"], str) else processed_results[0]["period_start"]
                            end_date = datetime.datetime.fromisoformat(processed_results[0]["period_end"]) if isinstance(processed_results[0]["period_end"], str) else processed_results[0]["period_end"]
                            
                            start_str = start_date.strftime('%B %d, %Y')
                            end_str = end_date.strftime('%B %d, %Y')
                            visitors = processed_results[0]["total_visitors"]
                            
                            response = f"The busiest period was from {start_str} to {end_str} with {int(visitors):,} visitors."
                        else:
                            # Generic response if expected fields not found
                            response = f"Found {len(processed_results)} results for your query about busy periods."
                    
                    elif "spending" in query_lower or "industry" in query_lower:
                        # For industry spending queries
                        if len(processed_results) == 1:
                            # Single result - likely "which industry has highest spending"
                            industry = processed_results[0].get(next(iter([k for k in processed_results[0].keys() if "industry" in k.lower() or "name" in k.lower()])), "")
                            spending = processed_results[0].get(next(iter([k for k in processed_results[0].keys() if "spending" in k.lower() or "amount" in k.lower()])), 0)
                            
                            response = f"The industry with the highest spending was {industry} with ${float(spending):,.2f} in total spending."
                        else:
                            # Multiple industries
                            response = f"Found spending data for {len(processed_results)} industries. The top industry is {processed_results[0].get(next(iter([k for k in processed_results[0].keys() if 'industry' in k.lower() or 'name' in k.lower()])), '')}."
                    
                    elif "visitor" in query_lower or "tourist" in query_lower:
                        # For visitor queries
                        if "visitor_count" in processed_results[0] or "total_visitors" in processed_results[0]:
                            visitor_key = "visitor_count" if "visitor_count" in processed_results[0] else "total_visitors"
                            total = sum(float(r[visitor_key]) for r in processed_results)
                            response = f"Found a total of {int(total):,} visitors. Details are shown in the visualization."
                        else:
                            response = f"Found visitor data with {len(processed_results)} records. See the visualization for details."
                    
                    else:
                        # Default response
                        response = f"I found {len(processed_results)} results for your query. The data is visualized below."
                else:
                    response = "I didn't find any data matching your query. Please try a different question."
                
                debug_service.add_step_details({
                    "response_generated": True,
                    "response_length": len(response)
                })
                debug_service.end_step()
                
                # Stream the response by chunks
                for chunk in self._split_into_chunks(response):
                    yield {"content_chunk": chunk}
                
            except Exception as e:
                debug_service.add_step_details({"error": str(e)})
                debug_service.end_step(error=e)
                logger.error(f"Error generating response: {str(e)}")
                
                # Roll back the transaction to avoid future errors
                self.dw_db.rollback()
                
                # Yield error message
                error_response = f"Error generating response: {str(e)}"
                for chunk in self._split_into_chunks(error_response):
                    yield {"content_chunk": chunk}
            
            # 5. Generate visualization if data is available
            if processed_results and len(processed_results) > 0:
                debug_service.start_step("visualization_generation")
                try:
                    # Create visualization with automatic chart selection
                    visualization_json = self.visualization_service.create_visualization(processed_results, message)
                    
                    if visualization_json:
                        debug_service.add_step_details({
                            "visualization_created": True,
                            "visualization_size": len(visualization_json)
                        })
                        
                        # Yield the visualization
                        yield {"visualization": visualization_json}
                    else:
                        debug_service.add_step_details({
                            "visualization_created": False,
                            "reason": "Visualization service returned None"
                        })
                    
                    debug_service.end_step()
                    
                except Exception as e:
                    debug_service.add_step_details({"error": str(e)})
                    debug_service.end_step(error=e)
                    logger.error(f"Error generating visualization: {str(e)}")
                    
                    # Roll back the transaction to avoid future errors
                    self.dw_db.rollback()
            
            # Yield debug info
            debug_info = self.debug_service.get_debug_info_for_response()
            if debug_info:
                yield {"debug_info": debug_info}
                
        except Exception as e:
            # Global error handling
            logger.error(f"Error in chat stream: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Roll back the transaction to avoid future errors
            try:
                self.dw_db.rollback()
            except Exception as rollback_error:
                logger.error(f"Error rolling back transaction: {str(rollback_error)}")
            
            # Yield error message
            yield {"content_chunk": f"Error: {str(e)}"}
            
            # Yield debug info if available
            if debug_service:
                debug_info = self.debug_service.get_debug_info_for_response()
                if debug_info:
                    yield {"debug_info": debug_info}

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
    
    def _split_into_chunks(self, text, chunk_size=10):
        """Split text into small chunks for streaming"""
        words = text.split()
        for i in range(0, len(words), chunk_size):
            yield " ".join(words[i:i+chunk_size])
            
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
