from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from ..db.database import SessionLocal, DatabaseService, get_db
from ..llm.openai_adapter import OpenAIAdapter
from ..schemas.chat import ChatMessage, ChatResponse
from ..db.schema_manager import SchemaManager
from sqlalchemy import text
import pandas as pd
import logging
import traceback
import asyncio
from .conversation_service import ConversationService
import json
from datetime import datetime
import time
from fastapi import HTTPException
from ..core.config import settings
from decimal import Decimal
import uuid
from ..utils.sql_utils import extract_sql_query, clean_sql_query
import re
import psycopg2
import plotly.graph_objects as go
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

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self, db_service=None, llm_adapter=None, schema_manager=None):
        """Initialize the chat service with OpenAI adapter and SQL generator"""
        self.llm = llm_adapter or OpenAIAdapter()
        self.sql_generator = SQLGenerator()
        self.db_service = db_service or DatabaseService()
        self.schema_manager = schema_manager
        self._query_cache = {}  # Simple query cache
        self._cache_ttl = 300  # Cache TTL in seconds (5 minutes)
        self.intent_parser = HybridIntentParser(llm_adapter=self.llm)
        self.geo_insights_service = GeoInsightsService(db_service=self.db_service)
        self.geo_visualization_service = GeoVisualizationService()
        logger.info("ChatService initialized successfully")

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
        bot_references = ["you", "your", "yourself", "chatbot", "bot", "assistant", "ai"]
        bot_reference_count = sum(1 for ref in bot_references if ref in message.split())
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
            schema_data = self.get_schema_info()
            
            # Organize schema by table
            tables = {}
            for item in schema_data:
                table_name = item["table_name"]
                if table_name not in tables:
                    tables[table_name] = []
                tables[table_name].append(f"{item['column_name']} ({item['data_type']})")
            
            # Build a user-friendly schema description
            summary = "Here's the data I have available:\n\n"
            
            if "aoi_days_raw" in tables:
                summary += "**Visitor Data (aoi_days_raw)**\n"
                summary += "This table contains daily visitor information with columns:\n"
                summary += "- aoi_date: The date of the visitor data\n"
                summary += "- visitors: JSON data containing 'swissTourist' and 'foreignTourist' counts\n\n"
                summary += "You can ask questions like:\n"
                summary += "- What were the peak tourism periods in 2023?\n"
                summary += "- What are the weekly visitor patterns in spring 2023?\n"
                summary += "- How do visitor patterns differ between domestic and international tourists?\n\n"
            
            if "master_card" in tables:
                summary += "**Transaction Data (master_card)**\n"
                summary += "This table contains transaction information with columns:\n"
                summary += "- txn_date: The date of the transaction\n"
                summary += "- industry: The industry sector of the transaction\n"
                summary += "- txn_amt: The transaction amount\n"
                summary += "- txn_cnt: The count of transactions\n"
                summary += "- segment: Market segment information\n"
                summary += "- Geographic information: geo_type, geo_name, central_latitude, central_longitude\n\n"
                summary += "You can ask questions like:\n"
                summary += "- What are the top spending categories in 2023?\n"
                summary += "- How does tourism spending vary by industry throughout the year?\n"
                summary += "- Which geographic regions saw the highest transaction volumes?\n"
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating schema summary: {str(e)}")
            logger.error(traceback.format_exc())
            return "I can help you analyze tourism data including visitor statistics and transaction data. Please ask a specific question about tourism patterns."
    
    async def process_chat(self, message: str, session_id: str = None, is_direct_query: bool = False) -> Dict[str, Any]:
        """Process a chat message and return a response with visualization"""
        try:
            # Check if it's a conversational message
            if self.is_conversational_message(message):
                return {
                    'message_id': str(uuid.uuid4()),
                    'content': "I'm here to help you analyze tourism data. Please ask me about visitor statistics, spending patterns, or tourism trends!",
                    'response': "I'm here to help you analyze tourism data. Please ask me about visitor statistics, spending patterns, or tourism trends!",
                    'visualization': None,
                    'sql_query': None,
                    'status': 'success'
                }

            # Check if it's a schema inquiry
            if self.is_schema_inquiry(message):
                schema_summary = self.get_schema_summary()
                return {
                    'message_id': str(uuid.uuid4()),
                    'content': schema_summary,
                    'response': schema_summary,
                    'visualization': None,
                    'sql_query': None,
                    'status': 'success'
                }

            # Parse the intent using the hybrid parser
            parsed_data = await self.intent_parser.parse_intent(message)
            intent = parsed_data.get('intent')
            time_range = parsed_data.get('time_range')
            granularity = parsed_data.get('granularity')
            region_info = parsed_data.get('region_info', {})
            comparison_type = parsed_data.get('comparison_type')
            
            logger.info(f"Parsed intent: {intent}, Time range: {time_range}, Granularity: {granularity}")
            
            # Enhanced geospatial detection
            is_map_request = parsed_data.get('is_map_request', False)
            contains_geo_terms = any(term in message.lower() for term in ['map', 'where', 'location', 'region', 'area', 'city', 'canton', 'country', 'geographic', 'spatial'])
            contains_geo_viz_terms = any(term in message.lower() for term in ['show', 'display', 'map', 'plot', 'visualize', 'visualization'])
            
            logger.info(f"Geospatial detection - is_map_request: {is_map_request}, contains_geo_terms: {contains_geo_terms}, contains_geo_viz_terms: {contains_geo_viz_terms}")
            
            # Handle geospatial queries separately
            if intent in [QueryIntent.GEO_SPATIAL, QueryIntent.REGION_ANALYSIS, 
                          QueryIntent.HOTSPOT_DETECTION, QueryIntent.SPATIAL_PATTERN]:
                logger.info("Routing to geospatial query handler")
                return await self._handle_geospatial_query(message, intent, region_info)
            # Add a more flexible router that can detect map requests even if the intent parser missed it
            elif contains_geo_terms and contains_geo_viz_terms:
                logger.info("Detected map request from keywords, routing to geospatial query handler")
                
                # Create basic region info from message if not available
                if not region_info:
                    # Extract region name - simple approach
                    region_match = re.search(r"(?:in|of|for|at)\s+(?:the\s+)?(\w+(?:\s+\w+){0,3})", message.lower())
                    region_name = region_match.group(1) if region_match else "not specified in the query"
                    
                    region_info = {
                        'region_name': region_name,
                        'region_type': 'unknown',
                        'is_map_request': True,
                        'fallback_intent': QueryIntent.TREND_ANALYSIS
                    }
                
                return await self._handle_geospatial_query(message, QueryIntent.GEO_SPATIAL, region_info)

            # Generate SQL query based on user message
            try:
                # Add original message to help with specific query optimizations
                query_result = self.sql_generator.generate_sql_query(message)
                
                # If this is a parsing object, add the original message for context
                if isinstance(query_result, dict) and not 'error' in query_result:
                    query_result['original_message'] = message
                    
                logger.debug(f"Generated SQL query: {query_result}")
                
                if not query_result or 'error' in query_result:
                    error_msg = query_result.get('error', 'Unknown error in query generation') if query_result else 'Failed to generate query'
                    logger.error(f"SQL generation failed for message: {message}, error: {error_msg}")
                    return {
                        'message_id': str(uuid.uuid4()),
                        'content': "I couldn't understand your query. Could you please rephrase it?",
                        'response': "I couldn't understand your query. Could you please rephrase it?",
                        'visualization': None,
                        'sql_query': None,
                        'status': 'error'
                    }
                
                sql_query = query_result.get('query', '')
                intent_str = query_result.get('intent', 'visitor_count')
                
                # Convert string intent to enum
                intent = None
                for intent_enum in QueryIntent:
                    if intent_enum.value == intent_str:
                        intent = intent_enum
                        break
                
                metadata = query_result.get('metadata', {})
                
                # Validate query - skip validation for direct API calls 
                # to prevent the double-query issue
                if not is_direct_query:
                    try:
                        # Check if validate_query is available
                        # Skip validation for simple queries to reduce database load
                        is_simple_query = len(sql_query.strip()) < 500 and "UNION" not in sql_query.upper() and "WITH" not in sql_query.upper()
                        
                        if hasattr(self.db_service, 'validate_query') and not is_simple_query:
                            is_valid = self.db_service.validate_query(sql_query)
                            if not is_valid:
                                logger.error(f"Invalid SQL query: {sql_query}")
                                return {
                                    'message_id': str(uuid.uuid4()),
                                    'content': "I couldn't generate a valid query for your question. Could you please rephrase it?",
                                    'response': "I couldn't generate a valid query for your question. Could you please rephrase it?",
                                    'visualization': None,
                                    'sql_query': sql_query,
                                    'status': 'error'
                                }
                        else:
                            # Skip validation if method not available or query is simple
                            logger.info(f"Skipping validation for query: {'simple query' if is_simple_query else 'validation not available'}")
                    except Exception as e:
                        logger.error(f"Error during query validation: {str(e)}")
                        logger.error(traceback.format_exc())
                else:
                    logger.info(f"Skipping validation for direct API call")
                
                # Execute query and get data
                try:
                    # Check cache first
                    cached_data = self._get_cached_query_result(sql_query)
                    if cached_data is not None:
                        data = cached_data
                    else:
                        data = self.db_service.execute_query(sql_query)
                        # Cache the result for future use
                        self._cache_query_result(sql_query, data)
                except TimeoutError as e:
                    return {
                        'message_id': str(uuid.uuid4()),
                        'content': str(e),
                        'response': str(e),
                        'visualization': None,
                        'sql_query': sql_query,
                        'status': 'error'
                    }
                
                if not data:
                    # Check if this is a future date query
                    time_range = metadata.get('time_range', {})
                    start_date = time_range.get('start_date', '')
                    
                    if start_date and datetime.strptime(start_date, '%Y-%m-%d') > datetime.now():
                        return {
                            'message_id': str(uuid.uuid4()),
                            'content': f"I don't have data for future dates like {start_date}. Would you like to see data from a past period instead?",
                            'response': f"I don't have data for future dates like {start_date}. Would you like to see data from a past period instead?",
                            'visualization': None,
                            'sql_query': sql_query,
                            'status': 'error'
                        }
                    
                    return {
                        'message_id': str(uuid.uuid4()),
                        'content': "I couldn't find any data matching your query.",
                        'response': "I couldn't find any data matching your query.",
                        'visualization': None,
                        'sql_query': sql_query,
                        'status': 'error'
                    }
                
                # Generate visualization based on intent and data
                try:
                    visualization_data = generate_visualization(data, intent)
                    visualization_json = json.dumps(visualization_data, cls=DateTimeEncoder) if visualization_data else None
                except Exception as viz_error:
                    logger.error(f"Error generating visualization: {str(viz_error)}")
                    logger.error(traceback.format_exc())
                    visualization_json = None
                
                # Generate analysis summary
                analysis = self._generate_analysis(data, intent, metadata)
                
                return {
                    'message_id': str(uuid.uuid4()),
                    'content': analysis,
                    'response': analysis,
                    'visualization': visualization_json,
                    'sql_query': sql_query,
                    'status': 'success'
                }
                
            except TimeoutError as e:
                logger.error(f"Query execution timed out: {str(e)}")
                return {
                    'message_id': str(uuid.uuid4()),
                    'content': str(e),
                    'response': str(e),
                    'visualization': None,
                    'sql_query': None,
                    'status': 'error'
                }
            except Exception as e:
                logger.error(f"Error processing SQL generation: {str(e)}")
                logger.error(traceback.format_exc())
            return {
                    'message_id': str(uuid.uuid4()),
                    'content': "Sorry, I encountered an error while processing your request.",
                    'response': "Sorry, I encountered an error while processing your request.",
                    'visualization': None,
                    'sql_query': None,
                    'status': 'error'
            }
            
        except Exception as e:
            logger.error(f"Error processing chat message: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'message_id': str(uuid.uuid4()),
                'content': "Sorry, I encountered an error while processing your request.",
                'response': "Sorry, I encountered an error while processing your request.",
                'visualization': None,
                'sql_query': None,
                'status': 'error'
            }
    
    def get_schema_info(self) -> List[Dict[str, Any]]:
        """
        Get database schema information
        """
        try:
            return self.db_service.get_schema_info()
        except Exception as e:
            logger.error(f"Error getting schema info: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
    
    def _generate_analysis(self, data: List[Dict[str, Any]], intent: Optional[QueryIntent], metadata: Dict[str, Any]) -> str:
        """
        Generate a natural language analysis of the data based on intent
        """
        try:
            if not data:
                return "No data available for analysis."
            
            if intent == QueryIntent.VISITOR_COMPARISON:
                return self._analyze_visitor_comparison(data)
            elif intent == QueryIntent.PEAK_PERIOD:
                return self._analyze_peak_period(data)
            elif intent == QueryIntent.SPENDING_ANALYSIS:
                return self._analyze_spending(data)
            elif intent == QueryIntent.TREND_ANALYSIS:
                return self._analyze_trend(data)
            else:
                return self._analyze_default(data)
                
        except Exception as e:
            logger.error(f"Error generating analysis: {str(e)}")
            return "Error generating analysis."
    
    def _analyze_visitor_comparison(self, data: List[Dict[str, Any]]) -> str:
        """Generate analysis for visitor comparison data"""
        try:
            total_swiss = sum(d.get('swiss_tourists', 0) for d in data)
            total_foreign = sum(d.get('foreign_tourists', 0) for d in data)
            total = total_swiss + total_foreign
            
            swiss_percent = (total_swiss / total * 100) if total > 0 else 0
            foreign_percent = (total_foreign / total * 100) if total > 0 else 0
            
            return (
                f"During this period, there were {total:,} total visitors. "
                f"Swiss tourists made up {swiss_percent:.1f}% ({total_swiss:,}) "
                f"while foreign tourists accounted for {foreign_percent:.1f}% ({total_foreign:,})."
            )
        except Exception as e:
            logger.error(f"Error analyzing visitor comparison: {str(e)}")
            return "Error analyzing visitor comparison data."
    
    def _analyze_peak_period(self, data: List[Dict[str, Any]]) -> str:
        """Generate analysis for peak period data"""
        try:
            if not data:
                return "No data available for peak period analysis."
            
            # Find peak day
            peak_day = max(data, key=lambda x: x.get('total_visitors', 0))
            peak_date = peak_day.get('date', 'unknown date')
            peak_visitors = peak_day.get('total_visitors', 0)
            
            # Calculate average
            avg_visitors = sum(d.get('total_visitors', 0) for d in data) / len(data)
            
            return (
                f"The peak tourism day was {peak_date} with {peak_visitors:,} visitors. "
                f"This is {(peak_visitors/avg_visitors - 1) * 100:.1f}% above the average "
                f"of {avg_visitors:.0f} visitors per day."
            )
        except Exception as e:
            logger.error(f"Error analyzing peak period: {str(e)}")
            return "Error analyzing peak period data."
    
    def _analyze_spending(self, data: List[Dict[str, Any]]) -> str:
        """Generate analysis for spending data"""
        try:
            if not data:
                return "No data available for spending analysis."
            
            total_spending = sum(d.get('total_spending', 0) for d in data)
            avg_transaction = sum(d.get('average_transaction', 0) for d in data) / len(data)
            
            # Find top spending industry
            top_industry = max(data, key=lambda x: x.get('total_spending', 0))
            
            return (
                f"Total spending across all industries was ${total_spending:,.2f}. "
                f"The average transaction value was ${avg_transaction:.2f}. "
                f"The highest spending was in {top_industry.get('industry', 'unknown industry')} "
                f"with ${top_industry.get('total_spending', 0):,.2f}."
            )
        except Exception as e:
            logger.error(f"Error analyzing spending: {str(e)}")
            return "Error analyzing spending data."
    
    def _analyze_trend(self, data: List[Dict[str, Any]]) -> str:
        """Generate analysis for trend data"""
        try:
            if not data:
                return "No data available for trend analysis."
            
            # Calculate growth rates
            first_day = data[0]
            last_day = data[-1]
            
            total_growth = ((last_day.get('total_visitors', 0) / first_day.get('total_visitors', 1) - 1) * 100)
            swiss_growth = ((last_day.get('swiss_tourists', 0) / first_day.get('swiss_tourists', 1) - 1) * 100)
            foreign_growth = ((last_day.get('foreign_tourists', 0) / first_day.get('foreign_tourists', 1) - 1) * 100)
            
            return (
                f"Over this period, total visitors {total_growth:+.1f}%. "
                f"Swiss tourists {swiss_growth:+.1f}% while foreign tourists {foreign_growth:+.1f}%. "
                f"This suggests {'increasing' if total_growth > 0 else 'decreasing'} tourism activity."
            )
        except Exception as e:
            logger.error(f"Error analyzing trend: {str(e)}")
            return "Error analyzing trend data."
    
    def _analyze_default(self, data: List[Dict[str, Any]]) -> str:
        """Generate default analysis when intent is not specified"""
        try:
            if not data:
                return "No data available for analysis."
            
            # Log data structure for debugging
            logger.debug(f"Analyze default data first item: {data[0]}")
            logger.debug(f"Analyze default data keys: {data[0].keys()}")
            
            # Try to find numeric columns - handle both numeric types and string representations
            numeric_cols = []
            
            for k, v in data[0].items():
                # Direct numeric types
                if isinstance(v, (int, float)):
                    numeric_cols.append(k)
                # String representations of numbers
                elif isinstance(v, str):
                    try:
                        float(v)  # Test if it can be converted
                        numeric_cols.append(k)
                    except ValueError:
                        pass
            
            # Common numeric columns we want to prioritize
            priority_cols = ['total_visitors', 'swiss_tourists', 'foreign_tourists', 'total_spending']
            
            # Prioritize certain columns if they exist
            for col in priority_cols:
                if col in numeric_cols:
                    # Use this column for analysis
                    values = []
                    for d in data:
                        try:
                            val = d.get(col, 0)
                            if isinstance(val, str):
                                val = float(val)
                            values.append(val)
                        except (ValueError, TypeError):
                            values.append(0)
                    
                    if values:
                        total = sum(values)
                        avg = total / len(values)
                        max_val = max(values)
                        min_val = min(values)
                        
                        return (
                            f"The {col.replace('_', ' ')} data shows a total of {total:,.0f} "
                            f"with an average of {avg:,.0f}. Values range from {min_val:,.0f} "
                            f"to {max_val:,.0f}."
                        )
            
            # If no priority columns, use any numeric column
            if numeric_cols:
                col = numeric_cols[0]
                values = []
                for d in data:
                    try:
                        val = d.get(col, 0)
                        if isinstance(val, str):
                            val = float(val)
                        values.append(val)
                    except (ValueError, TypeError):
                        values.append(0)
                
                if values:
                    total = sum(values)
                    avg = total / len(values)
                    max_val = max(values)
                    min_val = min(values)
                    
                    return (
                        f"The {col.replace('_', ' ')} data shows a total of {total:,.0f} "
                        f"with an average of {avg:,.0f}. Values range from {min_val:,.0f} "
                        f"to {max_val:,.0f}."
                    )
            
            # If we get here, check if 'date' column exists for time-based analysis
            if 'date' in data[0]:
                return f"Data available for {len(data)} time periods. Please ask for specific metrics like visitor counts, trends, or spending information."
                
            return "No numeric data available for analysis. Please try a more specific query."
                
        except Exception as e:
            logger.error(f"Error generating default analysis: {str(e)}")
            logger.error(traceback.format_exc())
            return "Error generating analysis. Please try a more specific query."
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'db_service'):
            self.db_service.close()

    def __del__(self):
        """Cleanup when the service is destroyed"""
        self.close()

    def _get_cached_query_result(self, sql_query):
        """Get cached query result if available and not expired"""
        if sql_query in self._query_cache:
            cache_entry = self._query_cache[sql_query]
            cache_time = cache_entry.get('timestamp', 0)
            current_time = time.time()
            
            # Check if cache is still valid
            if current_time - cache_time <= self._cache_ttl:
                logger.debug(f"Using cached query result for: {sql_query[:100]}...")
                return cache_entry.get('data')
        
        return None

    def _cache_query_result(self, sql_query, data):
        """Cache the query result with a timestamp"""
        self._query_cache[sql_query] = {
            'data': data,
            'timestamp': time.time()
        }
        # Limit cache size to avoid memory issues
        if len(self._query_cache) > 50:
            # Simple strategy: remove oldest entries
            oldest_key = min(self._query_cache.keys(), key=lambda k: self._query_cache[k]['timestamp'])
            del self._query_cache[oldest_key] 

    def _is_visitor_comparison_request(self, message: str) -> bool:
        """
        Determine if a message is asking for visitor comparison between regions
        """
        comparison_keywords = [
            "compare", "comparison", "comparing", "difference", "differences", 
            "versus", "vs", "against", "between", "distribution", "choropleth", 
            "heat map", "heatmap", "color coding", "color-coded", "ratio",
            "visualization", "visualize", "visualisation", "visualise", "map"
        ]
        
        tourist_keywords = [
            "tourist", "tourists", "visitor", "visitors", "traveler", "travelers",
            "traveller", "travellers", "guest", "guests", "swiss", "foreign", 
            "domestic", "international"
        ]
        
        # Check if message contains both comparison and tourist keywords
        has_comparison = any(keyword in message.lower() for keyword in comparison_keywords)
        has_tourist = any(keyword in message.lower() for keyword in tourist_keywords)
        
        return has_comparison and has_tourist

    async def _handle_geospatial_query(self, message: str, intent: QueryIntent, region_info: Dict[str, Any]) -> Dict[str, Any]:
        """Handle geospatial queries specifically"""
        try:
            # Extract region name and type from region_info
            region_name = region_info.get('region_name', '')
            region_type = region_info.get('region_type', 'unknown') # Default to unknown
            
            # Check if this is a visitor comparison request
            if self._is_visitor_comparison_request(message):
                intent = QueryIntent.VISITOR_COMPARISON
                logger.info(f"Detected visitor comparison request, overriding intent to {intent}")
            
            if not region_name:
                logger.error("Region name missing from intent parsing for geospatial query.")
                return { # Return error if region name is missing
                    'message_id': str(uuid.uuid4()),
                    'content': "Could not identify a region name in your query.",
                    'response': "Could not identify a region name in your query.",
                    'visualization': None,
                    'sql_query': None,
                    'status': 'error'
                }
            
            logger.info(f"Processing geospatial query for {region_name} ({region_type})")
            
            # Check if this is explicitly a map request
            is_map_request = region_info.get('is_map_request', False)
            fallback_intent = region_info.get('fallback_intent', QueryIntent.TREND_ANALYSIS)
            
            # Search for regions using the optimized service method
            regions = self.geo_insights_service.search_regions(region_name, region_type)
            
            if not regions:
                logger.warning(f"No geographic data found for {region_name}. Checking if we should use temporal fallback.")
                
                # If this is not explicitly a map request, or we have a fallback intent,
                # generate a temporal analysis query instead
                if not is_map_request or fallback_intent:
                    logger.info(f"Using fallback intent: {fallback_intent}")
                    
                    # Generate a time-based query for the region
                    fallback_query = f"Show me tourism trends in {region_name} over time"
                    
                    # Build a SQL query for temporal analysis
                    query_result = self.sql_generator.generate_sql_query(fallback_query)
                    if not query_result or 'error' in query_result:
                        return {
                            'message_id': str(uuid.uuid4()),
                            'content': f"I couldn't find any data for {region_name}. Please try a different region name.",
                            'response': f"I couldn't find any data for {region_name}. Please try a different region name.",
                            'visualization': None,
                            'sql_query': None,
                            'status': 'error'
                        }
                    
                    sql_query = query_result.get('query', '')
                    
                    # Execute the SQL query
                    try:
                        results = self.db_service.execute_query(sql_query)
                        if not results:
                            return {
                                'message_id': str(uuid.uuid4()),
                                'content': f"I couldn't find any data for {region_name}. Please try a different region name.",
                                'response': f"I couldn't find any data for {region_name}. Please try a different region name.",
                                'visualization': None,
                                'sql_query': sql_query,
                                'status': 'error'
                            }
                        
                        # Generate visualization for the temporal data
                        visualization = generate_visualization(results, fallback_intent)
                        visualization_json = json.dumps(visualization, cls=DateTimeEncoder) if visualization else None
                        
                        # Generate analysis from temporal data
                        analysis = generate_analysis_summary(results, fallback_intent)
                        
                        return {
                            'message_id': str(uuid.uuid4()),
                            'content': analysis,
                            'response': analysis,
                            'visualization': visualization_json,
                            'sql_query': sql_query,
                            'status': 'success'
                        }
                    except Exception as exec_error:
                        logger.error(f"Error executing fallback query: {str(exec_error)}")
                        # Continue to standard error response
                
                # If fallback failed or wasn't appropriate, return the standard error
                return {
                    'message_id': str(uuid.uuid4()),
                    'content': f"I couldn't find any data for {region_name}. Please try a different region name.",
                    'response': f"I couldn't find any data for {region_name}. Please try a different region name.",
                    'visualization': None,
                    'sql_query': None,
                    'status': 'error'
                }
            
            # Prepare data for visualization - Create complete region data structure
            for i, region in enumerate(regions):
                # Set a unique region_id based on type and name
                # Format: State_ticino or Msa_lugano
                region_id = f"{region['geo_type']}_{region['geo_name'].lower().replace(' ', '_')}"
                region['region_id'] = region_id
                region['region_name'] = region['geo_name']
                region['region_type'] = region['geo_type']
                
                # Add default center coordinates if missing
                if 'central_latitude' not in region or not region['central_latitude']:
                    region['central_latitude'] = 46.8182  # Default to Switzerland
                if 'central_longitude' not in region or not region['central_longitude']:
                    region['central_longitude'] = 8.2275  # Default to Switzerland
                
                # Ensure numeric values are properly formatted
                region['total_visitors'] = float(region.get('total_visitors', 0) or 0)
                region['swiss_tourists'] = float(region.get('swiss_tourists', 0) or 0)
                region['foreign_tourists'] = float(region.get('foreign_tourists', 0) or 0)
            
            # Get insights for the region
            insights = self.geo_insights_service.get_region_insights(regions[0]['region_id'])
            
            # Generate visualization based on intent
            visualization = None
            if intent == QueryIntent.REGION_ANALYSIS:
                visualization = self.geo_visualization_service.create_region_map(regions)
            elif intent == QueryIntent.HOTSPOT_DETECTION:
                hotspots = self.geo_insights_service.get_hotspots(regions[0]['region_id'])
                visualization = self.geo_visualization_service.create_hotspot_map(hotspots)
            elif intent == QueryIntent.INDUSTRY_ANALYSIS:
                # For industry analysis, we need spatial patterns data but render it differently
                patterns = self.geo_insights_service.get_spatial_patterns(regions[0]['region_id'])
                visualization = self.geo_visualization_service.create_industry_bounding_box_map(patterns, regions[0]['region_id'])
            elif intent == QueryIntent.SPATIAL_PATTERN:
                patterns = self.geo_insights_service.get_spatial_patterns(regions[0]['region_id'])
                
                # Check if this is an industry analysis request
                industry_keywords = ['industry', 'industries', 'sector', 'sectors', 'business', 'businesses', 
                                   'category', 'categories', 'aggregate', 'merged', 'bounding box', 'boundingbox', 
                                   'bound', 'boundary', 'area', 'zone', 'merge', 'color map', 'color', 'color coding']
                is_industry_request = any(keyword in message.lower() for keyword in industry_keywords)
                
                if is_industry_request:
                    # Use the industry bounding box visualization instead
                    visualization = self.geo_visualization_service.create_industry_bounding_box_map(patterns, regions[0]['region_id'])
                else:
                    visualization = self.geo_visualization_service.create_spatial_pattern_chart(patterns, regions[0]['region_id'])
            elif intent == QueryIntent.VISITOR_COMPARISON:
                # Determine which metric to use based on message content
                metric = "total_visitors"  # Default
                
                # Check for explicit mentions of Swiss or foreign tourists
                if any(word in message.lower() for word in ["swiss", "domestic", "local"]):
                    metric = "swiss"
                elif any(word in message.lower() for word in ["foreign", "international", "overseas"]):
                    metric = "foreign"
                elif any(word in message.lower() for word in ["ratio", "proportion", "comparison", "versus", "vs"]):
                    metric = "ratio"
                    
                # Use the specialized visitor comparison map
                visualization = self.geo_visualization_service.create_visitor_comparison_map(regions, metric)
            else:  # Default to GEO_SPATIAL
                visualization = self.geo_visualization_service.create_region_map(regions)
            
            # If visualization is None, create a default map
            if visualization is None:
                visualization = {
                    'type': 'region_map',
                    'data': {
                        'center': [8.2275, 46.8182],  # Switzerland center
                        'zoom': 7,
                        'regions': [{
                            'name': region['region_name'],
                            'type': region['region_type'],
                            'total_visitors': float(region.get('total_visitors', 0) or 0)
                        } for region in regions]
                    }
                }
            
            # Convert visualization to JSON
            try:
                visualization_json = json.dumps(visualization, cls=DateTimeEncoder) if visualization else None
            except Exception as viz_error:
                logger.error(f"Error converting visualization to JSON: {str(viz_error)}")
                visualization_json = None
            
            # Generate analysis text
            analysis = self._generate_geospatial_analysis(insights, intent)
            
            return {
                'message_id': str(uuid.uuid4()),
                'content': analysis,
                'response': analysis,
                'visualization': visualization_json,
                'sql_query': None,
                'status': 'success'
            }
        
        except Exception as e:
            logger.error(f"Error processing geospatial query: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'message_id': str(uuid.uuid4()),
                'content': "Sorry, I encountered an error while processing your geospatial request.",
                'response': "Sorry, I encountered an error while processing your geospatial request.",
                'visualization': None,
                'sql_query': None,
                'status': 'error'
            }
    
    def _generate_geospatial_analysis(self, insights: Dict[str, Any], intent: QueryIntent) -> str:
        """Generate natural language analysis for geospatial data"""
        try:
            if not insights:
                return "No geospatial data available for analysis."
            
            if intent == QueryIntent.HOTSPOT_DETECTION:
                # Find highest visitor count area
                return (
                    f"The tourism hotspot in {insights.get('region_name', 'this region')} has "
                    f"{insights.get('peak_visitors', 0):,} visitors at its peak. "
                    f"Swiss tourists account for {insights.get('swiss_tourists', 0):,} visitors, while "
                    f"foreign tourists account for {insights.get('foreign_tourists', 0):,} visitors."
                )
            elif intent == QueryIntent.INDUSTRY_ANALYSIS:
                # Analyze industry distribution
                return (
                    f"Tourism in {insights.get('region_name', 'this region')} shows a distribution across multiple industries "
                    f"with a total of {insights.get('total_visitors', 0):,} visitors. "
                    f"Each colored area represents a different industry sector, with point size indicating activity level. "
                    f"The map displays industry-specific boundaries to help visualize where different business types are concentrated."
                )
            elif intent == QueryIntent.SPATIAL_PATTERN:
                # Analyze spatial distribution pattern
                return (
                    f"Tourism in {insights.get('region_name', 'this region')} shows a spatial pattern with "
                    f"{insights.get('total_visitors', 0):,} total visitors. "
                    f"The average daily Swiss tourist count is {insights.get('avg_swiss_tourists', 0):.0f}, "
                    f"while the average foreign tourist count is {insights.get('avg_foreign_tourists', 0):.0f}."
                )
            else:  # Default to region analysis
                # Basic region summary
                swiss_percent = 0
                if float(insights.get('total_visitors', 0)) > 0:
                    swiss_percent = (float(insights.get('swiss_tourists', 0)) / float(insights.get('total_visitors', 1))) * 100
                
                return (
                    f"{insights.get('region_name', 'This region')} received {insights.get('total_visitors', 0):,} visitors. "
                    f"Approximately {swiss_percent:.1f}% were domestic tourists, "
                    f"with the remainder being international visitors."
                )
            
        except Exception as e:
            logger.error(f"Error generating geospatial analysis: {str(e)}")
            return "Error generating geospatial analysis."

    async def _handle_intent(self, message: str, intent_result: Dict[str, Any]) -> Dict[str, Any]:
        """Handle all intents by routing to appropriate handler"""
        intent = intent_result.get('intent', QueryIntent.TREND_ANALYSIS)
        metadata = intent_result.get('metadata', {})
        region_info = intent_result.get('region_info', {})
        
        # Override intent if this is a visitor comparison request
        if self._is_visitor_comparison_request(message) and region_info:
            intent = QueryIntent.VISITOR_COMPARISON
            intent_result['intent'] = QueryIntent.VISITOR_COMPARISON
            logger.info(f"Detected visitor comparison request, overriding intent to {intent}")

        # Handle geospatial queries separately
        if intent in [QueryIntent.GEO_SPATIAL, QueryIntent.REGION_ANALYSIS, 
                      QueryIntent.HOTSPOT_DETECTION, QueryIntent.SPATIAL_PATTERN]:
            logger.info("Routing to geospatial query handler")
            return await self._handle_geospatial_query(message, intent, region_info)
        # Add a more flexible router that can detect map requests even if the intent parser missed it
        elif contains_geo_terms and contains_geo_viz_terms:
            logger.info("Detected map request from keywords, routing to geospatial query handler")
            
            # Create basic region info from message if not available
            if not region_info:
                # Extract region name - simple approach
                region_match = re.search(r"(?:in|of|for|at)\s+(?:the\s+)?(\w+(?:\s+\w+){0,3})", message.lower())
                region_name = region_match.group(1) if region_match else "not specified in the query"
                
                region_info = {
                    'region_name': region_name,
                    'region_type': 'unknown',
                    'is_map_request': True,
                    'fallback_intent': QueryIntent.TREND_ANALYSIS
                }
            
            return await self._handle_geospatial_query(message, QueryIntent.GEO_SPATIAL, region_info)

        # Generate SQL query based on user message
        try:
            # Add original message to help with specific query optimizations
            query_result = self.sql_generator.generate_sql_query(message)
            
            # If this is a parsing object, add the original message for context
            if isinstance(query_result, dict) and not 'error' in query_result:
                query_result['original_message'] = message
                
            logger.debug(f"Generated SQL query: {query_result}")
            
            if not query_result or 'error' in query_result:
                error_msg = query_result.get('error', 'Unknown error in query generation') if query_result else 'Failed to generate query'
                logger.error(f"SQL generation failed for message: {message}, error: {error_msg}")
                return {
                    'message_id': str(uuid.uuid4()),
                    'content': "I couldn't understand your query. Could you please rephrase it?",
                    'response': "I couldn't understand your query. Could you please rephrase it?",
                    'visualization': None,
                    'sql_query': None,
                    'status': 'error'
                }
            
            sql_query = query_result.get('query', '')
            intent_str = query_result.get('intent', 'visitor_count')
            
            # Convert string intent to enum
            intent = None
            for intent_enum in QueryIntent:
                if intent_enum.value == intent_str:
                    intent = intent_enum
                    break
            
            metadata = query_result.get('metadata', {})
            
            # Validate query - skip validation for direct API calls 
            # to prevent the double-query issue
            if not is_direct_query:
                try:
                    # Check if validate_query is available
                    # Skip validation for simple queries to reduce database load
                    is_simple_query = len(sql_query.strip()) < 500 and "UNION" not in sql_query.upper() and "WITH" not in sql_query.upper()
                    
                    if hasattr(self.db_service, 'validate_query') and not is_simple_query:
                        is_valid = self.db_service.validate_query(sql_query)
                        if not is_valid:
                            logger.error(f"Invalid SQL query: {sql_query}")
                            return {
                                'message_id': str(uuid.uuid4()),
                                'content': "I couldn't generate a valid query for your question. Could you please rephrase it?",
                                'response': "I couldn't generate a valid query for your question. Could you please rephrase it?",
                                'visualization': None,
                                'sql_query': sql_query,
                                'status': 'error'
                            }
                    else:
                        # Skip validation if method not available or query is simple
                        logger.info(f"Skipping validation for query: {'simple query' if is_simple_query else 'validation not available'}")
                except Exception as e:
                    logger.error(f"Error during query validation: {str(e)}")
                    logger.error(traceback.format_exc())
            else:
                logger.info(f"Skipping validation for direct API call")
            
            # Execute query and get data
            try:
                # Check cache first
                cached_data = self._get_cached_query_result(sql_query)
                if cached_data is not None:
                    data = cached_data
                else:
                    data = self.db_service.execute_query(sql_query)
                    # Cache the result for future use
                    self._cache_query_result(sql_query, data)
            except TimeoutError as e:
                return {
                    'message_id': str(uuid.uuid4()),
                    'content': str(e),
                    'response': str(e),
                    'visualization': None,
                    'sql_query': sql_query,
                    'status': 'error'
                }
            
            if not data:
                # Check if this is a future date query
                time_range = metadata.get('time_range', {})
                start_date = time_range.get('start_date', '')
                
                if start_date and datetime.strptime(start_date, '%Y-%m-%d') > datetime.now():
                    return {
                        'message_id': str(uuid.uuid4()),
                        'content': f"I don't have data for future dates like {start_date}. Would you like to see data from a past period instead?",
                        'response': f"I don't have data for future dates like {start_date}. Would you like to see data from a past period instead?",
                        'visualization': None,
                        'sql_query': sql_query,
                        'status': 'error'
                    }
                
                return {
                    'message_id': str(uuid.uuid4()),
                    'content': "I couldn't find any data matching your query.",
                    'response': "I couldn't find any data matching your query.",
                    'visualization': None,
                    'sql_query': sql_query,
                    'status': 'error'
                }
            
            # Generate visualization based on intent and data
            try:
                visualization_data = generate_visualization(data, intent)
                visualization_json = json.dumps(visualization_data, cls=DateTimeEncoder) if visualization_data else None
            except Exception as viz_error:
                logger.error(f"Error generating visualization: {str(viz_error)}")
                logger.error(traceback.format_exc())
                visualization_json = None
            
            # Generate analysis summary
            analysis = self._generate_analysis(data, intent, metadata)
            
            return {
                'message_id': str(uuid.uuid4()),
                'content': analysis,
                'response': analysis,
                'visualization': visualization_json,
                'sql_query': sql_query,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error processing SQL generation: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'message_id': str(uuid.uuid4()),
                'content': "Sorry, I encountered an error while processing your request.",
                'response': "Sorry, I encountered an error while processing your request.",
                'visualization': None,
                'sql_query': None,
                'status': 'error'
            }

    async def _generate_visualization(self, data: List[Dict[str, Any]], intent_result: Dict[str, Any]) -> Optional[str]:
        """
        Generate a visualization based on the data and intent
        """
        try:
            if not data:
                return None
                
            intent = intent_result.get('intent')
            metadata = intent_result.get('metadata', {})
            
            # Generate visualization based on intent
            if intent == QueryIntent.VISITOR_COMPARISON:
                return await self._visualize_visitor_comparison(data)
            elif intent == QueryIntent.PEAK_PERIOD:
                return await self._visualize_peak_period(data)
            elif intent == QueryIntent.SPENDING:
                return await self._visualize_spending(data)
            elif intent == QueryIntent.TREND:
                return await self._visualize_trend(data)
            else:
                return await self._visualize_default(data)
                
        except Exception as e:
            logger.error(f"Error generating visualization: {str(e)}")
            logger.error(traceback.format_exc())
            return None 