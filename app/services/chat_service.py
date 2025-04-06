from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from app.db.database import SessionLocal, DatabaseService, get_db
from app.llm.openai_adapter import OpenAIAdapter
from app.schemas.chat import ChatMessage, ChatResponse
from app.db.schema_manager import SchemaManager
from sqlalchemy import text
import pandas as pd
import logging
import traceback
import asyncio
from app.services.conversation_service import ConversationService
import json
from datetime import datetime
import time
from fastapi import HTTPException
from app.core.config import settings
from decimal import Decimal
import uuid
from app.utils.sql_utils import extract_sql_query, clean_sql_query
import re
import psycopg2
import plotly.graph_objects as go
from app.utils.visualization import generate_visualization
from app.utils.sql_generator import SQLGenerator
from app.utils.db_utils import execute_query
from app.utils.analysis_generator import generate_analysis_summary
from app.utils.intent_parser import QueryIntent
from app.services.database_service import DatabaseService
from app.utils.intent_parser import IntentParser
from app.models.chat_request import ChatRequest

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self):
        """Initialize the chat service with OpenAI adapter and SQL generator"""
        self.llm = OpenAIAdapter()
        self.sql_generator = SQLGenerator()
        self.db_service = DatabaseService()
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
    
    async def get_schema_summary(self) -> str:
        """Generate a user-friendly summary of the database schema"""
        try:
            schema_data = await self.get_schema_info()
            
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
    
    async def process_chat(self, message: str, session_id: str) -> Dict[str, Any]:
        """Process a chat message and return a response with visualization"""
        try:
            # Check if it's a conversational message
            if self.is_conversational_message(message):
                return {
                    'response': "I'm here to help you analyze tourism data. Please ask me about visitor statistics, spending patterns, or tourism trends!",
                    'visualization': None,
                    'sql_query': None,
                    'data': None,
                    'analysis': None,
                    'metadata': {'type': 'conversation'}
                }

            # Check if it's a schema inquiry
            if self.is_schema_inquiry(message):
                schema_summary = await self.get_schema_summary()
                return {
                    'response': schema_summary,
                    'visualization': None,
                    'sql_query': None,
                    'data': None,
                    'analysis': None,
                    'metadata': {'type': 'schema_info'}
                }

            # Generate SQL query based on user message
            try:
                query_result = self.sql_generator.generate_sql_query(message)
                logger.debug(f"Generated SQL query: {query_result}")
                
                if not query_result or 'error' in query_result:
                    error_msg = query_result.get('error', 'Unknown error in query generation') if query_result else 'Failed to generate query'
                    logger.error(f"SQL generation failed for message: {message}, error: {error_msg}")
                    return {
                        'response': "I couldn't understand your query. Could you please rephrase it?",
                        'visualization': None,
                        'sql_query': None,
                        'data': None,
                        'analysis': None,
                        'metadata': {'type': 'error', 'reason': 'query_generation_failed', 'message': message, 'error': error_msg}
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
                
                # Validate query
                try:
                    is_valid = await self.db_service.validate_query(sql_query)
                    if not is_valid:
                        logger.error(f"Invalid SQL query: {sql_query}")
                        return {
                            'response': "I couldn't generate a valid query for your question. Could you please rephrase it?",
                            'visualization': None,
                            'sql_query': sql_query,
                            'data': None,
                            'analysis': None,
                            'metadata': {'type': 'error', 'reason': 'invalid_query', 'query': sql_query}
                        }
                except Exception as e:
                    logger.error(f"Error during query validation: {str(e)}")
                    logger.error(traceback.format_exc())
                    is_valid = False
                
                # Execute query and get data
                data = await self.db_service.execute_query(sql_query)
                
                if not data:
                    # Check if this is a future date query
                    time_range = metadata.get('time_range', {})
                    start_date = time_range.get('start_date', '')
                    
                    if start_date and datetime.strptime(start_date, '%Y-%m-%d') > datetime.now():
                        return {
                            'response': f"I don't have data for future dates like {start_date}. Would you like to see data from a past period instead?",
                            'visualization': None,
                            'sql_query': sql_query,
                            'data': None,
                            'analysis': None,
                            'metadata': {'type': 'future_date', 'start_date': start_date}
                        }
                    
                    return {
                        'response': "I couldn't find any data matching your query.",
                        'visualization': None,
                        'sql_query': sql_query,
                        'data': None,
                        'analysis': None,
                        'metadata': {'type': 'no_data', **metadata}
                    }
                
                # Generate visualization based on intent and data
                try:
                    visualization_data = generate_visualization(data, intent)
                    visualization_json = visualization_data
                except Exception as viz_error:
                    logger.error(f"Error generating visualization: {str(viz_error)}")
                    logger.error(traceback.format_exc())
                    visualization_json = None
                
                # Generate analysis summary
                analysis = self._generate_analysis(data, intent, metadata)
                
                return {
                    'response': "Here's what I found based on your query.",
                    'visualization': visualization_json,
                    'sql_query': sql_query,
                    'data': data,
                    'analysis': analysis,
                    'metadata': {'type': 'success', **metadata}
                }
                
            except Exception as e:
                logger.error(f"Error processing SQL generation: {str(e)}")
                logger.error(traceback.format_exc())
                return {
                    'response': "Sorry, I encountered an error while processing your request.",
                    'visualization': None,
                    'sql_query': None,
                    'data': None,
                    'analysis': None,
                    'metadata': {'type': 'error', 'reason': 'processing_error', 'error': str(e)}
                }
            
        except Exception as e:
            logger.error(f"Error processing chat message: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'response': "Sorry, I encountered an error while processing your request.",
                'visualization': None,
                'sql_query': None,
                'data': None,
                'analysis': None,
                'metadata': {'type': 'error', 'reason': 'processing_error', 'error': str(e)}
            }
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """
        Get database schema information
        """
        try:
            return await self.db_service.get_schema_info()
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
            
            # Try to find numeric columns
            numeric_cols = [k for k, v in data[0].items() if isinstance(v, (int, float))]
            
            if numeric_cols:
                col = numeric_cols[0]
                values = [d.get(col, 0) for d in data]
                total = sum(values)
                avg = total / len(values)
                max_val = max(values)
                min_val = min(values)
                
                return (
                    f"The {col.replace('_', ' ')} data shows a total of {total:,.2f} "
                    f"with an average of {avg:,.2f}. Values range from {min_val:,.2f} "
                    f"to {max_val:,.2f}."
                )
            else:
                return "No numeric data available for analysis."
                
        except Exception as e:
            logger.error(f"Error generating default analysis: {str(e)}")
            return "Error analyzing data."
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'db_service'):
            self.db_service.close()

    def __del__(self):
        """Cleanup when the service is destroyed"""
        self.close() 