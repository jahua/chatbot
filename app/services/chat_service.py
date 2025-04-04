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
from app.utils.visualization import create_visualization, figure_to_base64
from app.utils.sql_generator import generate_sql_query
from app.utils.analysis_generator import generate_analysis_summary, format_results_as_markdown_table

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self, db: DatabaseService, openai_adapter: OpenAIAdapter, schema_manager: SchemaManager):
        """Initialize chat service with dependencies"""
        self.db = db
        self.openai_adapter = openai_adapter
        self.schema_manager = schema_manager
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
    
    async def process_message(self, message: str) -> ChatResponse:
        """Process incoming chat message"""
        try:
            # Initialize response
            response = ChatResponse(
                message_id=str(uuid.uuid4()),
                content="",
                sql_query="",
                visualization=None,
                status="processing",
                response="Processing your request..."
            )

            # Generate SQL query
            logger.info("Generating SQL query")
            sql_query = await self.generate_sql_query(message)
            if not sql_query:
                return ChatResponse(
                    message_id=str(uuid.uuid4()),
                    content="I couldn't generate a SQL query for your question. Could you rephrase it?",
                    sql_query="",
                    visualization=None,
                    status="error",
                    response="I couldn't generate a SQL query for your question. Could you rephrase it?"
                )
            
            response.sql_query = sql_query
            logger.info(f"Generated SQL query: \n{sql_query}")

            # Execute query with timeout handling
            try:
                results = await self.execute_query(sql_query)
                if not results:
                    return ChatResponse(
                        message_id=str(uuid.uuid4()),
                        content="The query returned no results. Try adjusting your question.",
                        sql_query=sql_query,
                        visualization=None,
                        status="no_results",
                        response="The query returned no results. Try adjusting your question."
                    )
            except TimeoutError as te:
                return ChatResponse(
                    message_id=str(uuid.uuid4()),
                    content=str(te),
                    sql_query=sql_query,
                    visualization=None,
                    status="timeout",
                    response=str(te)
                )
            except Exception as e:
                return ChatResponse(
                    message_id=str(uuid.uuid4()),
                    content=f"Error executing query: {str(e)}",
                    sql_query=sql_query,
                    visualization=None,
                    status="error",
                    response=f"Error executing query: {str(e)}"
                )

            # Format results as markdown table
            markdown_table = format_results_as_markdown_table(results)
            
            # Generate visualization if applicable
            try:
                visualization = create_visualization(results, message)
                if visualization:
                    response.visualization = figure_to_base64(visualization)
            except Exception as e:
                logger.error(f"Error creating visualization: {str(e)}")
                # Continue without visualization

            # Generate analysis
            try:
                analysis = generate_analysis_summary(results, message)
                response.content = f"## 📊 Query Results\n\n{markdown_table}\n\n{analysis}"
                response.response = response.content
                response.status = "success"
            except Exception as e:
                logger.error(f"Error generating analysis: {str(e)}")
                response.content = f"## 📊 Query Results\n\n{markdown_table}\n\nI encountered an error analyzing the results."
                response.response = response.content
                response.status = "partial_success"

            return response

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            logger.error(traceback.format_exc())
            return ChatResponse(
                message_id=str(uuid.uuid4()),
                content=f"An error occurred: {str(e)}",
                sql_query="",
                visualization=None,
                status="error",
                response=f"An error occurred: {str(e)}"
            )
    
    async def get_schema_info(self) -> List[Dict[str, Any]]:
        """Get database schema information"""
        try:
            logger.debug("Getting schema information")
            schema_query = """
                SELECT table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'data_lake'
                ORDER BY table_name, ordinal_position;
            """
            results = await self.db.execute_query(schema_query)
            schema_dict = {}
            for row in results:
                table = row['table_name']
                if table not in schema_dict:
                    schema_dict[table] = []
                schema_dict[table].append(f"{row['column_name']} ({row['data_type']})")
            
            schema_string = "\n".join([f"Table {table}: {', '.join(columns)}" for table, columns in schema_dict.items()])
            logger.debug(f"Retrieved schema information: \n{schema_string}")
            # Return raw results for the endpoint
            return results
        except Exception as e:
            logger.error(f"Error getting schema information: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    async def close(self):
        """Cleanup resources"""
        if self.openai_adapter:
            await self.openai_adapter.close()
        logger.info("ChatService closed successfully")

    def __del__(self):
        """Cleanup when the service is destroyed"""
        if hasattr(self, 'db'):
            self.db.close() 

    async def generate_sql_query(self, message: str) -> str:
        """Generate SQL query from user message"""
        try:
            sql_query = generate_sql_query(message)
            logger.info(f"Generated SQL query: {sql_query}")
            return sql_query
        except Exception as e:
            logger.error(f"Error generating SQL query: {str(e)}")
            raise
    
    async def execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        try:
            results = self.db.execute_query(sql_query)
            logger.info(f"Query returned {len(results)} rows")
            return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    async def generate_analysis(self, data: List[Dict[str, Any]], query: str) -> str:
        """Generate analysis from query results"""
        try:
            analysis = generate_analysis(data, query)
            logger.info("Generated analysis summary")
            return analysis
        except Exception as e:
            logger.error(f"Error generating analysis: {str(e)}")
            raise 