from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from app.db.database import SessionLocal, DatabaseService
from app.llm.openai_adapter import OpenAIAdapter
from app.schemas.chat import ChatMessage, ChatResponse
from sqlalchemy import text
import pandas as pd
import logging
import traceback
import asyncio
from app.services.conversation_service import ConversationService
import json
from datetime import datetime
import plotly.graph_objects as go
import time
from fastapi import HTTPException
from app.core.config import settings
from decimal import Decimal
import uuid
from app.utils.sql_utils import extract_sql_query, clean_sql_query
import re

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self):
        """Initialize chat service with LLM and database connections"""
        try:
            logger.info("Initializing ChatService")
            self.llm = OpenAIAdapter()
            self.db = DatabaseService()
            logger.info("ChatService initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing ChatService: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def is_conversational_message(self, message: str) -> bool:
        """Detect if a message is conversational rather than a data query"""
        # List of common greetings and conversational phrases
        greetings = [
            "hi", "hello", "hey", "greetings", "good morning", "good afternoon", 
            "good evening", "how are you", "what's up", "howdy", "hola", 
            "nice to meet you", "thanks", "thank you"
        ]
        
        # Clean the message
        cleaned_message = message.lower().strip()
        
        # Check for simple greetings
        if any(greeting in cleaned_message for greeting in greetings):
            return True
            
        # Check for very short messages (less than 3 words)
        if len(cleaned_message.split()) < 3:
            return True
            
        # Check if it looks like a question about the bot itself
        bot_references = ["you", "your", "yourself", "chatbot", "bot", "assistant", "ai"]
        if any(ref in cleaned_message.split() for ref in bot_references) and len(cleaned_message.split()) < 6:
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
    
    async def process_message(self, message: str) -> Dict[str, Any]:
        """Process user message and return response"""
        start_time = time.time()
        
        try:
            logger.debug(f"Processing message: {message}")
            
            # Check if this is a conversational message rather than a data query
            if self.is_conversational_message(message):
                logger.info("Detected conversational message, providing standard response")
                response = {
                    "response": "Hello! I'm your tourism data assistant. I can help you analyze tourism data. Ask me questions like 'What are the peak tourism periods in 2023?' or 'What are the weekly visitor patterns in spring 2023?'",
                    "data": [],
                    "sql_query": None,
                    "error": None
                }
                return response
            
            # Check if the user is asking about available data or schema
            if self.is_schema_inquiry(message):
                logger.info("Detected schema inquiry, providing schema information")
                schema_summary = await self.get_schema_summary()
                response = {
                    "response": schema_summary,
                    "data": [],
                    "sql_query": None,
                    "error": None
                }
                return response
            
            # Get SQL query from LLM
            sql_query = await self.llm.generate_sql_query(message)
            logger.debug(f"Generated SQL query: {sql_query}")
            
            if not sql_query:
                logger.error("No SQL query generated")
                return {"response": "Sorry, I couldn't generate a SQL query for your question.", "error": "Could not generate SQL query"}
            
            # Check if the generated "SQL" is actually conversational text
            # This can happen if the LLM outputs explanatory text instead of SQL
            if re.search(r'\b(hello|hi|hey|greetings|sorry|assist|help you|welcome)\b', sql_query.lower()):
                logger.warning("LLM returned conversational text instead of SQL")
                return {
                    "response": "I apologize, but I couldn't generate a proper SQL query for your question. Please try rephrasing your question to focus on specific tourism data analysis.",
                    "data": [],
                    "sql_query": None,
                    "error": "Generated text was conversational, not SQL"
                }
            
            # Clean and validate SQL query if needed
            cleaned_query = sql_query.strip()

            # Execute query
            results = []
            db_error = None
            try:
                logger.debug(f"Executing SQL query: {cleaned_query}")
                start_db_time = time.time()
                results = await self.db.execute_query(cleaned_query)
                db_duration = time.time() - start_db_time
                logger.debug(f"Query returned {len(results)} results in {db_duration:.2f}s")
            except Exception as e:
                logger.error(f"Database query error: {str(e)}")
                logger.error(traceback.format_exc())
                error_str = str(e)
                # Provide more user-friendly error messages for common database errors
                if "column" in error_str and "does not exist" in error_str:
                    # Extract column name from the error message
                    column_match = re.search(r'column "([^"]+)" does not exist', error_str)
                    if column_match:
                        column_name = column_match.group(1)
                        
                        # Provide specific guidance for common column issues
                        if column_name == "transaction_date":
                            return {
                                "response": "I encountered an error with the column name. The correct column for transaction dates in the master_card table is 'txn_date', not 'transaction_date'.",
                                "error": f"Column name error: Use 'txn_date' instead of 'transaction_date'"
                            }
                        elif column_name == "amount":
                            return {
                                "response": "I encountered an error with the column name. The correct column for transaction amounts in the master_card table is 'txn_amt', not 'amount'.",
                                "error": f"Column name error: Use 'txn_amt' instead of 'amount'"
                            }
                        elif column_name == "date":
                            return {
                                "response": "I encountered an error with the column name. The correct column for dates in the aoi_days_raw table is 'aoi_date', not 'date'.",
                                "error": f"Column name error: Use 'aoi_date' instead of 'date'"
                            }
                        else:
                            return {
                                "response": f"I encountered an error with the database query. The column '{column_name}' does not exist in the table. Please try a different query with the correct column names.",
                                "error": f"Column name error: '{column_name}' does not exist"
                            }
                    else:
                        return {
                            "response": "I encountered an error with the database schema. A column referenced in the query doesn't exist. Please try a different query with the correct column names.",
                            "error": f"Database error: {error_str}"
                        }
                elif "syntax error" in error_str.lower():
                    return {
                        "response": "I encountered a syntax error in the SQL query. This might be due to incorrect SQL formatting. Please try rephrasing your question.",
                        "error": f"SQL syntax error: {error_str}"
                    }
                else:
                    return {
                        "response": f"There was an error executing the database query: {error_str}. Please try rephrasing your question.",
                        "error": f"Database error: {error_str}"
                    }
            
            # Convert results to DataFrame for potential analysis/visualization
            df = pd.DataFrame(results)

            # Generate response using LLM
            llm_response = None
            llm_error = None
            try:
                logger.debug("Generating LLM response")
                start_llm_time = time.time()
                llm_response = await self.llm.generate_response(message, df.to_string() if not df.empty else "No data returned from query.")
                llm_duration = time.time() - start_llm_time
                logger.debug(f"LLM response generated successfully in {llm_duration:.2f}s")
            except Exception as e:
                logger.error(f"LLM response error: {str(e)}")
                logger.error(traceback.format_exc())
                llm_error = f"LLM error: {str(e)}"
                llm_response = "I found some data but couldn't generate a detailed analysis. Please try rephrasing your question."
            
            # Prepare final response structure
            response_data = {
                "response": llm_response if llm_response else "Could not generate a textual response.",
                "data": results, # Send raw results for frontend to potentially display
                "sql_query": cleaned_query,
                "error": llm_error # Report LLM error if any
            }
            
            total_duration = time.time() - start_time
            logger.info(f"Total processing time for message: {total_duration:.2f}s")
            return response_data
            
        except Exception as e:
            logger.error(f"Critical error processing message: {str(e)}")
            logger.error(traceback.format_exc())
            return {"response": f"I encountered an error processing your request: {str(e)}", "error": f"Critical error processing message: {str(e)}"}
            
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
    
    def close(self):
        """Close connections"""
        try:
            logger.debug("Closing ChatService connections")
            if hasattr(self, 'db'):
                self.db.close()
            logger.debug("ChatService connections closed")
        except Exception as e:
            logger.error(f"Error closing ChatService: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def __del__(self):
        """Cleanup when the service is destroyed"""
        if hasattr(self, 'db'):
            self.db.close() 