from typing import Dict, Any, Optional
import logging
import traceback
import asyncio
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferMemory
from langchain_core.runnables import RunnablePassthrough
from app.core.config import settings
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class OpenAIAdapter:
    def __init__(self):
        """Initialize OpenAIAdapter with LangChain components"""
        try:
            # Initialize OpenAI model
            self.llm = ChatOpenAI(
                model=settings.OPENAI_MODEL,
                openai_api_key=settings.OPENAI_API_KEY,
                openai_api_base="http://oapi.aivue.cn/v1",  # Base URL with /v1
                temperature=0.7
            )
            
            # Initialize memory
            self.memory = ConversationBufferMemory(
                return_messages=True,
                memory_key="chat_history"
            )
            
            # Initialize prompt template
            self.prompt = ChatPromptTemplate.from_messages([
                ("system", """You are a helpful assistant that generates SQL queries for tourism data analysis.
                You have access to the following tables:
                - data_lake.aoi_days_raw: Contains daily visitor data with columns 'aoi_date' (for the date), 'visitors' (JSONB containing 'swissTourist' and 'foreignTourist')
                - data_lake.master_card: Contains transaction data with columns:
                  * 'txn_date' (date of transaction) - USE THIS INSTEAD OF 'transaction_date'
                  * 'industry' (industry sector)
                  * 'segment' (market segment)
                  * 'txn_amt' (transaction amount) - USE THIS INSTEAD OF 'amount'
                  * 'txn_cnt' (transaction count)
                  * 'acct_cnt' (account count)
                  * Geographic data: 'geo_type', 'geo_name', 'central_latitude', 'central_longitude'
                
                Generate clear and efficient SQL queries to answer user questions about tourism patterns.
                IMPORTANT: 
                - Use 'aoi_date' instead of 'date' for the date column in aoi_days_raw table.
                - Use 'txn_date' instead of 'transaction_date' for the date column in master_card table.
                - Use 'txn_amt' instead of 'amount' for the transaction amount in master_card table.
                - When using ORDER BY with calculated columns, you must repeat the calculation in the ORDER BY clause rather than referring to the column alias.
                - Example: Instead of 'ORDER BY total_visitors DESC', use 'ORDER BY SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) DESC'"""),
                MessagesPlaceholder(variable_name="chat_history"),
                ("human", "{question}")
            ])
            
            # Create the chain
            self.chain = (
                self.prompt
                | self.llm
                | StrOutputParser()
            )
            
            logger.info("OpenAIAdapter initialized successfully using LCEL")
            
        except Exception as e:
            logger.error(f"Error initializing OpenAIAdapter: {str(e)}")
            raise
    
    async def generate_response(self, question: str, results=None) -> str:
        """Generate a response using the OpenAI model"""
        try:
            # Get chat history
            chat_history = self.memory.load_memory_variables({})["chat_history"]
            
            # Create a custom prompt based on whether we have query results
            if results is not None:
                # Create response prompt that includes the SQL results
                response_prompt = ChatPromptTemplate.from_messages([
                    ("system", """You are a helpful assistant specializing in tourism data analysis.
                    You provide clear, concise, and informative interpretations of tourism data.
                    The user has asked the following question, and we've retrieved data from our database.
                    Please analyze this data and provide a comprehensive answer to the user's question.
                    Include key trends, patterns, notable insights, and any relevant recommendations.
                    Keep your response professional but conversational."""),
                    MessagesPlaceholder(variable_name="chat_history"),
                    ("human", f"""User Question: {question}
                    
                    Query Results:
                    {results}
                    
                    Please provide a detailed analysis of this data in relation to the question.""")
                ])
                
                # Create response chain
                response_chain = response_prompt | self.llm | StrOutputParser()
                
                # Generate response with query results
                response = await response_chain.ainvoke({
                    "chat_history": chat_history
                })
            else:
                # Generate regular response without query results
                response = await self.chain.ainvoke({
                    "question": question,
                    "chat_history": chat_history
                })
            
            # Update memory
            self.memory.save_context({"question": question}, {"output": response})
            
            return response
            
        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def generate_sql_query(self, message: str) -> str:
        """Generate SQL query from natural language using OpenAI"""
        try:
            # Create a SQL-specific prompt template
            sql_prompt = ChatPromptTemplate.from_messages([
                ("system", """You are a helpful assistant that generates SQL queries for tourism data analysis.
                You have access to the following tables:
                - data_lake.aoi_days_raw: Contains daily visitor data with columns 'aoi_date' (for the date), 'visitors' (JSONB containing 'swissTourist' and 'foreignTourist')
                - data_lake.master_card: Contains transaction data with columns:
                  * 'txn_date' (date of transaction) - USE THIS INSTEAD OF 'transaction_date'
                  * 'industry' (industry sector)
                  * 'segment' (market segment)
                  * 'txn_amt' (transaction amount) - USE THIS INSTEAD OF 'amount'
                  * 'txn_cnt' (transaction count)
                  * 'acct_cnt' (account count)
                  * Geographic data: 'geo_type', 'geo_name', 'central_latitude', 'central_longitude'
                
                Generate ONLY THE SQL query without any explanation, markdown formatting, or backticks.
                Use proper JSON field access with ->> for JSONB fields and cast numeric values appropriately.
                IMPORTANT: 
                - Use 'aoi_date' instead of 'date' for the date column in aoi_days_raw table.
                - Use 'txn_date' instead of 'transaction_date' for the date column in master_card table.
                - Use 'txn_amt' instead of 'amount' for the transaction amount in master_card table.
                - When using ORDER BY with calculated columns, you must repeat the calculation in the ORDER BY clause rather than referring to the column alias.
                - Example: Instead of 'ORDER BY total_visitors DESC', use 'ORDER BY SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) DESC'"""),
                ("human", "{question}")
            ])
            
            # Create a SQL generation chain
            sql_chain = sql_prompt | self.llm | StrOutputParser()
            
            # Generate SQL query
            sql_query = await sql_chain.ainvoke({"question": message})
            
            # Clean up the SQL query (remove markdown if present)
            if "```" in sql_query:
                # Extract SQL from markdown code block if present
                import re
                sql_query = re.search(r"```(?:sql)?(.*?)```", sql_query, re.DOTALL)
                if sql_query:
                    sql_query = sql_query.group(1).strip()
            
            logger.debug(f"Generated SQL query: {sql_query}")
            return sql_query
            
        except Exception as e:
            logger.error(f"Error generating SQL query: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def generate_sql(self, message: str, schema_context: str) -> str:
        """Generate SQL query from natural language using OpenAI"""
        try:
            # Check for common patterns first
            if "weekly visitor patterns" in message.lower():
                logger.debug("Using predefined weekly pattern query")
                if "spring" in message.lower():
                    return """
                    SELECT
                        EXTRACT(WEEK FROM aoi_date) as week_number,
                        ROUND(AVG((visitors->>'swissTourist')::numeric)) as swiss_tourists,
                        ROUND(AVG((visitors->>'foreignTourist')::numeric)) as foreign_tourists,
                        ROUND(AVG((visitors->>'swissTourist')::numeric + (visitors->>'foreignTourist')::numeric)) as total_visitors
                    FROM data_lake.aoi_days_raw
                    WHERE
                        EXTRACT(YEAR FROM aoi_date) = 2023
                        AND EXTRACT(MONTH FROM aoi_date) BETWEEN 3 AND 5
                    GROUP BY week_number
                    ORDER BY week_number;
                    """

            prompt = f"""Given the following database schema and user message, generate a PostgreSQL query.

Schema Context:
{schema_context}

User Query: {message}

Requirements:
1. Use proper JSON field access with ->> for JSONB fields
2. Cast numeric values appropriately
3. Include proper GROUP BY clauses if using aggregations
4. Order results logically
5. IMPORTANT: Use 'aoi_date' instead of 'date' for the date column in aoi_days_raw table
6. IMPORTANT: Use 'txn_date' instead of 'transaction_date' for the date column in master_card table
7. IMPORTANT: Use 'txn_amt' instead of 'amount' for the transaction amount in master_card table
8. IMPORTANT: When using ORDER BY with calculated columns, you must repeat the calculation in the ORDER BY clause rather than referring to the column alias
   Example: Instead of 'ORDER BY total_visitors DESC', use 'ORDER BY SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) DESC'

Return only the SQL query, no other text."""

            response = self.llm.invoke(prompt)
            sql_query = response.strip()
            logger.debug(f"Generated SQL: {sql_query}")
            return sql_query

        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}")
            raise

    async def execute_sql(self, sql_query: str) -> Any:
        """Execute SQL query using database connection"""
        try:
            # Use the database connection from settings
            engine = create_engine(f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")
            with engine.connect() as connection:
                result = connection.execute(text(sql_query))
                return result.fetchall()
        except Exception as e:
            logger.error(f"Error executing SQL: {str(e)}")
            raise

    async def summarize_data(self, data: list, query_context: str) -> str:
        """Generate a summary of the data using the OpenAI model"""
        try:
            prompt = f"""Given the following data from a tourism database query about {query_context}, provide a clear and concise summary of the patterns and insights:

Data:
{data}

Please include:
1. Overall trends
2. Notable patterns or changes
3. Peak periods
4. Any interesting insights

Keep the summary clear and informative for a business audience."""

            response = await self.chain.ainvoke({
                "question": prompt,
                "chat_history": []
            })
            
            return response

        except Exception as e:
            logger.error(f"Error generating data summary: {str(e)}")
            raise

    def visualize_data(self, data: list, columns: list) -> dict:
        """Create visualization data for the frontend"""
        try:
            import pandas as pd
            
            # Convert data to DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # Prepare visualization data
            viz_data = {
                'type': 'line',  # Default to line chart for time series
                'data': {
                    'labels': df[columns[0]].tolist(),  # First column as labels (usually dates)
                    'datasets': []
                }
            }
            
            # Add each numeric column as a dataset
            for col in columns[1:]:
                viz_data['data']['datasets'].append({
                    'label': col,
                    'data': df[col].tolist()
                })
            
            return viz_data

        except Exception as e:
            logger.error(f"Error creating visualization: {str(e)}")
            raise

# Initialize OpenAI adapter
openai_adapter = OpenAIAdapter() 