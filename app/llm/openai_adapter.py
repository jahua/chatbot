from typing import Dict, Any, Optional, List
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
from openai import AsyncOpenAI
import json

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class OpenAIAdapter:
    def __init__(self):
        """Initialize OpenAI adapter with API configuration"""
        self.client = AsyncOpenAI(
            api_key=settings.OPENAI_API_KEY,
            base_url=settings.OPENAI_API_BASE
        )
        self.model_name = settings.OPENAI_MODEL
        self.schema_info = """Tables:
- data_lake.aoi_days_raw: Contains daily visitor data
  * aoi_date (date): Date of visitor data
  * visitors (JSONB): Contains 'swissTourist' and 'foreignTourist' counts

- data_lake.master_card: Contains transaction data
  * txn_date (date): Date of transaction
  * industry (text): Industry sector
  * segment (text): Market segment
  * txn_amt (numeric): Transaction amount
  * txn_cnt (numeric): Transaction count
  * acct_cnt (numeric): Account count
  * geo_type (text): Geographic type
  * geo_name (text): Geographic name
  * central_latitude (numeric): Location latitude
  * central_longitude (numeric): Location longitude"""
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
    
    async def generate_response(self, question: str, data: str, chat_history: List[Dict[str, Any]] = None) -> str:
        """Generate natural language response from data"""
        try:
            # Format chat history for context
            history_context = ""
            if chat_history:
                history_context = "\nPrevious conversation:\n"
                for msg in chat_history[-3:]:  # Use last 3 messages for context
                    if msg["role"] == "user":
                        history_context += f"User: {msg['content']}\n"
                    else:
                        history_context += f"Assistant: {msg['content']}\n"

            # Create the prompt with data and chat history
            prompt = f"""Given the following data:
{data}

And the user's question:
{question}

{history_context}

Provide a detailed analysis of the data that answers the question. Your response should:
1. Start with a clear overview of the key findings
2. Include specific numbers and trends from the data
3. Organize information with appropriate headings
4. Provide insights and recommendations when relevant
5. Use clear and professional language
6. Maintain consistency with previous responses in the conversation

Format your response using Markdown for better readability."""

            # Get completion from OpenAI using the chain
            response = await self.chain.ainvoke({
                "question": prompt,
                "chat_history": chat_history or []
            })
            return response

        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def generate_sql_query(self, question: str, chat_history: List[Dict[str, Any]] = None) -> str:
        """Generate SQL query from natural language question"""
        try:
            # Format chat history for context
            history_context = ""
            if chat_history:
                history_context = "\nPrevious conversation:\n"
                for msg in chat_history[-3:]:  # Use last 3 messages for context
                    if msg["role"] == "user":
                        history_context += f"User: {msg['content']}\n"
                    else:
                        history_context += f"Assistant: {msg['content']}\n"

            # Create the prompt with schema info and chat history
            prompt = f"""Given the following database schema:
{self.schema_info}

And the following question:
{question}

{history_context}

Generate a SQL query to answer this question. The query should:
1. Be compatible with PostgreSQL
2. Use the correct table and column names from the schema
3. Include appropriate JOINs if needed
4. Use aggregations (COUNT, SUM, AVG, etc.) when relevant
5. Include WHERE clauses to filter data appropriately
6. Order results in a meaningful way
7. Limit results to a reasonable number if returning many rows

Return ONLY the SQL query, without any explanation or comments."""

            # Get completion from OpenAI using the chain
            response = await self.chain.ainvoke({
                "question": prompt,
                "chat_history": chat_history or []
            })
            sql_query = response.strip()
            return sql_query

        except Exception as e:
            logger.error(f"Error generating SQL query: {str(e)}")
            logger.error(traceback.format_exc())
            return None

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

    async def generate_visualization(self, data: List[Dict[str, Any]], question: str) -> Dict[str, Any]:
        """Generate a visualization based on the data and question"""
        try:
            # Format chat history for context
            chat_history = self._format_chat_history(chat_history)
            
            # Create prompt for visualization
            prompt = f"""Based on the following data and question, create a visualization:

Question: {question}

Data: {json.dumps(data, indent=2)}

Please create a visualization that best represents this data. Consider:
1. The type of data (time series, categorical, numerical)
2. The relationships between variables
3. The key insights to highlight

Return the visualization as a JSON object with the following structure:
{{
    "type": "line|bar|area|scatter|pie",
    "data": {{
        "x": [...],
        "y": [...],
        "labels": [...],
        "title": "...",
        "xaxis": "...",
        "yaxis": "..."
    }},
    "layout": {{
        "title": "...",
        "xaxis": {{"title": "..."}},
        "yaxis": {{"title": "..."}}
    }}
}}"""

            # Generate visualization using the chain
            response = await self.chain.ainvoke({"question": prompt, "chat_history": chat_history})
            
            # Parse the response
            try:
                visualization = json.loads(response)
                return visualization
            except json.JSONDecodeError:
                logger.error("Failed to parse visualization JSON")
                return None
                
        except Exception as e:
            logger.error(f"Error generating visualization: {str(e)}")
            logger.error(traceback.format_exc())
            return None

# Initialize OpenAI adapter
openai_adapter = OpenAIAdapter() 