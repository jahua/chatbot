import os
import logging
import traceback
import asyncio
from typing import Dict, Any

from dotenv import load_dotenv
from sqlalchemy import create_engine
from langchain_anthropic import ChatAnthropic  # Use ChatAnthropic for chat models
from langchain_community.utilities import SQLDatabase
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.chains import create_sql_query_chain, LLMChain # Import create_sql_query_chain
# Removed SQLDatabaseChain, create_sql_agent, SQLDatabaseToolkit, AgentType, RunEvalConfig, LangChainTracer, CallbackManager

from langsmith import Client
from app.core.config import settings

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configure LangSmith (keeping for tracing if needed)
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_ENDPOINT"] = os.getenv("LANGCHAIN_ENDPOINT", "https://api.smith.langchain.com")
os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGCHAIN_API_KEY", "your_langchain_api_key_here")
os.environ["LANGCHAIN_PROJECT"] = os.getenv("LANGCHAIN_PROJECT", "tourism-chatbot")

# Initialize LangSmith client
langsmith_client = Client()

class ClaudeAdapter:
    def __init__(self, db_url):
        self.db = SQLDatabase.from_uri(db_url)
        # Use ChatAnthropic instead of the base Anthropic LLM
        self.llm = ChatAnthropic(
            model="claude-3-sonnet-20240229",
            anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
            temperature=0.1
        )
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )

        # Initialize SQL query generation chain using LCEL
        self.sql_query_chain = create_sql_query_chain(self.llm, self.db)

        # Define the chain part for executing the query and parsing output
        # self.sql_execute_chain = self.db.run | StrOutputParser() # Keep execute_sql method for now

        # Commented out old SQL Agent initialization
        # self.sql_agent = create_sql_agent(
        #     llm=self.llm,
        #     toolkit=SQLDatabaseToolkit(db=self.db, llm=self.llm),
        #     agent_type=AgentType.OPENAI_FUNCTIONS,
        #     verbose=True,
        #     memory=self.memory
        # )

        # Removed old SQLDatabaseChain initialization
        # self.sql_chain = SQLDatabaseChain.from_llm(...)

        # Initialize response chain (keeping old LLMChain for now)
        self.response_chain = LLMChain(
            llm=self.llm,
            prompt=ChatPromptTemplate.from_messages([
                ("system", "You are a helpful tourism data analyst. Provide clear, concise explanations of the data."),
                ("human", "{query}") # Note: This expects 'query', not 'user_query' based on definition
            ])
        )

        # Removed CallbackManager setup
        # self.callback_manager = CallbackManager([LangChainTracer()])

        logger.info("ClaudeAdapter initialized successfully using LCEL")

    async def generate_sql(self, message: str, schema_summary: str) -> str:
        """Generate SQL query using LangChain LCEL"""
        logger.debug(f"Generating SQL for message: {message}")

        try:
            # Check for common patterns first (keep existing logic)
            if "weekly visitor patterns" in message.lower():
                logger.debug("Using predefined weekly pattern query")
                # ... (keep existing predefined query logic) ...
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
                else:
                    return """
                    SELECT
                        EXTRACT(WEEK FROM aoi_date) as week_number,
                        ROUND(AVG((visitors->>'swissTourist')::numeric)) as swiss_tourists,
                        ROUND(AVG((visitors->>'foreignTourist')::numeric)) as foreign_tourists,
                        ROUND(AVG((visitors->>'swissTourist')::numeric + (visitors->>'foreignTourist')::numeric)) as total_visitors
                    FROM data_lake.aoi_days_raw
                    WHERE EXTRACT(YEAR FROM aoi_date) = 2023
                    GROUP BY week_number
                    ORDER BY week_number;
                    """

            # For other queries, use the new LCEL SQL query chain
            logger.debug("Using LangChain LCEL SQL generation")
            # Use invoke with 'question' key for create_sql_query_chain
            sql_query_result = await asyncio.to_thread(
                self.sql_query_chain.invoke,
                {"question": message} # Pass user message as 'question'
            )

            logger.debug(f"Generated SQL: {sql_query_result}")
            return sql_query_result.strip()

        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def execute_sql(self, sql_query: str) -> Any: # Return type might not always be Dict
        """Execute SQL query using SQLDatabase utility"""
        logger.debug(f"Executing SQL query: {sql_query}")

        try:
            # Execute query with timeout using db.run directly
            result = await asyncio.wait_for(
                asyncio.to_thread(self.db.run, sql_query),
                timeout=10.0  # Keep timeout logic
            )

            logger.debug(f"Query executed successfully. Result: {result}")
            # db.run typically returns a string representation of the results
            return result # Return the raw result (likely a string)

        except asyncio.TimeoutError:
            logger.error("SQL query execution timed out")
            raise Exception("Database query timed out. Please try a simpler query.")
        except Exception as e:
            logger.error(f"Error executing SQL: {str(e)}")
            logger.error(traceback.format_exc())
            # Propagate specific database errors if possible/needed
            raise

    async def generate_response(
        self,
        sql_query: str,
        query_result: Any, # Adjusted type hint
        user_query: str
    ) -> str:
        """Generate response using LangChain LLMChain"""
        logger.debug("Generating response for query results")

        try:
            # Using the existing response_chain LLMChain
            response_dict = await asyncio.to_thread( # invoke returns a dict
                self.response_chain.invoke,
                {
                    "query": user_query, # Matches the prompt variable
                    # Consider adding context about the query and results for better response
                    # "sql": sql_query,
                    # "results": str(query_result)
                }
            )
            response_text = response_dict.get("text", "") # Extract text response

            logger.debug(f"Generated response: {response_text}")
            return response_text.strip()

        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            logger.error(traceback.format_exc())
            raise

# Initialize Claude adapter
claude_adapter = ClaudeAdapter(f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}") 