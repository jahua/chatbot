from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.sql_database import SQLDatabase
from langchain.llms.openai import OpenAI
from langchain.agents import AgentExecutor
from langchain_core.prompts import MessagesPlaceholder
from langchain.memory import ConversationBufferMemory
from typing import Dict, Any, List
import os
import json
import streamlit as st
import logging

# Configure logging
logger = logging.getLogger(__name__)

class LangChainSQLHelper:
    """Helper class to integrate LangChain SQL capabilities with the frontend"""
    
    def __init__(self):
        """Initialize the LangChain SQL helper with database connection and models"""
        # Load environment variables if not already loaded
        self._load_env_variables()
        
        # Initialize components only when needed to avoid unnecessary API calls
        self._agent_executor = None
        self._db = None
        self._memory = None
        
    def _load_env_variables(self):
        """Load necessary environment variables"""
        # DB connection details should be loaded from environment
        self.db_config = {
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "dbname": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD")
        }
        
        # OpenAI API key should be available in environment
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        
        if not self.openai_api_key:
            logger.warning("OpenAI API key not found. LangChain SQL capabilities will not work.")
        
        if not all(self.db_config.values()):
            logger.warning("Database configuration incomplete. LangChain SQL capabilities may not work.")
    
    def _initialize_agent(self):
        """Initialize the LangChain SQL agent"""
        if not self._agent_executor and self.openai_api_key:
            try:
                # Create database connection string
                db_uri = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
                
                # Initialize database connection
                self._db = SQLDatabase.from_uri(db_uri)
                
                # Initialize memory
                self._memory = ConversationBufferMemory(
                    memory_key="chat_history",
                    return_messages=True
                )
                
                # Initialize language model
                llm = OpenAI(temperature=0, openai_api_key=self.openai_api_key)
                
                # Create SQL toolkit
                toolkit = SQLDatabaseToolkit(db=self._db, llm=llm)
                
                # Create agent with conversation history
                self._agent_executor = create_sql_agent(
                    llm=llm,
                    toolkit=toolkit,
                    verbose=True,
                    agent_kwargs={
                        "extra_prompt_messages": [MessagesPlaceholder(variable_name="chat_history")],
                    },
                    memory=self._memory
                )
                
                logger.info("LangChain SQL agent initialized successfully")
                return True
            
            except Exception as e:
                logger.error(f"Error initializing LangChain SQL agent: {str(e)}")
                return False
        
        return self._agent_executor is not None
    
    def execute_sql_query(self, query: str) -> Dict[str, Any]:
        """Execute a SQL query using LangChain capabilities"""
        # Initialize agent if not already done
        if not self._initialize_agent():
            return {
                "success": False,
                "error": "Could not initialize LangChain SQL agent. Check configuration and API keys.",
                "content": "Sorry, I couldn't process your SQL query at this time."
            }
        
        try:
            # Execute the query through the LangChain agent
            result = self._agent_executor.invoke({"input": query})
            
            return {
                "success": True,
                "result": result,
                "content": result["output"] if "output" in result else "Query executed successfully."
            }
        
        except Exception as e:
            logger.error(f"Error executing SQL query with LangChain: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "content": f"Sorry, I encountered an error while processing your query: {str(e)}"
            }
    
    def process_natural_language_query(self, query: str) -> Dict[str, Any]:
        """Process a natural language query using LangChain SQL capabilities"""
        # Initialize agent if not already done
        if not self._initialize_agent():
            return {
                "success": False,
                "error": "Could not initialize LangChain SQL agent. Check configuration and API keys.",
                "content": "Sorry, I couldn't process your question at this time."
            }
        
        try:
            # Process the natural language query through the LangChain agent
            result = self._agent_executor.invoke({"input": query})
            
            return {
                "success": True,
                "result": result,
                "content": result["output"] if "output" in result else "Query executed successfully."
            }
        
        except Exception as e:
            logger.error(f"Error processing natural language query with LangChain: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "content": f"Sorry, I encountered an error while processing your question: {str(e)}"
            }

# Create a singleton instance of the helper
langchain_helper = LangChainSQLHelper()

def get_langchain_helper():
    """Get the singleton instance of the LangChain SQL helper"""
    return langchain_helper 