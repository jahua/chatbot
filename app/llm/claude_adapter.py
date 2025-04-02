import os
import logging
import traceback
import asyncio
from typing import Dict, Any

from dotenv import load_dotenv
from sqlalchemy import create_engine
from langchain_anthropic import ChatAnthropic
from langchain_community.utilities import SQLDatabase
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferMemory
from langchain.chains import create_sql_query_chain, LLMChain
from langchain_core.runnables import RunnablePassthrough
from app.core.config import settings

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ClaudeAdapter:
    def __init__(self):
        """Initialize ClaudeAdapter with LangChain components"""
        try:
            # Initialize Claude model
            self.llm = ChatAnthropic(
                model=settings.OPENAI_MODEL,
                anthropic_api_key=settings.OPENAI_API_KEY,
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
                - data_lake.aoi_days_raw: Contains daily visitor data
                - data_lake.master_card: Contains transaction data
                
                Generate clear and efficient SQL queries to answer user questions about tourism patterns."""),
                MessagesPlaceholder(variable_name="chat_history"),
                ("human", "{question}")
            ])
            
            # Create the chain
            self.chain = (
                RunnablePassthrough.assign(
                    chat_history=self.memory.load_memory_variables({})["chat_history"]
                )
                | self.prompt
                | self.llm
                | StrOutputParser()
            )
            
            logger.info("ClaudeAdapter initialized successfully using LCEL")
            
        except Exception as e:
            logger.error(f"Error initializing ClaudeAdapter: {str(e)}")
            raise
    
    async def generate_response(self, question: str) -> str:
        """Generate a response using the Claude model"""
        try:
            # Generate response
            response = await self.chain.ainvoke({"question": question})
            
            # Update memory
            self.memory.save_context({"question": question}, {"output": response})
            
            return response
            
        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            raise

# Initialize Claude adapter
claude_adapter = ClaudeAdapter() 