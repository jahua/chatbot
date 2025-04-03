from typing import List, Dict, Any
from langchain.agents import initialize_agent, AgentType
from langchain.tools import Tool
from langchain.memory import ConversationBufferMemory
from app.llm.openai_adapter import OpenAIAdapter
from app.db.database import DatabaseService
from app.tools.visitor_stats_tool import create_visitor_stats_tool
import logging

logger = logging.getLogger(__name__)

class TourismAgent:
    def __init__(self, llm: OpenAIAdapter, db: DatabaseService):
        self.llm = llm
        self.db = db
        self.tools = self._create_tools()
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        self.agent = self._create_agent()

    def _create_tools(self) -> List[Tool]:
        """Create the set of tools available to the agent"""
        tools = [
            create_visitor_stats_tool(self.db),
            # Add more tools here as needed
        ]
        return tools

    def _create_agent(self):
        """Initialize the LangChain agent"""
        return initialize_agent(
            tools=self.tools,
            llm=self.llm,
            agent=AgentType.OPENAI_FUNCTIONS,
            memory=self.memory,
            verbose=True
        )

    async def process_message(self, message: str) -> Dict[str, Any]:
        """Process a user message using the agent"""
        try:
            # Run the agent
            response = await self.agent.arun(message)
            
            logger.info("Agent processed message successfully")
            
            return {
                "response": response,
                "error": None
            }
            
        except Exception as e:
            logger.error(f"Error processing message with agent: {str(e)}")
            return {
                "response": None,
                "error": str(e)
            } 