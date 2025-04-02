from typing import Dict, List, Any, Optional, Tuple
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.runnables import RunnablePassthrough
from langgraph.graph import Graph, StateGraph
from langgraph.prebuilt import ToolExecutor
from langchain.tools import Tool
from langchain.memory import ConversationBufferMemory
from langchain.chains import LLMChain
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser

class BaseAgent:
    def __init__(self, llm, tools: List[Tool], memory: Optional[ConversationBufferMemory] = None):
        self.llm = llm
        self.tools = tools
        self.memory = memory or ConversationBufferMemory(
            return_messages=True,
            memory_key="chat_history"
        )
        
        # Initialize the prompt template
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a helpful AI assistant that helps users query and analyze tourism data.
            You have access to various tools to help with SQL query generation, data analysis, and visualization.
            
            Current tools available:
            {tools}
            
            Use these tools to help users get insights from the tourism data.
            Always explain your reasoning and provide clear, concise responses."""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad")
        ])
        
        # Initialize the agent chain
        self.chain = self._create_agent_chain()
        
    def _create_agent_chain(self) -> LLMChain:
        """Create the agent chain with tools and memory"""
        return LLMChain(
            llm=self.llm,
            prompt=self.prompt,
            memory=self.memory,
            tools=self.tools
        )
    
    async def process_message(self, message: str) -> Dict[str, Any]:
        """Process a user message and return the response"""
        try:
            # Format the tools for the prompt
            tools_desc = "\n".join([f"- {tool.name}: {tool.description}" for tool in self.tools])
            
            # Process the message
            response = await self.chain.arun(
                input=message,
                tools=tools_desc
            )
            
            return {
                "success": True,
                "response": response,
                "chat_history": self.memory.chat_memory.messages
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "chat_history": self.memory.chat_memory.messages
            } 