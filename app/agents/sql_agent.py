from typing import Dict, List, Any, Optional
from langchain.tools import Tool
from langchain_core.messages import HumanMessage, AIMessage
from .base_agent import BaseAgent
from app.db.database import get_db
from app.llm.openai_adapter import openai_adapter
from langchain.agents import AgentExecutor, create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.sql_database import SQLDatabase
from langchain.llms.base import BaseLLM
from langchain.agents.agent_types import AgentType
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.pydantic_v1 import BaseModel, Field
from app.core.langsmith_config import get_traceable_decorator
from app.core.config import settings
from sqlalchemy.orm import Session

class SQLQueryInput(BaseModel):
    query: str = Field(description="The SQL query to execute")
    schema_context: str = Field(description="The database schema context")

class SQLQueryOutput(BaseModel):
    success: bool = Field(description="Whether the query was successful")
    sql_query: Optional[str] = Field(description="The generated SQL query")
    results: Optional[List[Dict[str, Any]]] = Field(description="The query results")
    analysis: Optional[str] = Field(description="Analysis of the results")
    error: Optional[str] = Field(description="Error message if query failed")

class SQLAgent(BaseAgent):
    def __init__(self, model_name: str = "openai"):
        # Select the appropriate LLM based on model_name
        self.model_name = model_name
        self.llm = self._get_llm()
        self.db = self._get_db()
        self.toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
        self.agent_executor = self._create_agent()
        self.traceable = get_traceable_decorator()
        
        # Define SQL-specific tools
        tools = [
            Tool(
                name="generate_sql",
                func=self._generate_sql,
                description="Generate SQL query from natural language"
            ),
            Tool(
                name="execute_sql",
                func=self._execute_sql,
                description="Execute SQL query and return results"
            ),
            Tool(
                name="analyze_results",
                func=self._analyze_results,
                description="Analyze SQL query results and provide insights"
            )
        ]
        
        super().__init__(llm=self.llm, tools=tools)
    
    def _get_llm(self) -> BaseLLM:
        """Get the appropriate LLM based on model name"""
        if self.model_name == "openai":
            from app.llm.openai_adapter import OpenAIAdapter
            return OpenAIAdapter()
        elif self.model_name == "gemini":
            from app.llm.gemini_adapter import GeminiAdapter
            return GeminiAdapter()
        elif self.model_name == "ollama":
            from app.llm.ollama_adapter import OllamaAdapter
            return OllamaAdapter()
        elif self.model_name == "vanna":
            from app.llm.vanna_adapter import VannaAdapter
            return VannaAdapter()
        else:
            raise ValueError(f"Unsupported model: {self.model_name}")

    def _get_db(self) -> SQLDatabase:
        """Get database connection"""
        return SQLDatabase.from_uri(self._get_db_url())

    def _get_db_url(self) -> str:
        """Get database URL from settings"""
        return f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"

    def _create_agent(self) -> AgentExecutor:
        """Create the SQL agent"""
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a SQL expert. Your task is to:
            1. Understand the user's question
            2. Generate appropriate SQL queries
            3. Execute the queries
            4. Analyze the results
            5. Provide insights

            Use the provided database schema to generate accurate queries.
            Always explain your reasoning and provide context for the results."""),
            ("user", "{input}")
        ])

        agent = create_sql_agent(
            llm=self.llm,
            toolkit=self.toolkit,
            agent_type=AgentType.OPENAI_FUNCTIONS,
            verbose=True,
            prompt=prompt
        )

        return AgentExecutor.from_agent_and_tools(
            agent=agent,
            tools=self.toolkit.get_tools(),
            verbose=True
        )

    async def _generate_sql(self, query: str, schema_context: str) -> str:
        """Generate SQL query from natural language"""
        return await self.llm.generate_sql(query, schema_context)
    
    async def _execute_sql(self, sql_query: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        try:
            result = self.db.execute(sql_query).fetchall()
            return [dict(row) for row in result]
        except Exception as e:
            raise Exception(f"Error executing SQL query: {str(e)}")
    
    async def _analyze_results(self, query: str, sql_query: str, results: List[Dict[str, Any]]) -> str:
        """Analyze SQL query results and provide insights"""
        return await self.llm.generate_response(query, sql_query, str(results))
    
    @traceable
    async def process_query(self, query: str, schema_context: str) -> Dict[str, Any]:
        """Process a user query and return results"""
        try:
            # Create input for the agent
            input_data = SQLQueryInput(
                query=query,
                schema_context=schema_context
            )

            # Execute the agent
            result = await self.agent_executor.arun(
                input_data.query,
                schema_context=input_data.schema_context
            )

            # Parse and format the results
            return SQLQueryOutput(
                success=True,
                sql_query=result.get("sql_query"),
                results=result.get("results"),
                analysis=result.get("analysis")
            ).dict()

        except Exception as e:
            return SQLQueryOutput(
                success=False,
                error=str(e)
            ).dict()

    def __del__(self):
        self.db.close() 