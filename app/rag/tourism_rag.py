from typing import List, Dict, Any, Optional
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.output_parsers import StrOutputParser, JsonOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableLambda
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.utilities import SQLDatabase
from langchain.chains import create_sql_query_chain
from langchain.memory import ConversationBufferMemory
from app.core.config import settings
from app.db.vector_store import VectorStore
import logging

logger = logging.getLogger(__name__)

class TourismRAG:
    def __init__(self):
        # Initialize vector store
        self.vector_store = VectorStore()
        
        # Initialize embeddings
        self.embeddings = OpenAIEmbeddings(
            openai_api_key=settings.OPENAI_API_KEY,
            openai_api_base=settings.OPENAI_API_BASE
        )
        
        # Initialize database connection
        self.db = SQLDatabase.from_uri(
            f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        )
        
        # Initialize memory
        self.memory = ConversationBufferMemory(
            return_messages=True,
            memory_key="chat_history"
        )
        
        # Initialize chains
        self._initialize_chains()
        
        logger.info("TourismRAG initialized successfully")
    
    def _initialize_chains(self):
        """Initialize LangChain chains for the RAG pipeline"""
        
        # Schema retrieval prompt
        schema_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a database schema expert. Given the user's question, 
            identify the relevant tables and columns needed to answer it.
            Return a JSON object with:
            - tables: List of relevant table names
            - columns: List of relevant column names
            - relationships: List of relevant relationships between tables"""),
            ("human", "{question}")
        ])
        
        # SQL generation prompt
        sql_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a SQL expert. Generate a PostgreSQL query to answer the user's question.
            Use the provided schema context and previous conversation history.
            Return ONLY the SQL query, nothing else."""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{question}"),
            ("human", "Schema Context: {schema_context}")
        ])
        
        # Analysis prompt
        analysis_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a tourism data analyst. Analyze the query results and provide insights.
            Format your response in markdown with:
            1. Key findings and trends
            2. Notable patterns or anomalies
            3. Relevant comparisons
            4. Important metrics"""),
            ("human", "Question: {question}"),
            ("human", "SQL Query: {sql_query}"),
            ("human", "Results: {results}")
        ])
        
        # Create the RAG chain
        self.rag_chain = (
            RunnablePassthrough.assign(
                schema_context=self._get_schema_context,
                sql_query=self._generate_sql,
                results=self._execute_sql
            )
            | analysis_prompt
            | StrOutputParser()
        )
    
    def _get_schema_context(self, question: str) -> str:
        """Retrieve relevant schema context for the question"""
        try:
            schema_info = self.vector_store.get_schema_context(question)
            return schema_info["context"]
        except Exception as e:
            logger.error(f"Error retrieving schema context: {str(e)}")
            return ""
    
    def _generate_sql(self, question: str, schema_context: str) -> str:
        """Generate SQL query using LangChain"""
        try:
            sql_chain = create_sql_query_chain(
                llm=self.llm,
                db=self.db,
                prompt=sql_prompt
            )
            return sql_chain.invoke({
                "question": question,
                "schema_context": schema_context,
                "chat_history": self.memory.chat_memory.messages
            })
        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}")
            raise
    
    def _execute_sql(self, sql_query: str) -> str:
        """Execute SQL query and return results"""
        try:
            return self.db.run(sql_query)
        except Exception as e:
            logger.error(f"Error executing SQL: {str(e)}")
            raise
    
    async def process_query(self, question: str) -> Dict[str, Any]:
        """Process a user query through the RAG pipeline"""
        try:
            # Get chat history
            chat_history = self.memory.chat_memory.messages
            
            # Run the RAG chain
            response = await self.rag_chain.ainvoke({
                "question": question,
                "chat_history": chat_history
            })
            
            # Update memory
            self.memory.chat_memory.add_user_message(question)
            self.memory.chat_memory.add_ai_message(response)
            
            return {
                "response": response,
                "chat_history": chat_history
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            raise 