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
from langchain_openai import ChatOpenAI
from app.core.config import settings
from app.db.schema_manager import schema_manager
import logging

logger = logging.getLogger(__name__)

class TourismRAG:
    def __init__(self):
        # Initialize embeddings
        self.embeddings = OpenAIEmbeddings(
            openai_api_key=settings.OPENAI_API_KEY,
            openai_api_base=settings.OPENAI_API_BASE
        )
        
        # Initialize LLM
        self.llm = ChatOpenAI(
            model_name=settings.OPENAI_MODEL,
            openai_api_key=settings.OPENAI_API_KEY,
            openai_api_base=settings.OPENAI_API_BASE
        )
        
        # Initialize database connection
        self.db = SQLDatabase.from_uri(
            f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
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
        
        # SQL generation prompt
        sql_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a SQL expert specializing in PostgreSQL with JSON handling.
            Generate a SQL query to answer the user's question using the provided schema context.
            
            Key considerations:
            1. Use proper JSON operators (->, ->>) for accessing JSON fields
            2. Handle date ranges appropriately
            3. Consider using window functions for time series analysis
            4. Use appropriate aggregations for visitor counts
            5. Join with master_card table when spending data is needed
            
            The schema context includes:
            - Table descriptions and their relationships
            - JSON field structures and their meanings
            - Common query patterns for different analysis types
            
            Return ONLY the SQL query, nothing else."""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{question}"),
            ("human", "Schema Context: {schema_context}")
        ])
        
        # Analysis and visualization prompt
        analysis_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a tourism data analyst. Analyze the query results and provide insights.
            Format your response in markdown with:
            
            1. Key Findings:
               - Overall trends and patterns
               - Notable changes or anomalies
               - Peak periods or significant events
            
            2. Data Analysis:
               - Statistical summaries
               - Comparisons between categories
               - Time series patterns
            
            3. Visualization Recommendations:
               - Suggest appropriate chart types
               - Specify key metrics to visualize
               - Highlight important data points
            
            4. Business Insights:
               - Impact on tourism operations
               - Opportunities for improvement
               - Recommendations for action
            
            Use clear, concise language and focus on actionable insights."""),
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
            # Get schema context from schema manager
            context = schema_manager.get_relevant_context(question)
            
            # Format the context for SQL generation
            formatted_context = []
            
            # Add table descriptions and columns
            for table_name, table_info in context["tables"].items():
                formatted_context.append(f"\nTable: {table_name}")
                if "description" in table_info:
                    formatted_context.append(f"Description: {table_info['description']}")
                if "columns" in table_info:
                    formatted_context.append("Columns:")
                    formatted_context.extend([f"- {col}" for col in table_info["columns"]])
            
            # Add JSON field information
            if context["json_fields"]:
                formatted_context.append("\nJSON Fields:")
                for field_key, field_info in context["json_fields"].items():
                    formatted_context.append(f"\n{field_key}:")
                    formatted_context.extend([f"- {info}" for info in field_info])
            
            # Add query patterns
            if context["query_patterns"]:
                formatted_context.append("\nRelevant Query Patterns:")
                formatted_context.extend([f"- {pattern}" for pattern in context["query_patterns"]])
            
            return "\n".join(formatted_context)
            
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
            
            # Generate the query
            query = sql_chain.invoke({
                "question": question,
                "schema_context": schema_context,
                "chat_history": self.memory.chat_memory.messages
            })
            
            # Validate and optimize the query
            optimized_query = self._optimize_query(query)
            
            return optimized_query
            
        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}")
            raise
    
    def _optimize_query(self, query: str) -> str:
        """Optimize the generated SQL query"""
        try:
            # Add common optimizations
            optimized = query
            
            # Ensure proper JSON field access and type casting
            json_fields = {
                "visitors": ["swissLocal", "swissTourist", "foreignWorker", "swissCommuter", "foreignTourist"],
                "demographics": ["maleProportion", "ageDistribution"],
                "top_foreign_countries": ["name", "visitors"]
            }
            
            for field, subfields in json_fields.items():
                for subfield in subfields:
                    old_pattern = f"{field}->'{subfield}'"
                    new_pattern = f"({field}->>''{subfield}'')::numeric"
                    optimized = optimized.replace(old_pattern, new_pattern)
            
            # Add appropriate indexes hint
            if "aoi_date" in optimized and "aoi_id" in optimized:
                optimized = optimized.replace("FROM data_lake.aoi_days_raw", 
                                           "FROM data_lake.aoi_days_raw /*+ INDEX(ix_aoi_days_date_id) */")
            
            # Add LIMIT if missing for large result sets
            if "LIMIT" not in optimized.upper() and "COUNT" not in optimized.upper():
                optimized += "\nLIMIT 1000"
            
            return optimized
            
        except Exception as e:
            logger.error(f"Error optimizing query: {str(e)}")
            return query
    
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