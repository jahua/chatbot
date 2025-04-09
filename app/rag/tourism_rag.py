import os
import logging
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
from app.services.geo_insights_service import GeoInsightsService
from app.visualization.geo_insights_viz import GeoInsightsVisualizer
from app.db.database import DatabaseService
from app.agents.geo_insight_agent import GeoInsightAgent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TourismRAG:
    """RAG implementation for tourism data"""
    
    def __init__(self):
        """Initialize the RAG system"""
        try:
            # Initialize database service
            self.db_service = DatabaseService()
            
            # Initialize services with database service
            self.geo_service = GeoInsightsService(db_service=self.db_service)
            
            # Get the absolute path to the tourism regions directory
            regions_dir = os.path.join(os.getcwd(), "note/Tourism regions")
            self.visualizer = GeoInsightsVisualizer(regions_dir=regions_dir)
            
            # Initialize LLM
            try:
                self.llm = ChatOpenAI(
                    model=settings.OPENAI_MODEL,
                    temperature=0.7,
                    api_key=settings.OPENAI_API_KEY,
                    openai_api_base=settings.OPENAI_API_BASE
                )
            except Exception as e:
                logger.warning(f"Failed to initialize ChatOpenAI: {str(e)}")
                logger.warning("Falling back to simple text-based response system")
                self.llm = None
            
            # Initialize memory
            self.memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
            
            # Initialize GeoInsightAgent
            self.geo_agent = GeoInsightAgent(llm=self.llm, memory=self.memory)
            
            # Initialize chains
            self._initialize_chains()
            
            logger.info("Successfully initialized TourismRAG")
            
        except Exception as e:
            logger.error(f"Error initializing TourismRAG: {str(e)}")
            raise RuntimeError("Failed to initialize TourismRAG") from e
            
    def get_response(self, query: str) -> Dict[str, Any]:
        """Get response for a user query"""
        try:
            # Check if query is about visualization
            if any(keyword in query.lower() for keyword in ['map', 'visualize', 'show', 'plot', 'chart']):
                return {
                    'type': 'visualization',
                    'content': self.visualizer.create_region_map(),
                    'message': 'Here is a visualization of the tourism regions.'
                }
            
            # For text queries, search regions directly
            regions = self.geo_service.search_regions(query)
            
            if not regions:
                return {
                    'type': 'text',
                    'content': "I couldn't find any regions matching your query. Try a different search term or ask for a visualization.",
                    'sources': []
                }
            
            # Format the response
            response = "Here are the regions that match your query:\n\n"
            for region in regions:
                response += f"**{region.get('name', 'Unknown')}**\n"
                response += f"{region.get('description', 'No description available')}\n"
                response += f"Location: {region.get('location', 'Location unknown')}\n"
                response += f"Visitors: {region.get('visitors', 0):,}\n"
                response += f"Attractions: {', '.join(region.get('attractions', []))}\n\n"
            
            return {
                'type': 'text',
                'content': response,
                'sources': [f"Region: {region.get('name')}" for region in regions]
            }
            
        except Exception as e:
            logger.error(f"Error getting response: {str(e)}")
            return {
                'type': 'error',
                'content': f"Sorry, I encountered an error: {str(e)}"
            }
            
    def get_region_insights(self, region_id: str) -> Dict[str, Any]:
        """Get detailed insights about a specific region"""
        try:
            return self.geo_service.get_region_insights(region_id)
        except Exception as e:
            logger.error(f"Error getting region insights: {str(e)}")
            return {}
            
    def search_regions(self, query: str) -> List[Dict[str, Any]]:
        """Search for regions based on a text query"""
        try:
            return self.geo_service.search_regions(query)
        except Exception as e:
            logger.error(f"Error searching regions: {str(e)}")
            return []

    def _initialize_chains(self):
        """Initialize LangChain chains for the RAG pipeline"""
        
        # SQL generation prompt
        self.sql_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a SQL expert specializing in PostgreSQL with JSON handling and PostGIS spatial functions.
            Generate a SQL query to answer the user's question using the provided schema context.
            
            Key considerations:
            1. Use proper JSON operators (->, ->>) for accessing JSON fields
            2. Handle date ranges appropriately
            3. Consider using window functions for time series analysis
            4. Use appropriate aggregations for visitor counts
            5. Join with master_card table when spending data is needed
            6. Use PostGIS functions for spatial queries when needed
            
            The schema context includes:
            - Table descriptions and their relationships
            - JSON field structures and their meanings
            - Common query patterns for different analysis types
            - Spatial data structures and PostGIS functions
            
            Return ONLY the SQL query, nothing else."""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{question}"),
            ("human", "Schema Context: {schema_context}")
        ])
        
        # Analysis and visualization prompt
        self.analysis_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a tourism data analyst with expertise in geospatial analysis. Analyze the query results and provide insights.
            Format your response in markdown with:
            
            1. Key Findings:
               - Overall trends and patterns
               - Notable changes or anomalies
               - Peak periods or significant events
               - Geographic patterns and spatial relationships
            
            2. Data Analysis:
               - Statistical summaries
               - Comparisons between categories
               - Time series patterns
               - Spatial clustering and hotspots
            
            3. Visualization Recommendations:
               - Suggest appropriate chart types
               - Specify key metrics to visualize
               - Highlight important data points
               - Recommend map-based visualizations when relevant
            
            4. Business Insights:
               - Impact on tourism operations
               - Opportunities for improvement
               - Recommendations for action
               - Geographic opportunities and challenges
            
            Use clear, concise language and focus on actionable insights."""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{question}"),
            ("human", "Query Results: {query_results}")
        ])
        
        # Initialize the chains
        self.sql_chain = create_sql_query_chain(self.llm, self.db_service.db, self.sql_prompt)
        self.analysis_chain = self.analysis_prompt | self.llm | StrOutputParser()
        
        logger.info("Chains initialized successfully")
    
    async def process_query(self, question: str) -> Dict[str, Any]:
        """Process a user query through the RAG pipeline"""
        try:
            # Get chat history
            chat_history = self.memory.chat_memory.messages
            
            # Run the RAG chain
            response = await self.analysis_chain.ainvoke({
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