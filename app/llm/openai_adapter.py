from typing import Dict, Any, Optional, List
import logging
import traceback
import asyncio
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser, JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferMemory
from langchain_core.runnables import RunnablePassthrough
from app.core.config import settings
from sqlalchemy import create_engine, text
from openai import AsyncOpenAI
from app.db.schema_manager import schema_manager
import json
import re

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class OpenAIAdapter:
    """Adapter for OpenAI API interactions"""
    
    def __init__(self, api_key: Optional[str] = None, api_base: Optional[str] = None):
        """Initialize OpenAI adapter with API configuration"""
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.api_base = api_base or os.getenv("OPENAI_API_BASE")
        
        if not self.api_key:
            raise ValueError("OpenAI API key is required")
            
        # Initialize OpenAI client
        self.client = AsyncOpenAI(
            api_key=self.api_key,
            base_url=self.api_base
        )
        
        # Initialize LangChain chat model
        self.chat_model = ChatOpenAI(
            api_key=self.api_key,
            model_name=settings.OPENAI_MODEL,
            openai_api_base=self.api_base
        )
        
        # Initialize memory and prompts
        self.memory = ConversationBufferMemory(
            return_messages=True,
            memory_key="chat_history"
        )
        self._initialize_prompts()
            
    def _initialize_prompts(self):
        """Initialize prompt templates with intelligent schema context"""
        # SQL generation prompt with dynamic schema context
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a helpful assistant that generates SQL queries for tourism data analysis.
            You have access to a dynamic schema context that will be provided for each query.
            
            The data comes from two main sources:
            1. Swisscom Tourism API - Provides visitor data based on SIM card movements
            2. Mastercard Geo Insights - Provides spending patterns in 1.2x1.2km tiles
            
            Key guidelines for query generation:
            1. Use proper JSON operators (->, ->>) for JSONB fields
            2. Cast numeric values appropriately using ::numeric
            3. Use proper date handling functions
            4. Consider table relationships and join conditions
            5. Use appropriate indexes when available
            6. Handle aggregations and grouping correctly
            
            Special considerations:
            1. Visitor categories are based on >30 min dwell time
            2. Overnight visitors spend 4+ hours between 00:00-05:00
            3. Previous locations require 20+ min stay before arrival
            4. Mastercard data is indexed to 2018 baseline
            5. Geographic analysis may require spatial functions
            
            IMPORTANT NAMING CONVENTIONS:
            - Use 'aoi_date' for dates in aoi_days_raw table
            - Use 'txn_date' for dates in master_card table
            - Use 'txn_amt' for transaction amounts
            
            When using ORDER BY with calculated columns, repeat the calculation instead of using column alias."""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{question}"),
            ("human", "Schema Context: {schema_context}")
        ])
        
        # Analysis prompt for query results
        self.analysis_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a tourism data analyst. Analyze the query results using the schema context.
            Consider:
            1. Visitor Patterns:
               - Swiss vs foreign visitors
               - Local vs tourist proportions
               - Commuter patterns
               - Overnight stays (4+ hours, 00:00-05:00)
            
            2. Temporal Patterns:
               - Daily/weekly trends
               - Seasonal variations
               - Peak periods
               - Dwell time distributions
            
            3. Geographic Insights:
               - Origin analysis (municipalities, cantons, countries)
               - Previous location patterns
               - Regional differences
               - Spatial correlations
            
            4. Demographic Analysis:
               - Age group distributions
               - Gender proportions
               - Visitor segment characteristics
            
            5. Economic Impact:
               - Spending patterns by industry
               - Domestic vs international transactions
               - Average transaction metrics
               - Geographic spending distribution
            
            Format your response in markdown with clear sections and insights."""),
            ("human", "Question: {question}"),
            ("human", "Schema Context: {schema_context}"),
            ("human", "Results: {results}")
        ])

    async def generate_sql_query(self, question: str, chat_history: List[Dict[str, Any]] = None) -> str:
        """Generate SQL query with intelligent schema context"""
        try:
            # Get relevant schema context for the question
            schema_context = schema_manager.get_relevant_context(question)
            
            # Format schema context for the prompt
            formatted_context = self._format_schema_context(schema_context)
            
            # Generate SQL query
            sql_query = await self.chain.ainvoke({
                "question": question,
                "schema_context": formatted_context,
                "chat_history": chat_history or []
            })
            
            # Optimize the query
            optimized_query = self._optimize_query(sql_query, schema_context)
            
            return optimized_query

        except Exception as e:
            logger.error(f"Error generating SQL query: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def _format_schema_context(self, context: Dict[str, Any]) -> str:
        """Format schema context for prompt consumption"""
        formatted = []
        
        # Add domain knowledge
        if context["domain_knowledge"]:
            formatted.append("\nDomain Knowledge:")
            for category, info_list in context["domain_knowledge"].items():
                formatted.append(f"\n{category.replace('_', ' ').title()}:")
                formatted.extend([f"- {info}" for info in info_list])
        
        # Add table information
        for table_name, table_info in context["tables"].items():
            formatted.append(f"\nTable: {table_name}")
            if "description" in table_info:
                formatted.append(f"Description: {table_info['description']}")
            if "columns" in table_info:
                formatted.append("Columns:")
                formatted.extend([f"- {col}" for col in table_info["columns"]])
        
        # Add JSON field details
        if context["json_fields"]:
            formatted.append("\nJSON Fields:")
            for field_key, field_info in context["json_fields"].items():
                formatted.append(f"\n{field_key}:")
                formatted.extend([f"- {info}" for info in field_info])
        
        # Add relevant query patterns
        if context["query_patterns"]:
            formatted.append("\nRelevant Query Patterns:")
            formatted.extend([f"- {pattern}" for pattern in context["query_patterns"]])
        
        return "\n".join(formatted)
    
    def _optimize_query(self, query: str, schema_context: Dict[str, Any]) -> str:
        """Optimize SQL query using schema context"""
        try:
            optimized = query
            
            # Get JSON field information from schema context
            json_fields = {}
            for table_info in schema_context["tables"].values():
                for col_name, col_info in table_info.get("columns", {}).items():
                    if col_info.get("type") == "jsonb" and isinstance(col_info.get("json_structure"), dict):
                        json_fields[col_name] = list(col_info["json_structure"].keys())
            
            # Ensure proper JSON field access and type casting
            for field, subfields in json_fields.items():
                for subfield in subfields:
                    old_pattern = f"{field}->'{subfield}'"
                    new_pattern = f"({field}->>''{subfield}'')::numeric"
                    optimized = optimized.replace(old_pattern, new_pattern)
            
            # Add index hints based on schema context
            if "aoi_date" in optimized and "aoi_id" in optimized:
                optimized = optimized.replace(
                    "FROM data_lake.aoi_days_raw",
                    "FROM data_lake.aoi_days_raw /*+ INDEX(ix_aoi_days_date_id) */"
                )
            
            # Add LIMIT for large result sets
            if "LIMIT" not in optimized.upper() and "COUNT" not in optimized.upper():
                optimized += "\nLIMIT 1000"
            
            return optimized

        except Exception as e:
            logger.error(f"Error optimizing query: {str(e)}")
            return query
    
    async def generate_response(self, question: str, data: str, chat_history: List[Dict[str, Any]] = None) -> str:
        """Generate response with schema-aware analysis"""
        try:
            # Get relevant schema context
            schema_context = schema_manager.get_relevant_context(question)
            formatted_context = self._format_schema_context(schema_context)
            
            # Create analysis prompt with schema context
            response = await self.chain.ainvoke({
                "question": question,
                "schema_context": formatted_context,
                "results": data,
                "chat_history": chat_history or []
            })
            
            return response

        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def generate_visualization(self, data: List[Dict[str, Any]], question: str) -> Dict[str, Any]:
        """Generate visualization with schema context awareness"""
        try:
            # Get schema context for visualization guidance
            schema_context = schema_manager.get_relevant_context(question)
            
            # Create visualization prompt
            prompt = f"""Based on the schema context and data, create an appropriate visualization:

Schema Context:
{self._format_schema_context(schema_context)}

Question: {question}

Data: {json.dumps(data, indent=2)}

Consider:
1. The data structure and relationships
2. Appropriate visualization types for the data
3. Key metrics and dimensions to highlight
4. Time series patterns if relevant
5. Geographic visualizations if location data is present

Return a visualization configuration as JSON."""

            # Generate visualization using the chain
            response = await self.chain.ainvoke({
                "question": prompt,
                "chat_history": []
            })
            
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

    async def agenerate_text(self, prompt: str, output_type: str = "text") -> str:
        """Generate text using OpenAI API with error handling"""
        try:
            # Check if API key is available
            if not self.api_key:
                logger.error("OpenAI API key is missing")
                return "Error: API key is missing"
                
            # Check if API base URL is properly configured
            if not self.api_base:
                logger.error("OpenAI API base URL is missing")
                return "Error: API base URL is missing"
                
            # Log API configuration for debugging
            logger.debug(f"Using OpenAI API base: {self.api_base}")
            
            # Generate response based on output type
            if output_type == "json":
                # For JSON output, use a more structured prompt
                messages = [
                    {"role": "system", "content": "You are a helpful assistant that provides accurate information in JSON format."},
                    {"role": "user", "content": prompt}
                ]
                
                response = await self.client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=messages,
                    temperature=0.2,
                    max_tokens=500
                )
                
                return response.choices[0].message.content
            else:
                # For text output
                messages = [
                    {"role": "system", "content": "You are a helpful assistant that provides accurate information."},
                    {"role": "user", "content": prompt}
                ]
                
                response = await self.client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=messages,
                    temperature=0.7,
                    max_tokens=1000
                )
                
                return response.choices[0].message.content
                
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating text: {error_message}")
            logger.error(traceback.format_exc())
            
            # Check for authentication errors
            if "401" in error_message or "unauthorized" in error_message.lower() or "无效的令牌" in error_message:
                logger.error("Authentication error with OpenAI API. Please check your API key and base URL.")
                return "Error: Authentication failed with the language model service."
                
            # Check for rate limiting
            if "429" in error_message or "rate limit" in error_message.lower():
                logger.error("Rate limit exceeded with OpenAI API.")
                return "Error: Rate limit exceeded with the language model service."
                
            # Generic error
            return f"Error: {error_message}"

    async def close(self):
        """Cleanup resources"""
        if hasattr(self, 'client'):
            await self.client.close()
        if hasattr(self, 'chat_model'):
            await self.chat_model.aclose()
        logger.info("OpenAIAdapter closed successfully")

# Initialize OpenAI adapter
openai_adapter = OpenAIAdapter() 