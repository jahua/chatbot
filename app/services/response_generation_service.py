import logging
import json
from typing import Dict, Any, Optional, List, Tuple
import os
import traceback
import asyncio
from datetime import datetime, date
from decimal import Decimal

from app.rag.debug_service import DebugService
from app.utils.intent_parser import QueryIntent
from app.models.prompt_config import PromptConfig
from app.core.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def custom_json_serializer(obj):
    """Convert Decimal, date, and datetime objects for JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    try:
        # Let default encoder handle others or raise TypeError
        return json.JSONEncoder.default(None, obj)
    except TypeError:
         raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

class ResponseGenerationService:
    """Service for generating natural language responses to user queries"""
    
    def __init__(self, llm_adapter, debug_service: Optional[DebugService] = None):
        """
        Initialize the ResponseGenerationService
        
        Args:
            llm_adapter: The language model adapter to use for text generation
            debug_service: Optional debug service for tracking debug info
        """
        self.llm_adapter = llm_adapter
        self.debug_service = debug_service
        self.prompt_config = PromptConfig()
        self.api_timeout = settings.LLM_API_TIMEOUT  # Get timeout from settings
    
    async def generate_response(
        self, 
        query: str, 
        sql_query: Optional[str] = None,
        sql_results: Optional[List[Dict[str, Any]]] = None,
        intent: Optional[QueryIntent] = None,
        visualization_info: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate a natural language response based on user query and SQL results.
        
        Args:
            query: The original user query
            sql_query: Optional SQL query that was executed
            sql_results: Optional results from SQL query execution
            intent: Optional detected query intent
            visualization_info: Optional visualization info (type, data)
            context: Optional additional context
            
        Returns:
            Generated natural language response
        """
        try:
            # Start debug step if debug service is available
            if self.debug_service:
                self.debug_service.start_step("llm_response_generation", {
                    "query": query,
                    "has_sql_query": sql_query is not None,
                    "has_sql_results": sql_results is not None and len(sql_results) > 0
                })
            
            # Check for single value results for quick fallbacks
            single_value = self._check_for_single_value(sql_results)
            
            # Format visualization info
            vis_description = None
            if visualization_info:
                vis_type = visualization_info.get("type", "table")
                vis_description = f"A {vis_type} visualization of the data was created."
            
            # Format the prompt for the response generation
            prompt = self._create_response_prompt(
                query=query,
                sql_query=sql_query,
                sql_results=sql_results,
                intent=intent,
                visualization_info=vis_description,
                context=context
            )
            
            # Log the prompt
            logger.debug(f"Response generation prompt: {prompt}")
            
            # Add timeout handling to LLM call with retries
            max_retries = 3
            retry_count = 0
            last_error = None
            
            while retry_count < max_retries:
                try:
                    # Generate response using the LLM with timeout
                    generated_response = await asyncio.wait_for(
                        self.llm_adapter.agenerate_text(prompt),
                        timeout=self.api_timeout
                    )
                    
                    # Check if the response indicates an API error
                    if generated_response.startswith("Error:"):
                        logger.error(f"LLM API error in response generation: {generated_response}")
                        last_error = generated_response
                        retry_count += 1
                        continue
                    
                    # Add debug details if debug service is available
                    if self.debug_service:
                        self.debug_service.add_step_details({
                            "prompt": prompt,
                            "response_length": len(generated_response) if generated_response else 0,
                            "retries": retry_count
                        })
                        self.debug_service.end_step("llm_response_generation", success=True)
                    
                    # Enhance response with visualization information
                    enhanced_response = self._enhance_response_with_visualization_info(generated_response, visualization_info)
                    
                    return enhanced_response
                
                except asyncio.TimeoutError:
                    logger.error(f"LLM API call timed out after {self.api_timeout} seconds (attempt {retry_count + 1}/{max_retries})")
                    last_error = f"API timeout after {self.api_timeout}s"
                    retry_count += 1
                    continue
                except Exception as e:
                    logger.error(f"Error in LLM call (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                    last_error = str(e)
                    retry_count += 1
                    continue
            
            # If we get here, all retries failed
            logger.error(f"All {max_retries} attempts failed. Last error: {last_error}")
            if self.debug_service:
                self.debug_service.end_step("llm_response_generation", success=False, error=last_error)
            
            # Use fallback response
            return self._generate_fallback_response(query, sql_query, sql_results, single_value)
            
        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            logger.error(traceback.format_exc())
            
            # End debug step with error if debug service is available
            if self.debug_service:
                self.debug_service.end_step("llm_response_generation", success=False, error=str(e))
            
            # Generate fallback response
            return self._generate_fallback_response(query, sql_query, sql_results, single_value)
    
    def _check_for_single_value(self, sql_results: Optional[List[Dict[str, Any]]]) -> Optional[Tuple[str, Any]]:
        """Check if results contain a single value that can be easily reported"""
        if not sql_results or len(sql_results) != 1:
            return None
            
        # Check if there's only one value in the result
        result = sql_results[0]
        if len(result) != 1:
            return None
            
        # Extract the single key-value pair
        key = list(result.keys())[0]
        value = result[key]
        
        return (key, value)
    
    def _generate_fallback_response(
        self, 
        query: str, 
        sql_query: Optional[str], 
        sql_results: Optional[List[Dict[str, Any]]],
        single_value: Optional[Tuple[str, Any]] = None
    ) -> str:
        """Generate a fallback response when the LLM call fails"""
        try:
            # Check if we have a single value result that's easy to report
            if single_value:
                key, value = single_value
                key_readable = key.replace("_", " ")
                
                # Format specific types of values
                if isinstance(value, (int, float)):
                    # Handle numeric values
                    return f"Based on the data, the {key_readable} is {value:,.2f}."
                elif isinstance(value, (str, datetime)):
                    # Handle string or date values
                    return f"Based on the data, the {key_readable} is {value}."
                else:
                    # Generic format
                    return f"The query returned that the {key_readable} is {value}."
            
            # For empty results
            if not sql_results or len(sql_results) == 0:
                return "I didn't find any data matching your query. Please try a different question or modify your search criteria."
            
            # Enhanced analytics for specific query types based on SQL query and results
            query_lower = query.lower()
            
            # Handle industry spending queries
            if ("industry" in query_lower and "spending" in query_lower) or ("highest spending" in query_lower):
                if len(sql_results) > 0 and "industry_name" in sql_results[0] and ("total_spending" in sql_results[0] or "spending" in sql_results[0]):
                    industry = sql_results[0].get("industry_name", "Unknown")
                    spending_key = "total_spending" if "total_spending" in sql_results[0] else "spending" if "spending" in sql_results[0] else next((k for k in sql_results[0].keys() if "spend" in k.lower()), None)
                    
                    if spending_key:
                        spending = sql_results[0].get(spending_key, 0)
                        if isinstance(spending, (int, float)):
                            spending_formatted = f"{spending:,.2f}"
                            return f"The industry with the highest spending is {industry} with a total of {spending_formatted}. This represents a significant portion of tourism expenditure, indicating {industry}'s importance in the tourism economy."
            
            # Handle visitor count queries
            if "visitor" in query_lower or "tourist" in query_lower:
                if len(sql_results) > 0 and any(k for k in sql_results[0].keys() if "visitor" in k.lower() or "tourist" in k.lower()):
                    visitor_key = next((k for k in sql_results[0].keys() if "visitor" in k.lower() or "tourist" in k.lower()), None)
                    region_key = next((k for k in sql_results[0].keys() if "region" in k.lower() or "location" in k.lower()), None)
                    date_key = next((k for k in sql_results[0].keys() if "date" in k.lower() or "time" in k.lower() or "year" in k.lower() or "month" in k.lower()), None)
                    
                    if visitor_key:
                        visitors = sql_results[0].get(visitor_key, 0)
                        region = sql_results[0].get(region_key, "the analyzed region") if region_key else "the analyzed region"
                        time_period = f" in {sql_results[0].get(date_key)}" if date_key and sql_results[0].get(date_key) else ""
                        
                        return f"{region.capitalize()} had {visitors:,} visitors{time_period}. This data helps understand tourism patterns and can inform strategic planning for hospitality and retail sectors."
            
            # Handle time-based queries (busiest periods)
            if "busiest" in query_lower or "peak" in query_lower:
                if len(sql_results) > 0:
                    time_key = next((k for k in sql_results[0].keys() if "week" in k.lower() or "day" in k.lower() or "month" in k.lower()), None)
                    visitor_key = next((k for k in sql_results[0].keys() if "visitor" in k.lower() or "tourist" in k.lower() or "count" in k.lower()), None)
                    
                    if time_key and visitor_key:
                        time_value = sql_results[0].get(time_key)
                        visitor_count = sql_results[0].get(visitor_key)
                        return f"The busiest period was {time_key.replace('_', ' ')} {time_value} with {visitor_count:,} visitors. This peak period indicates optimal timing for tourism-related promotions and resource allocation."
            
            # Generic response for multiple results
            if len(sql_results) == 1:
                # Single result with multiple columns - create a simple summary
                result = sql_results[0]
                key_parts = []
                for key, value in result.items():
                    formatted_key = key.replace("_", " ")
                    if isinstance(value, (int, float)) and value > 1000:
                        formatted_value = f"{value:,.2f}"
                    else:
                        formatted_value = str(value)
                    key_parts.append(f"{formatted_key} is {formatted_value}")
                
                summary = ", ".join(key_parts)
                return f"Based on the analysis: {summary}. This information provides insight into tourism patterns that can help with strategic planning."
            else:
                # Multiple results - provide a count and basic stats
                return f"The analysis found {len(sql_results)} results for your query about {query}. The data shows variations across different categories that can help identify key trends in tourism behavior."
        
        except Exception as e:
            logger.error(f"Error generating fallback response: {str(e)}")
            # Ultimate fallback
            return "The analysis shows important patterns in tourism data that can help inform strategic decisions. For more detailed insights, please try a more specific query."
    
    def _create_response_prompt(
        self,
        query: str,
        sql_query: Optional[str] = None,
        sql_results: Optional[List[Dict[str, Any]]] = None,
        intent: Optional[QueryIntent] = None,
        visualization_info: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a prompt for generating a natural language response based on query and results
        
        Args:
            query: The original user query
            sql_query: Optional SQL query that was executed
            sql_results: Optional results from SQL query execution
            intent: Optional detected query intent (can be QueryIntent object or string)
            visualization_info: Optional info about visualization
            context: Optional additional context
            
        Returns:
            A prompt for the LLM
        """
        # Basic structure of the prompt
        has_results = sql_results is not None and len(sql_results) > 0
        
        # Handle both QueryIntent objects and string intents
        if intent is None:
            intent_str = "unknown"
        elif hasattr(intent, 'value'):
            intent_str = intent.value
        else:
            intent_str = str(intent)
        
        # Safely serialize results, handling Decimal and Date/Datetime
        try:
            result_str = json.dumps(sql_results, indent=2, default=custom_json_serializer) if has_results else "No results found."
        except TypeError as e:
            logger.error(f"Error serializing SQL results to JSON: {e}")
            result_str = "[Error: Could not serialize results]"
        
        # Base prompt
        prompt = f"""
You are an AI assistant for a tourism analytics dashboard. You need to respond to a user query based on SQL results.

Original User Query: {query}
"""
        # Add SQL query if available
        if sql_query:
            prompt += f"\nSQL Query Executed: {sql_query}\n"
        
        # Add SQL results if available
        if has_results:
            prompt += f"\nSQL Results: {result_str}\n"
        else:
            prompt += "\nNo data was found for this query.\n"
        
        # Add intent information
        prompt += f"\nQuery Intent: {intent_str}\n"
        
        # Add visualization information
        if visualization_info:
            prompt += f"\nVisualization Info: {visualization_info}\n"
        
        # Add context information
        context_info = []
        if context:
            for key, value in context.items():
                if key != "schema" and value:  # Skip large schema information
                    if isinstance(value, (dict, list)):
                        context_info.append(f"{key}: {json.dumps(value)}")
                    else:
                        context_info.append(f"{key}: {value}")
        
        if context_info:
            prompt += f"\nAdditional Context:\n" + "\n".join(context_info) + "\n"
        
        # Include task instructions to guide the response
        prompt += """
Your task is to analyze the SQL results and provide a natural, conversational response to the user's query.
The response should:
1. Be concise and to the point
2. Highlight key findings or trends from the data
3. Reference specific numbers or statistics when relevant
4. Include insights that might be useful for tourism analysis

If no data was found, politely inform the user and suggest they try alternative queries as mentioned in the suggestions.

IMPORTANT GUIDELINES:
1. Focus on answering only what was asked - don't expand unnecessarily into unrelated areas
2. Be precise with numbers - use proper formatting (e.g., "1,234,567" not "1234567")
3. For percentages, specify the precise meaning (e.g., "a 10% increase" rather than just "10%")
4. When mentioning time periods, be explicit (e.g., "during the summer of 2023" rather than just "during summer")
5. For comparisons, provide both absolute and relative differences when possible
6. If the query requires a specific tourism metric, explain its meaning briefly
7. Maintain specificity - use exact region/location names from the results
8. For year-over-year or period-over-period comparisons, highlight notable changes
9. For very small or very large values, consider providing context for scale
10. Avoid making claims not supported by the data

Please respond only with the final answer to the user, in a conversational tone. Do not include any preamble like "Based on the data" or "According to the SQL results".
"""
        
        return prompt

    def _enhance_response_with_visualization_info(
        self, response: str, visualization_info: Optional[Dict[str, Any]]) -> str:
        """Enhance response with visualization information"""
        # Skip if there's no visualization or if it's not a plotly chart
        if not visualization_info or not isinstance(visualization_info, dict):
            return response
        
        try:
            # Get visualization type
            vis_type = visualization_info.get("type", "")
            
            # Check for valid plotly visualization
            if vis_type == "plotly" and isinstance(visualization_info.get("data"), dict):
                layout = visualization_info.get("data", {}).get("layout", {})
                title = ""
                
                # Handle different ways the title might be stored
                if isinstance(layout, dict):
                    if isinstance(layout.get("title"), str):
                        title = layout.get("title", "")
                    elif isinstance(layout.get("title"), dict):
                        title = layout.get("title", {}).get("text", "")
                
                # Check for Swiss and international tourist visualization
                if "Swiss" in title and "International" in title and "Tourist" in title:
                    # Add a specific message for the monthly tourist comparison
                    visualization_message = (
                        "\n\nI've created a bar chart visualization comparing Swiss and international "
                        "tourists per month. The chart shows the distribution of both visitor types "
                        "throughout the year, allowing you to see seasonal patterns and compare "
                        "domestic vs. international tourism flows."
                    )
                    
                    # Add the message to the response
                    return response + visualization_message
        except Exception as e:
            logger.error(f"Error enhancing response with visualization info: {str(e)}")
        
        # Default case - return original response
        return response 