import logging
import json
from typing import Dict, Any, Optional, List, Tuple
import os
import traceback
import asyncio
from datetime import datetime

from app.rag.debug_service import DebugService
from app.utils.intent_parser import QueryIntent
from app.models.prompt_config import PromptConfig
from app.core.config import settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        """Create a prompt for response generation"""
        # Get the response prompt template
        prompt_template = self.prompt_config.get_template("response_generation")
        
        # Format the SQL results as a string, limited to a reasonable size
        results_str = json.dumps(sql_results, indent=2)
        if len(results_str) > 10000:  # Limit result size
            results_str = results_str[:10000] + "...[truncated]"
            
        # Add suggestions for alternative queries when no results are found
        alternative_suggestions = []
        if not sql_results or (isinstance(sql_results, list) and len(sql_results) == 0):
            # Generate contextual suggestions based on the query
            query_lower = query.lower()
            
            # Region-specific suggestions
            if any(region in query_lower for region in ["lugano", "locarno", "bellinzona", "mendrisio", "city"]):
                alternative_suggestions.append(f"Try a different region like 'Lugano', 'Locarno', or 'Bellinzona'")
                alternative_suggestions.append(f"Show me tourism trends across all of Ticino")
            else:
                alternative_suggestions.append(f"How many tourists visited Lugano in 2023?")
                alternative_suggestions.append(f"Show me tourism trends for a specific region in Ticino")
            
            # Time-specific suggestions
            if any(time_term in query_lower for time_term in ["year", "month", "season", "summer", "winter", "2023", "2022"]):
                alternative_suggestions.append(f"What about tourism in a different time period?")
                alternative_suggestions.append(f"Compare tourism between summer and winter months")
            else:
                alternative_suggestions.append(f"Tourism statistics for summer 2023")
                alternative_suggestions.append(f"Compare winter vs summer tourism patterns")
            
            # Spending-specific suggestions
            if "spending" in query_lower or "expenditure" in query_lower:
                alternative_suggestions.append(f"Which industry had the highest spending?")
                alternative_suggestions.append(f"Compare spending between different tourism sectors")
            else:
                alternative_suggestions.append(f"What was the breakdown of tourism spending by industry?")
            
            # Visitor type suggestions
            if "tourist" in query_lower or "visitor" in query_lower:
                alternative_suggestions.append(f"Compare Swiss tourists vs foreign tourists")
                alternative_suggestions.append(f"Which region had the most visitors?")
            else:
                alternative_suggestions.append(f"How do Swiss and foreign tourist numbers compare?")
                
            # Always include a few general alternatives
            alternative_suggestions.append(f"Show me the busiest period for tourism in Ticino")
            
            # Remove duplicates and limit to 5 suggestions
            alternative_suggestions = list(dict.fromkeys(alternative_suggestions))[:5]
            
        # Format the values for the prompt
        prompt_values = {
            "user_query": query,
            "sql_query": sql_query if sql_query else "No SQL query was executed.",
            "sql_results": results_str if sql_results else "No results available.",
            "query_intent": intent.value if intent else "unknown",
            "visualization_info": visualization_info if visualization_info else "No visualization was created.",
            "alternative_suggestions": json.dumps(alternative_suggestions)
        }
        
        # Add any additional context
        if context:
            prompt_values.update(context)
            
        # Format the prompt template with the values
        prompt = prompt_template.format(**prompt_values)
        return prompt 

    def _enhance_response_with_visualization_info(
        self, response: str, visualization_info: Optional[Dict[str, Any]]) -> str:
        """Enhance response with visualization information"""
        # Skip if there's no visualization or if it's not a plotly chart
        if not visualization_info or not isinstance(visualization_info, dict):
            return response
        
        # Check for Swiss and international tourist visualization
        if (visualization_info.get("type") == "plotly" and 
            isinstance(visualization_info.get("data"), dict) and
            isinstance(visualization_info.get("data").get("layout"), dict) and
            "Swiss and International Tourists" in str(visualization_info.get("data").get("layout").get("title", ""))):
            
            # Add a specific message for the monthly tourist comparison
            visualization_message = (
                "\n\nI've created a bar chart visualization comparing Swiss and international "
                "tourists per month. The chart shows the distribution of both visitor types "
                "throughout the year, allowing you to see seasonal patterns and compare "
                "domestic vs. international tourism flows."
            )
            
            # Add the message to the response
            return response + visualization_message
        
        return response 