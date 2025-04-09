from typing import Dict, Any, List, Optional
import logging
import json
from datetime import datetime
from app.utils.intent_parser import IntentParser, QueryIntent, TimeGranularity
from app.llm.openai_adapter import OpenAIAdapter
import re
from enum import Enum

logger = logging.getLogger(__name__)

class QueryIntent(Enum):
    """Enum for different query intents"""
    VISITOR_COMPARISON = "visitor_comparison"
    PEAK_PERIOD = "peak_period"
    SPENDING = "spending"
    TREND = "trend"
    REGION_ANALYSIS = "region_analysis"
    HOTSPOT_DETECTION = "hotspot_detection"
    SPATIAL_PATTERN = "spatial_pattern"
    INDUSTRY_ANALYSIS = "industry_analysis"
    VISITOR_COUNT = "visitor_count"
    GEO_SPATIAL = "geo_spatial"
    
    def __str__(self):
        return self.value

class HybridIntentParser:
    """
    Hybrid intent parser that combines rule-based and LLM-based parsing
    to determine user intent from natural language queries.
    """
    
    def __init__(self, llm_adapter=None):
        """Initialize the hybrid intent parser with components"""
        self.llm_adapter = llm_adapter
        self.rule_parser = IntentParser()  # Rule-based parser
        logger.info("HybridIntentParser initialized")
    
    async def parse_intent(self, message: str) -> Dict[str, Any]:
        """
        Parse user message to determine intent and extract parameters
        using both rule-based and LLM approaches
        """
        try:
            # First try rule-based parsing
            rule_result = self._parse_with_rules(message)
            
            # Check if this is explicitly a map/geographic visualization request
            is_map_request = any(term in message.lower() for term in [
                'map', 'geographic', 'geospatial', 'visualization', 'visualize', 'plot', 'show'
            ]) and any(term in message.lower() for term in [
                'region', 'area', 'canton', 'city', 'country', 'location', 'where', 'place'
            ])
            
            # For geospatial queries, enhance with LLM if available
            if rule_result.get('intent') in [
                QueryIntent.GEO_SPATIAL, 
                QueryIntent.REGION_ANALYSIS,
                QueryIntent.HOTSPOT_DETECTION,
                QueryIntent.SPATIAL_PATTERN
            ]:
                # Check if we have a valid LLM adapter
                if self.llm_adapter:
                    try:
                        llm_result = await self._parse_with_llm(message, rule_result)
                        # Merge results, preferring LLM for region info
                        if 'region_info' in llm_result:
                            rule_result['region_info'] = llm_result['region_info']
                    except Exception as llm_error:
                        logger.error(f"Error using LLM for geospatial parsing: {str(llm_error)}")
                        # Continue with rule-based result
                else:
                    logger.info("LLM adapter not available for geospatial query enhancement")
                
                # Ensure we have region info from rule-based parsing if LLM failed
                if 'region_info' not in rule_result and 'region_name' in rule_result:
                    rule_result['region_info'] = {
                        'region_name': rule_result.get('region_name', ''),
                        'region_type': rule_result.get('region_type', 'unknown')
                    }
                
                # Store original intent for potential fallback
                original_intent = rule_result.get('intent')
                
                # Add a flag to indicate if this is explicitly a map visualization request
                rule_result['is_map_request'] = is_map_request
                
                # Add a fallback intent for cases where geographic data might not be available
                if is_map_request and original_intent == QueryIntent.GEO_SPATIAL:
                    # If it's a general geo request, default to REGION_ANALYSIS
                    rule_result['fallback_intent'] = QueryIntent.REGION_ANALYSIS
                else:
                    # Otherwise, provide trend analysis as a fallback
                    rule_result['fallback_intent'] = QueryIntent.TREND_ANALYSIS
                
                return rule_result
            
            return rule_result
            
        except Exception as e:
            logger.error(f"Error in hybrid intent parsing: {str(e)}")
            # Fallback to basic intent - visitor count
            return {
                'intent': QueryIntent.VISITOR_COUNT,
                'time_range': {'start_date': '2023-01-01', 'end_date': '2024-01-01'},
                'granularity': TimeGranularity.DAY
            }
    
    def _parse_with_rules(self, message: str) -> Dict[str, Any]:
        """Use rule-based parsing to extract intent and parameters"""
        return self.rule_parser.parse_query_intent(message)
    
    async def _parse_with_llm(self, message: str, rule_result: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance intent parsing with LLM for more complex queries"""
        try:
            # Check if LLM adapter is available and properly configured
            if not self.llm_adapter:
                logger.warning("LLM adapter not available, falling back to rule-based parsing")
                return {}
                
            prompt = f"""
            Extract precise geospatial information from this query:
            Query: "{message}"
            
            Focus on identifying:
            1. The region name (e.g., Ticino, Lugano, Swiss Alps)
            2. The region type (e.g., canton, city, district, area)
            
            Respond in JSON format:
            {{
                "region_info": {{
                    "region_name": "extracted name",
                    "region_type": "extracted type"
                }}
            }}
            """
            
            try:
                response = await self.llm_adapter.agenerate_text(prompt, output_type="json")
                
                # Clean the response string
                cleaned_response = response.strip()
                if cleaned_response.startswith("```json"):
                    cleaned_response = cleaned_response[7:]  # Remove ```json
                if cleaned_response.endswith("```"):
                    cleaned_response = cleaned_response[:-3] # Remove ```
                cleaned_response = cleaned_response.strip() # Remove any extra whitespace

                try:
                    # Attempt to parse the cleaned JSON string
                    result = json.loads(cleaned_response)
                    return result
                except json.JSONDecodeError:
                    # Log the cleaned response that failed parsing
                    logger.error(f"LLM returned invalid JSON after cleaning: {cleaned_response}")
                    return {}
            except Exception as llm_error:
                # Handle authentication errors or other LLM-related issues
                logger.error(f"LLM service error: {str(llm_error)}")
                # Extract region info from rule_result if available
                if 'region_info' in rule_result:
                    logger.info("Using region info from rule-based parsing")
                    return {'region_info': rule_result['region_info']}
                return {}
                
        except Exception as e:
            logger.error(f"Error in LLM-based intent parsing: {str(e)}")
            return {} 