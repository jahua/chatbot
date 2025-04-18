import logging
import json
from typing import Dict, Any, Optional, List, Tuple
import os

from app.rag.debug_service import DebugService
from app.utils.intent_parser import QueryIntent
from app.models.prompt_config import PromptConfig

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
            
            # Generate response using the LLM
            generated_response = await self.llm_adapter.agenerate_text(prompt)
            
            # Add debug details if debug service is available
            if self.debug_service:
                self.debug_service.add_step_details({
                    "prompt": prompt,
                    "response_length": len(generated_response) if generated_response else 0
                })
                
                # End debug step
                self.debug_service.end_step()
                
            return generated_response
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            
            # End debug step with error if debug service is available
            if self.debug_service:
                self.debug_service.end_step(success=False, error=str(e))
                
            return f"I apologize, but I encountered an error while generating a response: {str(e)}"
    
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
            
        # Format the values for the prompt
        prompt_values = {
            "user_query": query,
            "sql_query": sql_query if sql_query else "No SQL query was executed.",
            "sql_results": results_str if sql_results else "No results available.",
            "query_intent": intent.value if intent else "unknown",
            "visualization_info": visualization_info if visualization_info else "No visualization was created."
        }
        
        # Add any additional context
        if context:
            prompt_values.update(context)
            
        # Format the prompt template with the values
        prompt = prompt_template.format(**prompt_values)
        return prompt 