import os
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class PromptConfig:
    """Class for managing prompt templates"""
    
    def __init__(self):
        """Initialize the prompt config"""
        self.templates = {
            "response_generation": """
You are an AI assistant for a tourism analytics dashboard. You need to respond to a user query based on SQL results.

Original User Query: {user_query}

SQL Query Executed: {sql_query}

SQL Results: {sql_results}

Query Intent: {query_intent}

Visualization Info: {visualization_info}

Your task is to analyze the SQL results and provide a natural, conversational response to the user's query.
The response should:
1. Be concise and to the point
2. Highlight key findings or trends from the data
3. Reference specific numbers or statistics when relevant
4. Include insights that might be useful for tourism analysis

Please respond only with the final answer to the user, in a conversational tone. Do not include any preamble like "Based on the data" or "According to the SQL results".
"""
        }
        
    def get_template(self, template_name: str) -> str:
        """Get a prompt template by name"""
        if template_name not in self.templates:
            logger.warning(f"Template '{template_name}' not found, using default template")
            return "Please provide a response to: {user_query}"
            
        return self.templates[template_name]
        
    def add_template(self, name: str, template: str) -> None:
        """Add a new template or update an existing one"""
        self.templates[name] = template 