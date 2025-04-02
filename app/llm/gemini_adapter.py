from typing import Optional
import google.generativeai as genai
from app.core.config import settings
import json
import os
import asyncio
from functools import partial
import re
import logging
import traceback
import time

class GeminiAdapter:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY") or settings.GEMINI_API_KEY
        # Set up logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing GeminiAdapter...")
        self.logger.info(f"Using API key: {self.api_key[:10]}...")
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-1.5-pro')
        self.last_request_time = 0
        self.min_request_interval = 2.0  # Increased to 2 seconds between requests
        self.logger.debug("GeminiAdapter initialized successfully")
        
    async def _wait_for_rate_limit(self):
        """Wait if necessary to respect rate limits"""
        current_time = asyncio.get_event_loop().time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            wait_time = self.min_request_interval - time_since_last_request
            self.logger.debug(f"Rate limit: Waiting {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)
        self.last_request_time = asyncio.get_event_loop().time()
        
    async def _handle_rate_limit(self, error_msg: str) -> Optional[int]:
        """Handle rate limit errors and return retry delay if applicable"""
        if "429" in error_msg and "quota" in error_msg.lower():
            try:
                # Extract retry delay from error message
                retry_delay = int(error_msg.split("retry_delay")[1].split("seconds")[0].strip())
                self.logger.warning(f"Rate limit hit. Waiting {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                return retry_delay
            except:
                self.logger.warning("Could not parse retry delay from error message")
        return None
        
    async def generate_sql(self, message: str, schema_summary: str) -> str:
        """Generate SQL query from natural language message"""
        self.logger.debug(f"Generating SQL for message: {message}")
        
        try:
            await self._wait_for_rate_limit()
            
            # Predefined queries for common patterns
            if "weekly visitor patterns" in message.lower():
                self.logger.debug("Using predefined weekly visitor pattern query")
                if "spring" in message.lower():
                    self.logger.debug("Generating spring-specific query")
                    query = """
                    SELECT 
                        EXTRACT(WEEK FROM aoi_date) as week_number,
                        ROUND(AVG((visitors->>'swissTourist')::numeric)) as swiss_tourists,
                        ROUND(AVG((visitors->>'foreignTourist')::numeric)) as foreign_tourists,
                        ROUND(AVG((visitors->>'swissTourist')::numeric + (visitors->>'foreignTourist')::numeric)) as total_visitors
                    FROM data_lake.aoi_days_raw
                    WHERE 
                        EXTRACT(YEAR FROM aoi_date) = 2023
                        AND EXTRACT(MONTH FROM aoi_date) BETWEEN 3 AND 5
                    GROUP BY week_number
                    ORDER BY week_number;
                    """
                    self.logger.debug(f"Generated spring query: {query}")
                    return query
                else:
                    self.logger.debug("Generating general weekly query")
                    query = """
                    SELECT 
                        EXTRACT(WEEK FROM aoi_date) as week_number,
                        ROUND(AVG((visitors->>'swissTourist')::numeric)) as swiss_tourists,
                        ROUND(AVG((visitors->>'foreignTourist')::numeric)) as foreign_tourists,
                        ROUND(AVG((visitors->>'swissTourist')::numeric + (visitors->>'foreignTourist')::numeric)) as total_visitors
                    FROM data_lake.aoi_days_raw
                    WHERE EXTRACT(YEAR FROM aoi_date) = 2023
                    GROUP BY week_number
                    ORDER BY week_number;
                    """
                    self.logger.debug(f"Generated general query: {query}")
                    return query
            
            # For other queries, use the model with a timeout
            self.logger.debug("Using model to generate custom query")
            prompt = f"""Given the following database schema and user message, generate a SQL query that will return the requested data.
            For JSON fields, use proper type casting and aggregation.
            For the visitors JSON field, use (visitors->>'fieldName')::numeric to convert to numbers.

Schema:
{schema_summary}

User message: {message}

Generate a SQL query that will return the requested data. The query should:
1. Use proper type casting for JSON fields
2. Handle aggregations correctly
3. Include appropriate GROUP BY clauses
4. Order results logically

SQL query:"""
            self.logger.debug(f"Generated prompt: {prompt}")

            # Use asyncio.wait_for to add a timeout
            async def generate():
                self.logger.debug("Starting model generation")
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(None, lambda: self.model.generate_content(prompt))
                self.logger.debug("Model generation completed")
                return response.text.strip()
            
            try:
                self.logger.debug("Waiting for model response with timeout")
                response_text = await asyncio.wait_for(generate(), timeout=10.0)  # 10 second timeout
                self.logger.debug(f"Received response: {response_text}")
            except asyncio.TimeoutError:
                self.logger.error("Query generation timed out")
                raise Exception("Query generation timed out. Please try again.")
            
            # Clean up the response
            if response_text.startswith('```sql'):
                response_text = response_text[6:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            
            cleaned_query = response_text.strip()
            self.logger.debug(f"Final cleaned query: {cleaned_query}")
            return cleaned_query
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error generating SQL: {error_msg}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
            
    async def generate_response(self, query: str, sql_query: str, data: str) -> str:
        """Generate natural language response from SQL results"""
        try:
            await self._wait_for_rate_limit()
            
            prompt = f"""
            Analyze the following data from a SQL query:
            
            Query: {sql_query}
            Data:
            {data}
            
            Please provide a detailed analysis in markdown format with:
            1. Key findings in bullet points
            2. Statistical insights
            3. Notable patterns or trends
            """
            
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.model.generate_content(prompt)
            )
            
            return response.text.strip()
            
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error generating response: {error_msg}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Handle rate limit
            retry_delay = await self._handle_rate_limit(error_msg)
            if retry_delay:
                try:
                    await self._wait_for_rate_limit()
                    response = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: self.model.generate_content(prompt)
                    )
                    return response.text.strip()
                except Exception as retry_error:
                    self.logger.error(f"Error in retry attempt: {str(retry_error)}")
            
            return f"Error analyzing data: {error_msg}" 