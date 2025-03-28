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

logger = logging.getLogger(__name__)

class GeminiAdapter:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY") or settings.GEMINI_API_KEY
        logger.info(f"Initializing GeminiAdapter with API key: {self.api_key[:10]}...")
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-1.5-pro')
        self.last_request_time = 0
        self.min_request_interval = 2.0  # Increased to 2 seconds between requests
        
    async def _wait_for_rate_limit(self):
        """Wait if necessary to respect rate limits"""
        current_time = asyncio.get_event_loop().time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last_request)
        self.last_request_time = asyncio.get_event_loop().time()
        
    async def _handle_rate_limit(self, error_msg: str) -> Optional[int]:
        """Handle rate limit errors and return retry delay if applicable"""
        if "429" in error_msg and "quota" in error_msg.lower():
            try:
                # Extract retry delay from error message
                retry_delay = int(error_msg.split("retry_delay")[1].split("seconds")[0].strip())
                logger.warning(f"Rate limit hit. Waiting {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                return retry_delay
            except:
                logger.warning("Could not parse retry delay from error message")
        return None
        
    async def generate_sql(self, query: str, schema_context: str) -> str:
        """Generate SQL query from natural language"""
        try:
            await self._wait_for_rate_limit()
            
            prompt = f"""
            Given the following database schema:
            {schema_context}
            
            Generate a PostgreSQL query for this question: {query}
            
            Return ONLY the SQL query, nothing else.
            """
            
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.model.generate_content(prompt)
            )
            
            return response.text.strip()
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error generating SQL: {error_msg}")
            logger.error(traceback.format_exc())
            
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
                    logger.error(f"Error in retry attempt: {str(retry_error)}")
            
            return None
            
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
            logger.error(f"Error generating response: {error_msg}")
            logger.error(traceback.format_exc())
            
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
                    logger.error(f"Error in retry attempt: {str(retry_error)}")
            
            return f"Error analyzing data: {error_msg}" 