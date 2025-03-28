from typing import Optional
import aiohttp
import json
import logging
import traceback
from app.core.config import settings

logger = logging.getLogger(__name__)

class OllamaAdapter:
    def __init__(self):
        self.base_url = settings.OLLAMA_BASE_URL
        self.model = settings.OLLAMA_MODEL
        logger.info(f"Initializing OllamaAdapter with model: {self.model}")
        
    async def generate_sql(self, query: str, schema_context: str) -> Optional[str]:
        """Generate SQL query from natural language"""
        prompt = f"""You are a SQL expert. Generate a PostgreSQL query for the following user query.
        
Schema Context:
{schema_context}

Example Query:
SELECT 
    data_lake.aoi_days_raw.aoi_date,
    data_lake.aoi_days_raw.visitors ->> 'total' as total_visitors,
    data_lake.aoi_days_raw.visitors ->> 'swiss_local' as swiss_local,
    data_lake.aoi_days_raw.visitors ->> 'swiss_tourist' as swiss_tourist
FROM data_lake.aoi_days_raw
WHERE data_lake.aoi_days_raw.aoi_id = 'some-id'
AND data_lake.aoi_days_raw.aoi_date BETWEEN '2023-01-01' AND '2023-01-31';

User Query:
{query}

Return ONLY the SQL query, nothing else."""
        
        try:
            logger.info(f"Generating SQL for query: {query}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.2,
                            "top_p": 0.8,
                            "top_k": 40,
                            "num_predict": 1024
                        }
                    }
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        sql_query = result.get("response", "").strip()
                        
                        # Remove any markdown code block formatting
                        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
                        
                        logger.info(f"Generated SQL: {sql_query}")
                        
                        # Basic SQL validation
                        if not sql_query.upper().startswith('SELECT'):
                            logger.error(f"Invalid SQL generated: {sql_query}")
                            return None
                            
                        return sql_query
                    else:
                        error_text = await response.text()
                        logger.error(f"Error from Ollama API: {error_text}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    async def generate_response(self, prompt: str, sql_query: str, data: str) -> str:
        """Generate natural language response with structured analysis"""
        analysis_prompt = f"""
        Analyze the following data and provide:
        1. A summary of key findings in bullet points
        2. A structured representation of the data (as a table/dataframe if applicable)
        3. Statistical insights or trends
        4. Any notable patterns or anomalies
        
        SQL Query:
        {sql_query}
        
        Data:
        {data}
        
        Format the response in a clear, structured way using markdown formatting.
        """
        
        try:
            logger.info(f"Generating analysis for data: {data[:100]}...")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": analysis_prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.7,
                            "top_p": 0.8,
                            "top_k": 40,
                            "num_predict": 2048
                        }
                    }
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        analysis = result.get("response", "")
                        logger.debug(f"Raw analysis response: {analysis[:100]}...")
                        return analysis
                    else:
                        error_text = await response.text()
                        logger.error(f"Error from Ollama API: {error_text}")
                        return f"Error analyzing data: {error_text}"
                        
        except Exception as e:
            logger.error(f"Error generating response: {str(e)}")
            logger.error(traceback.format_exc())
            return f"Error analyzing data: {str(e)}"

# Initialize Ollama adapter
ollama_adapter = OllamaAdapter() 