from anthropic import Anthropic
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from app.core.config import settings
from typing import Dict, Any, Optional

class ClaudeAdapter:
    def __init__(self):
        self.client = Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        
        # SQL Generation Prompt
        self.sql_prompt = PromptTemplate(
            input_variables=["schema_context", "user_query"],
            template="""
            You are an expert SQL developer specializing in tourism analytics.
            Use the PostgreSQL dialect when generating SQL.
            Write only SQL code, no explanations needed unless asked.
            Stay within the provided DB schema.
            
            Important Rules:
            1. All table names must be prefixed with 'data_lake.' schema name
            2. When referencing columns, use the full table reference, e.g., data_lake.regions.region_name
            3. In the SELECT clause, give clear alias names without dots
            4. The regions table contains:
               - region_id (integer)
               - region_name (varchar)
               - region_type (varchar) - can be 'canton' or 'district'
               - parent_region_id (integer) - references the parent region
            5. Example queries:
               -- Get all districts in Ticino
               SELECT region_name, region_type 
               FROM data_lake.regions 
               WHERE region_type = 'district' 
               AND parent_region_id = (SELECT region_id FROM data_lake.regions WHERE region_name = 'Ticino');
               
               -- Get visit counts by region
               SELECT r.region_name, COUNT(v.visit_id) as total_visits
               FROM data_lake.regions r
               LEFT JOIN data_lake.visits v ON r.region_id = v.region_id
               GROUP BY r.region_name;
            
            Database Schema Context:
            {schema_context}
            
            User Query:
            {user_query}
            
            Generate SQL query:
            """
        )
        
        # Response Generation Prompt
        self.response_prompt = PromptTemplate(
            input_variables=["sql_query", "query_result", "user_query"],
            template="""
            You are a helpful tourism data analyst. Explain the query results in a clear and concise way.
            
            User Query:
            {user_query}
            
            SQL Query Used:
            {sql_query}
            
            Query Results:
            {query_result}
            
            Provide a natural language explanation of the results:
            """
        )
    
    async def generate_sql(self, schema_context: str, user_query: str) -> str:
        """Generate SQL query from natural language using Claude"""
        prompt = self.sql_prompt.format(
            schema_context=schema_context,
            user_query=user_query
        )
        
        response = await self.client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            temperature=0.1,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        
        return response.content[0].text.strip()
    
    async def generate_response(
        self,
        sql_query: str,
        query_result: Dict[str, Any],
        user_query: str
    ) -> str:
        """Generate natural language response from query results"""
        prompt = self.response_prompt.format(
            sql_query=sql_query,
            query_result=str(query_result),
            user_query=user_query
        )
        
        response = await self.client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            temperature=0.1,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        
        return response.content[0].text.strip()
    
    async def process_query(
        self,
        schema_context: str,
        user_query: str,
        query_result: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Process a complete query cycle"""
        # Generate SQL
        sql_query = await self.generate_sql(schema_context, user_query)
        
        # Generate response if query result is provided
        response = None
        if query_result is not None:
            response = await self.generate_response(sql_query, query_result, user_query)
        
        return {
            "sql_query": sql_query,
            "response": response
        }

# Initialize Claude adapter
claude_adapter = ClaudeAdapter() 