from typing import Dict, Any, Optional, List
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
import re
from app.rag.debug_service import DebugService
from app.llm.openai_adapter import OpenAIAdapter
import json

logger = logging.getLogger(__name__)

# --- System Prompt Template --- 
# Moved outside the class for clarity
SQL_GENERATION_SYSTEM_PROMPT = """
You are an expert PostgreSQL data analyst AI. You are interacting with a database containing tourism data (visitors, spending) primarily in the 'dw' schema.
Your goal is to generate a syntactically correct PostgreSQL query to answer the user's question based *only* on the provided schema and context.

**Instructions:**
1.  **Analyze the User Question:** Understand the user's intent.
2.  **Use Provided Schema (dw) ONLY:** Base your query *strictly* on the tables and columns described in the 'LIVE SCHEMA (dw)' section. Most tables like `dw.fact_visitor`, `dw.fact_spending`, `dw.dim_date`, `dw.dim_region`, `dw.dim_industry` are in the `dw` schema. Do not hallucinate columns or tables.
3.  **Handle Visitor Columns in `dw.fact_visitor`:** 
    - This table contains visitor counts in specific numeric columns like `swiss_tourists`, `foreign_tourists`, `swiss_locals`, `foreign_workers`, `swiss_commuters`, and `total_visitors`.
    - **DO NOT** assume a JSONB 'visitors' column for these counts.
    - To get the total number of *tourists*, sum the `swiss_tourists` and `foreign_tourists` columns: `SUM(f.swiss_tourists + f.foreign_tourists)`.
    - Use the `total_visitors` column if the query asks for *all* visitor types combined.
4.  **Handle JSONB Columns (Other):** Other columns might be JSONB (like `demographics`, `dwell_time`). For these, use standard PostgreSQL JSONB operators (`->>`, `->`, etc.) based on the schema description if you need to access nested data.
5.  **Use Context:** Refer to the 'DW CONTEXT' section for available regions, date ranges, and descriptions of key metrics to help formulate correct filters and joins.
6.  **Join Appropriately:** Join tables within the `dw` schema when necessary (e.g., `dw.fact_visitor f` with `dw.dim_date d`). If you need to join with a table known to be outside the `dw` schema (like `data_lake.aoi_days_raw`, if applicable based on context), use the fully qualified name.
7.  **Date Filtering and Operations:** 
    - Use the `dw.dim_date` table for filtering by year, month, day, season, etc. 
    - Example: `JOIN dw.dim_date d ON f.date_id = d.date_id WHERE d.year = 2023 AND d.month BETWEEN 3 AND 5` for Spring 2023.
    - The `season` column in `dw.dim_date` can also be used (e.g., `WHERE d.season = 'Spring'`).
    - The date column is named `full_date` (not `date`). Use this column for date operations: `DATE_TRUNC('week', d.full_date)`.
8.  **EXTRACT Functions & GROUP BY:** 
    - When using EXTRACT() for dates in SELECT, you MUST either:
      a) Use the exact same EXTRACT expression in the GROUP BY clause. For example, if you have `SELECT EXTRACT(year FROM d.full_date) AS year`, then use `GROUP BY EXTRACT(year FROM d.full_date)`.
      b) Or use d.year, d.month columns directly from dim_date instead of EXTRACT if available.
    - DO NOT use column aliases in the GROUP BY clause when working with EXTRACT or other expressions.
9.  **Aggregation:** Use appropriate aggregate functions (SUM, AVG, COUNT, MAX, MIN).
10. **Clarity:** Alias tables (e.g., `FROM dw.fact_visitor f JOIN dw.dim_date d ON ...`) for readability.
11. **Efficiency:** Only select the necessary columns. If asking for a total or count, don't select individual rows unless needed.
12. **Limit Results:** Unless the user asks for all data, add `LIMIT 10` or a similar reasonable limit to prevent excessively large results.
13. **PostgreSQL Dialect:** Ensure the query uses valid PostgreSQL syntax.
14. **No DML:** NEVER generate INSERT, UPDATE, DELETE, or DROP statements.
15. **Output:** Return ONLY the SQL query, nothing else. Start with `WITH` or `SELECT`.
"""

class SQLGenerationService:
    # Note: Removed dw_db from init as it's not used directly for generation
    def __init__(self, llm_adapter: OpenAIAdapter, debug_service: Optional[DebugService] = None):
        """Initialize SQLGenerationService with LLM adapter and optional debug service"""
        self.llm_adapter = llm_adapter
        self.debug_service = debug_service
        logger.info("SQLGenerationService initialized successfully")
    
    async def generate_query(self, user_question: str, live_schema_string: str, dw_context: dict = None) -> str:
        """
        Generate a SQL query based on the user's question.
        
        Args:
            user_question: The user's natural language question
            live_schema_string: The database schema information
            dw_context: Optional context about the data warehouse
            
        Returns:
            A SQL query string
        """
        if self.debug_service:
            self.debug_service.start_step("sql_generation_llm", details={
                "query_text": user_question,
                "schema_context_provided": live_schema_string is not None,
                "dw_context_provided": dw_context is not None
            })
        
        try:
            # Develop prompt for OpenAI with schema and user question
            prompt = self._build_sql_prompt(user_question, live_schema_string, dw_context)
            
            # Get raw response from OpenAI
            raw_response = await self.llm_adapter.agenerate_text(prompt)
            
            # Check if the response indicates an API error
            if raw_response.startswith("Error:"):
                logger.error(f"LLM API error: {raw_response}")
                if self.debug_service:
                    self.debug_service.add_step_details({
                        "api_error": raw_response
                    })
                    self.debug_service.end_step("sql_generation_llm", success=False, error=raw_response)
                return raw_response
            
            # Log raw output for debugging
            if self.debug_service:
                self.debug_service.add_step_details({
                    "llm_raw_output": raw_response
                })
            
            # Extract SQL query from response
            sql_query = self._extract_sql_from_response(raw_response)
            
            # Fix common SQL errors
            sql_query = self._fix_common_sql_errors(sql_query)
            
            # Log extracted SQL
            if self.debug_service:
                self.debug_service.add_step_details({
                    "extracted_sql": sql_query
                })
            
            # Validate with schema if possible
            # if live_schema_string:
            #     sql_query = self._validate_and_fix_sql(sql_query, live_schema_string)
            
            logger.info(f"LLM generated SQL query: {sql_query}")
            
            if self.debug_service:
                self.debug_service.end_step("sql_generation_llm", success=True)
            
            return sql_query
            
        except Exception as e:
            logger.error(f"Error generating SQL query: {str(e)}")
            if self.debug_service:
                self.debug_service.end_step("sql_generation_llm", success=False, error=str(e))
            raise

    def _fix_common_sql_errors(self, sql_query: str) -> str:
        """Fix common SQL errors that appear in generated queries"""
        if not sql_query:
            return sql_query
            
        # Fix for the week_of_year column issue
        fixed_query = sql_query
        
        # Replace d.week_of_year with EXTRACT(WEEK FROM d.full_date)
        if "d.week_of_year" in fixed_query:
            logger.info("Fixing d.week_of_year reference")
            fixed_query = fixed_query.replace("d.week_of_year", "EXTRACT(WEEK FROM d.full_date) AS week_of_year")
            
            # Make sure any GROUP BY d.week_of_year is also updated
            fixed_query = fixed_query.replace("GROUP BY d.week_of_year", "GROUP BY EXTRACT(WEEK FROM d.full_date)")
            
            # Make sure any ORDER BY d.week_of_year is also updated
            fixed_query = fixed_query.replace("ORDER BY d.week_of_year", "ORDER BY EXTRACT(WEEK FROM d.full_date)")
            
        # Replace any other table aliases with proper column references
        if "spring_visitors" in fixed_query:
            # Fix common issue with CTE column references 
            if "week_of_year, total_visitors" in fixed_query:
                fixed_query = fixed_query.replace(
                    "week_of_year, total_visitors", 
                    "spring_visitors.week_of_year, spring_visitors.total_visitors"
                )
        
        # Fix EXTRACT GROUP BY issues
        if "EXTRACT" in fixed_query and "GROUP BY" in fixed_query:
            logger.info("Checking for EXTRACT GROUP BY issues")
            
            # Look for patterns like: SELECT EXTRACT(...) AS alias ... GROUP BY alias
            extract_patterns = re.finditer(r'EXTRACT\s*\(\s*(\w+)\s+FROM\s+([^\)]+)\)\s+AS\s+(\w+)', fixed_query, re.IGNORECASE)
            
            for match in extract_patterns:
                extract_func = match.group(1)  # year, month, etc.
                source_col = match.group(2)    # d.full_date
                alias = match.group(3)         # year, month, etc.
                
                # Look for GROUP BY with just the alias
                group_by_alias_pattern = rf'GROUP BY\s+{alias}(,|\s|$)'
                
                if re.search(group_by_alias_pattern, fixed_query, re.IGNORECASE):
                    logger.info(f"Found GROUP BY with alias '{alias}' instead of full EXTRACT expression")
                    
                    # Replace GROUP BY alias with the full EXTRACT expression
                    full_extract = f"EXTRACT({extract_func} FROM {source_col})"
                    fixed_query = re.sub(
                        group_by_alias_pattern,
                        f"GROUP BY {full_extract}\\1",
                        fixed_query
                    )
                    logger.info(f"Fixed GROUP BY clause to use full EXTRACT expression: {full_extract}")
        
        # Add more fixes for common errors as they are encountered
        
        return fixed_query

    def _build_sql_prompt(self, user_question: str, live_schema_string: str, dw_context: dict = None) -> str:
        """
        Build the prompt for SQL generation
        
        Args:
            user_question: The user's question
            live_schema_string: The database schema information
            dw_context: Additional context information
            
        Returns:
            A formatted prompt string
        """
        # Start with the system prompt
        full_prompt = SQL_GENERATION_SYSTEM_PROMPT
        
        # Add schema information
        full_prompt += f"\n\n--- LIVE SCHEMA (dw) ---\n\n{live_schema_string}\n\n"
        
        # Add DW context if available
        if dw_context:
            full_prompt += f"--- DW CONTEXT ---\n{json.dumps(dw_context, indent=2)}\n\n"
        
        # Add the user question
        full_prompt += f"\nUser Question: {user_question}\n\nGenerate the PostgreSQL query:"
        
        return full_prompt
    
    def _extract_sql_from_response(self, response: str) -> str:
        """
        Extract SQL query from the LLM response
        
        Args:
            response: The raw response from the LLM
            
        Returns:
            The extracted SQL query
        """
        if not response:
            return ""
            
        # Try to extract SQL from markdown code blocks
        sql_match = re.search(r'```(?:sql)?(.*?)```', response, re.DOTALL)
        if sql_match:
            sql_query = sql_match.group(1).strip()
            return sql_query
        
        # If no code block found, look for SELECT or WITH statements
        query_match = re.search(r'(WITH|SELECT).*?;', response, re.DOTALL | re.IGNORECASE)
        if query_match:
            return query_match.group(0).strip()
        
        # If all else fails, just return the response
        return response.strip()

    # Removed _extract_query_intent, _generate_busiest_period_query,
    # _generate_spending_query, _generate_visitor_query, _generate_general_query
    # as the LLM is now responsible for generation based on context.

    # Removed execute_query method as execution happens in ChatService.

    # Removed _extract_query_intent, _generate_busiest_period_query,
    # _generate_spending_query, _generate_visitor_query, _generate_general_query
    # as the LLM is now responsible for generation based on context.

    # Removed execute_query method as execution happens in ChatService. 