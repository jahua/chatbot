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
    - **IMPORTANT:** For date-based reporting, prefer using the existing columns in the dim_date table instead of EXTRACT functions:
      ```sql
      -- PREFERRED APPROACH - Using dim_date table columns
      SELECT d.year, d.month, d.month_name,
             SUM(fv.total_visitors) AS total_visitors
      FROM dw.fact_visitor fv
      JOIN dw.dim_date d ON fv.date_id = d.date_id
      GROUP BY d.year, d.month, d.month_name
      ORDER BY d.year, d.month
      ```
    - If you must use EXTRACT(), you MUST include the entire EXTRACT expression in the GROUP BY clause:
      ```sql
      -- CORRECT EXTRACT USAGE
      SELECT EXTRACT(year FROM d.full_date) AS year, 
             EXTRACT(month FROM d.full_date) AS month,
             SUM(fv.total_visitors) AS total_visitors
      FROM dw.fact_visitor fv
      JOIN dw.dim_date d ON fv.date_id = d.date_id
      GROUP BY EXTRACT(year FROM d.full_date), EXTRACT(month FROM d.full_date)
      ORDER BY EXTRACT(year FROM d.full_date), EXTRACT(month FROM d.full_date)
      ```
    - NEVER group by column aliases in PostgreSQL:
      ```sql
      -- INCORRECT - DON'T DO THIS
      SELECT EXTRACT(year FROM d.full_date) AS year, 
             EXTRACT(month FROM d.full_date) AS month,
             SUM(fv.total_visitors) AS total_visitors
      FROM dw.fact_visitor fv
      JOIN dw.dim_date d ON fv.date_id = d.date_id
      GROUP BY year, month
      ```
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
                "schema_context_keys": list(filter(None, ["live_schema_string" if live_schema_string else None, "dw_context" if dw_context else None]))
            })
        
        try:
            # Develop prompt for OpenAI with schema and user question
            prompt = self._build_sql_prompt(user_question, live_schema_string, dw_context)
            
            # Get raw response from OpenAI
            raw_response = await self.llm_adapter.agenerate_text(prompt)
            
            # Check if the response indicates an API error
            if raw_response.startswith("Error:") or raw_response.startswith("Request error occurred:"):
                logger.error(f"LLM API error: {raw_response}")
                if self.debug_service:
                    self.debug_service.add_step_details({
                        "llm_raw_output": raw_response,
                        "api_error": raw_response
                    })
                    self.debug_service.end_step("sql_generation_llm", success=False, error=raw_response)
                
                # Return a default fallback SQL instead of the error message
                fallback_sql = self._get_fallback_sql(user_question)
                logger.info(f"API error detected, using fallback SQL: {fallback_sql}")
                return fallback_sql
            
            # Log raw output for debugging
            if self.debug_service:
                self.debug_service.add_step_details({
                    "llm_raw_output": raw_response
                })
            
            # Extract SQL query from response
            sql_query = self._extract_sql_from_response(raw_response)
            
            # If SQL extraction failed, use a fallback
            if not sql_query or sql_query.strip() == "":
                logger.warning("Failed to extract valid SQL from LLM response")
                fallback_sql = self._get_fallback_sql(user_question)
                logger.info(f"Using fallback SQL: {fallback_sql}")
                
                if self.debug_service:
                    self.debug_service.add_step_details({
                        "extraction_failed": True,
                        "extracted_sql": fallback_sql
                    })
                
                return fallback_sql
            
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
            
            # Return fallback SQL on any error
            fallback_sql = self._get_fallback_sql(user_question)
            logger.info(f"Exception occurred, using fallback SQL: {fallback_sql}")
            return fallback_sql
            
    def _get_fallback_sql(self, user_question: str) -> str:
        """Generate a fallback SQL query when LLM generation fails"""
        # Extract year from user question or default to current year
        year_match = re.search(r'\b20\d{2}\b', user_question)
        target_year = year_match.group(0) if year_match else '2023'
        
        # Check for Swiss vs international tourist comparison request
        if (("swiss" in user_question.lower() and "international" in user_question.lower() 
             or "swiss" in user_question.lower() and "foreign" in user_question.lower())
            and "tourist" in user_question.lower() and "month" in user_question.lower()):
            
            # Special case for Swiss vs international tourists by month (optimized for bar chart)
            return f"""
            SELECT 
                d.year,
                d.month,
                d.month_name, 
                SUM(fv.swiss_tourists) as swiss_tourists,
                SUM(fv.foreign_tourists) as foreign_tourists
            FROM dw.fact_visitor fv
            JOIN dw.dim_date d ON fv.date_id = d.date_id
            WHERE d.year = {target_year}
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.month
            """
        
        # Simple fallback that should work for most cases
        if "industry" in user_question.lower() and "spending" in user_question.lower():
            return f"""
            SELECT i.industry_name, SUM(fs.total_amount) as total_spending
            FROM dw.fact_spending fs
            JOIN dw.dim_industry i ON fs.industry_id = i.industry_id
            JOIN dw.dim_date d ON fs.date_id = d.date_id
            WHERE d.year = {target_year}
            GROUP BY i.industry_name
            ORDER BY total_spending DESC
            LIMIT 10
            """
        elif "spending" in user_question.lower() or "amount" in user_question.lower():
            return f"""
            SELECT d.year, d.month, d.month_name, r.region_name, 
                   SUM(fs.total_amount) as total_spending
            FROM dw.fact_spending fs
            JOIN dw.dim_date d ON fs.date_id = d.date_id
            JOIN dw.dim_region r ON fs.region_id = r.region_id
            WHERE d.year = {target_year}
            GROUP BY d.year, d.month, d.month_name, r.region_name
            ORDER BY d.year, d.month, total_spending DESC
            LIMIT 20
            """
        elif "visitor" in user_question.lower() or "tourist" in user_question.lower():
            return f"""
            SELECT d.full_date, d.year, d.month, d.month_name, r.region_name, 
                   SUM(fv.total_visitors) as total_visitors
            FROM dw.fact_visitor fv
            JOIN dw.dim_date d ON fv.date_id = d.date_id
            JOIN dw.dim_region r ON fv.region_id = r.region_id
            WHERE d.year = {target_year}
            GROUP BY d.full_date, d.year, d.month, d.month_name, r.region_name
            ORDER BY total_visitors DESC
            LIMIT 10
            """
        else:
            # Most generic fallback
            return f"""
            SELECT d.year, d.month, d.month_name, r.region_name, 
                   SUM(fs.total_amount) as total_spending
            FROM dw.fact_spending fs
            JOIN dw.dim_date d ON fs.date_id = d.date_id
            JOIN dw.dim_region r ON fs.region_id = r.region_id
            WHERE d.year = {target_year}
            GROUP BY d.year, d.month, d.month_name, r.region_name
            ORDER BY d.year, d.month, total_spending DESC
            LIMIT 20
            """

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
        
        # Fix EXTRACT GROUP BY issues - part 1: replace aliases with full expressions in GROUP BY
        if "EXTRACT" in fixed_query and "GROUP BY" in fixed_query:
            logger.info("Checking for GROUP BY issues with EXTRACT")
            
            # First, find all extract expressions in the SELECT clause
            select_extract_patterns = re.finditer(r'EXTRACT\s*\(\s*(\w+)\s+FROM\s+([^\)]+)\)\s+AS\s+(\w+)', fixed_query, re.IGNORECASE)
            
            # Store each alias and its corresponding EXTRACT expression
            extract_aliases = {}
            for match in select_extract_patterns:
                extract_func = match.group(1)  # year, month, etc.
                source_col = match.group(2)    # d.full_date
                alias = match.group(3)         # year, month, etc.
                extract_aliases[alias] = f"EXTRACT({extract_func} FROM {source_col})"
            
            # Now find the GROUP BY clause
            group_by_match = re.search(r'GROUP BY\s+(.*?)(?:ORDER BY|LIMIT|;|$)', fixed_query, re.IGNORECASE | re.DOTALL)
            
            if group_by_match and extract_aliases:
                group_by_clause = group_by_match.group(1).strip()
                original_group_by = "GROUP BY " + group_by_clause
                new_group_by_parts = []
                
                # Split the group by clause by commas
                group_by_parts = [part.strip() for part in group_by_clause.split(',')]
                
                for part in group_by_parts:
                    # If this part is an alias that should be an EXTRACT expression
                    if part in extract_aliases:
                        new_group_by_parts.append(extract_aliases[part])
                    else:
                        new_group_by_parts.append(part)
                
                # Create the new GROUP BY clause
                new_group_by = "GROUP BY " + ", ".join(new_group_by_parts)
                
                # Replace the old GROUP BY with the new one
                fixed_query = fixed_query.replace(original_group_by, new_group_by)
                logger.info(f"Fixed GROUP BY clause to use full EXTRACT expressions")
        
        # Special fix for month and year visualizations - convert to use existing dim_date columns
        if ("month" in fixed_query.lower() or "year" in fixed_query.lower()) and "EXTRACT" in fixed_query and "d.full_date" in fixed_query:
            # Check if we're trying to visualize by month/year
            is_visualization_query = (
                "month" in fixed_query.lower() and 
                ("bar chart" in fixed_query.lower() or 
                 "group by" in fixed_query.lower() or
                 "order by" in fixed_query.lower())
            )
            
            if is_visualization_query:
                logger.info("Detected visualization query with EXTRACT - attempting to optimize")
                # Try to replace EXTRACT(month FROM d.full_date) with d.month and d.month_name
                extract_month_pattern = r'EXTRACT\s*\(\s*month\s+FROM\s+d\.full_date\s*\)(?:\s+AS\s+\w+)?'
                extract_year_pattern = r'EXTRACT\s*\(\s*year\s+FROM\s+d\.full_date\s*\)(?:\s+AS\s+\w+)?'
                
                select_clause_match = re.search(r'SELECT\s+(.*?)(?:FROM|$)', fixed_query, re.IGNORECASE | re.DOTALL)
                
                if select_clause_match:
                    select_clause = select_clause_match.group(1)
                    new_select_clause = select_clause
                    
                    # Replace month extract with d.month, d.month_name
                    if re.search(extract_month_pattern, select_clause, re.IGNORECASE):
                        new_select_clause = re.sub(
                            extract_month_pattern, 
                            "d.month, d.month_name", 
                            new_select_clause,
                            flags=re.IGNORECASE
                        )
                        
                    # Replace year extract with d.year
                    if re.search(extract_year_pattern, select_clause, re.IGNORECASE):
                        new_select_clause = re.sub(
                            extract_year_pattern, 
                            "d.year", 
                            new_select_clause,
                            flags=re.IGNORECASE
                        )
                    
                    # Update the SELECT clause
                    fixed_query = fixed_query.replace(select_clause, new_select_clause)
                    
                    # Now update GROUP BY to match
                    group_by_match = re.search(r'GROUP BY\s+(.*?)(?:ORDER BY|LIMIT|;|$)', fixed_query, re.IGNORECASE | re.DOTALL)
                    
                    if group_by_match:
                        group_by_clause = group_by_match.group(1).strip()
                        new_group_by_clause = group_by_clause
                        
                        # Replace month extract in GROUP BY
                        if re.search(extract_month_pattern, group_by_clause, re.IGNORECASE):
                            new_group_by_clause = re.sub(
                                extract_month_pattern, 
                                "d.month, d.month_name", 
                                new_group_by_clause,
                                flags=re.IGNORECASE
                            )
                            
                        # Replace year extract in GROUP BY
                        if re.search(extract_year_pattern, group_by_clause, re.IGNORECASE):
                            new_group_by_clause = re.sub(
                                extract_year_pattern, 
                                "d.year", 
                                new_group_by_clause,
                                flags=re.IGNORECASE
                            )
                        
                        # Update the GROUP BY clause
                        fixed_query = fixed_query.replace(
                            f"GROUP BY {group_by_clause}", 
                            f"GROUP BY {new_group_by_clause}"
                        )
                        
                        logger.info("Optimized query to use d.month, d.month_name, and d.year instead of EXTRACT")

        # Fix EXTRACT GROUP BY issues - part 2: missing column in GROUP BY
        if "EXTRACT" in fixed_query:
            logger.info("Checking for missing columns in GROUP BY with EXTRACT")
            
            # Find all EXTRACT expressions in SELECT
            select_extracts = re.findall(r'EXTRACT\s*\(\s*(\w+)\s+FROM\s+([^\)]+)\)', fixed_query, re.IGNORECASE)
            extract_expressions = {f"EXTRACT({func} FROM {col})": (func, col) for func, col in select_extracts}
            
            # If we have GROUP BY and EXTRACT expressions
            if "GROUP BY" in fixed_query and extract_expressions:
                group_by_match = re.search(r'GROUP BY\s+(.*?)(?:ORDER BY|LIMIT|;|$)', fixed_query, re.IGNORECASE | re.DOTALL)
                
                if group_by_match:
                    group_by_clause = group_by_match.group(1).strip()
                    logger.info(f"Current GROUP BY clause: {group_by_clause}")
                    
                    missing_extracts = []
                    
                    # Check each EXTRACT expression
                    for extract_expr, (func, col) in extract_expressions.items():
                        # Skip if the expression starts with "EXTRACT(EXTRACT" to avoid nesting
                        if extract_expr.upper().startswith("EXTRACT(EXTRACT"):
                            continue
                            
                        # If the exact expression doesn't appear in GROUP BY
                        if extract_expr not in group_by_clause:
                            # Check if column is mentioned directly in GROUP BY
                            column_in_group_by = col in group_by_clause
                            
                            # If neither the EXTRACT nor the column is in GROUP BY
                            if not column_in_group_by:
                                logger.info(f"Column {col} from EXTRACT not found in GROUP BY, adding it")
                                missing_extracts.append(extract_expr)
                    
                    if missing_extracts:
                        # Add the missing EXTRACT expressions to GROUP BY
                        new_group_by = f"GROUP BY {group_by_clause}, {', '.join(missing_extracts)}"
                        fixed_query = fixed_query.replace(f"GROUP BY {group_by_clause}", new_group_by)
                        logger.info(f"Updated GROUP BY to: {new_group_by}")
        
        # Ensure d.full_date is in GROUP BY if EXTRACT is used with it
        if "d.full_date" in fixed_query and "EXTRACT" in fixed_query and "GROUP BY" in fixed_query:
            extract_from_date_matches = re.finditer(r'EXTRACT\s*\(\s*(\w+)\s+FROM\s+d\.full_date\)', fixed_query, re.IGNORECASE)
            
            extract_exprs = []
            for match in extract_from_date_matches:
                extract_exprs.append(match.group(0))
            
            if extract_exprs:
                group_by_match = re.search(r'GROUP BY\s+(.*?)(?:ORDER BY|LIMIT|;|$)', fixed_query, re.IGNORECASE | re.DOTALL)
                
                if group_by_match:
                    group_by_clause = group_by_match.group(1).strip()
                    missing_extracts = []
                    
                    for extract_expr in extract_exprs:
                        # Skip if already in GROUP BY to avoid adding duplicates
                        if extract_expr in group_by_clause:
                            continue
                            
                        # Skip if it would cause nesting
                        if extract_expr.upper().startswith("EXTRACT(EXTRACT"):
                            continue
                            
                        # If neither the extract nor d.full_date is in GROUP BY
                        if extract_expr not in group_by_clause and "d.full_date" not in group_by_clause:
                            missing_extracts.append(extract_expr)
                    
                    if missing_extracts:
                        # Add the extract expressions to GROUP BY
                        new_group_by = f"GROUP BY {group_by_clause}, {', '.join(missing_extracts)}"
                        fixed_query = fixed_query.replace(f"GROUP BY {group_by_clause}", new_group_by)
                        logger.info(f"Added missing d.full_date EXTRACT to GROUP BY: {new_group_by}")
        
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