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
7.  **Date Filtering:** Use the `dw.dim_date` table for filtering by year, month, day, season, etc. Example: `JOIN dw.dim_date d ON f.date_id = d.date_id WHERE d.year = 2023 AND d.month BETWEEN 3 AND 5` for Spring 2023. The `season` column in `dw.dim_date` can also be used (e.g., `WHERE d.season = 'Spring'`).
8.  **Aggregation:** Use appropriate aggregate functions (SUM, AVG, COUNT, MAX, MIN).
9.  **Clarity:** Alias tables (e.g., `FROM dw.fact_visitor f JOIN dw.dim_date d ON ...`) for readability.
10. **Efficiency:** Only select the necessary columns. If asking for a total or count, don't select individual rows unless needed.
11. **Limit Results:** Unless the user asks for all data, add `LIMIT 10` or a similar reasonable limit to prevent excessively large results.
12. **PostgreSQL Dialect:** Ensure the query uses valid PostgreSQL syntax.
13. **No DML:** NEVER generate INSERT, UPDATE, DELETE, or DROP statements.
14. **Output:** Return ONLY the SQL query, nothing else. Start with `WITH` or `SELECT`.
"""

class SQLGenerationService:
    # Note: Removed dw_db from init as it's not used directly for generation
    def __init__(self, llm_adapter: OpenAIAdapter, debug_service: Optional[DebugService] = None):
        """Initialize SQLGenerationService with LLM adapter and optional debug service"""
        self.llm_adapter = llm_adapter
        self.debug_service = debug_service
        logger.info("SQLGenerationService initialized successfully")
    
    async def generate_query(self, query_text: str, schema_context: Dict[str, Any]) -> str:
        """Generate SQL query using LLM based on natural language query and combined schema context."""
        if self.debug_service:
            self.debug_service.start_step("sql_generation_llm", {
                "query_text": query_text,
                "schema_context_keys": list(schema_context.keys())
            })
        
        sql_query = ""
        try:
            live_schema = schema_context.get("live_schema_string", "")
            dw_context_info = schema_context.get("dw_context", {})

            if not live_schema:
                raise ValueError("Live schema context is missing or empty.")

            # Prepare the context string for the prompt
            context_for_prompt = f"\n--- LIVE SCHEMA (dw) ---\n{live_schema}\n"
            context_for_prompt += f"\n--- DW CONTEXT ---\n{json.dumps(dw_context_info, indent=2)}\n"

            # Prepare messages for the LLM (Now formatting as a single string)
            # Combine system prompt, context, and user question into a single prompt string
            full_prompt_string = f"{SQL_GENERATION_SYSTEM_PROMPT}\n\n{context_for_prompt}\n\nUser Question: {query_text}\n\nGenerate the PostgreSQL query:"

            # Call the LLM adapter using agenerate_text
            # Assuming llm_adapter has a method like 'agenerate_text'
            generated_text = await self.llm_adapter.agenerate_text(full_prompt_string)
            
            if not generated_text:
                raise ValueError("LLM failed to generate SQL query text.")

            # Basic extraction/cleaning (might need refinement)
            # Look for SELECT or WITH statement, potentially strip markdown
            match = re.search(r'(WITH|SELECT).*;', generated_text, re.IGNORECASE | re.DOTALL)
            if match:
                sql_query = match.group(0).strip()
                # Remove potential markdown backticks
                sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
            else:
                 # Fallback: assume the whole response might be the query if simple extraction fails
                 sql_query = generated_text.strip().replace('```sql', '').replace('```', '').strip()
                 if not (sql_query.upper().startswith("SELECT") or sql_query.upper().startswith("WITH")):
                      raise ValueError(f"LLM generated invalid SQL start: {sql_query[:100]}...")

            if self.debug_service:
                self.debug_service.add_step_details({
                    "llm_raw_output": generated_text,
                    "extracted_sql": sql_query
                })
            
            logger.info(f"LLM generated SQL query: {sql_query}")
            return sql_query

        except Exception as e:
            logger.error(f"Error generating SQL query via LLM: {str(e)}", exc_info=True)
            if self.debug_service:
                self.debug_service.add_step_details({"error": str(e)})
                # Mark step as failed only if we re-raise or return an error indicator
                # self.debug_service.end_step(error=e) # Keep step open if we return empty/fallback
            # Depending on desired behavior, either re-raise, return None, or return empty string
            raise # Re-raise the exception to be caught by ChatService

    # Removed _extract_query_intent, _generate_busiest_period_query,
    # _generate_spending_query, _generate_visitor_query, _generate_general_query
    # as the LLM is now responsible for generation based on context.

    # Removed execute_query method as execution happens in ChatService.

    # Removed _extract_query_intent, _generate_busiest_period_query,
    # _generate_spending_query, _generate_visitor_query, _generate_general_query
    # as the LLM is now responsible for generation based on context.

    # Removed execute_query method as execution happens in ChatService. 