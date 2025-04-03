from typing import Dict, Any
from langchain.schema.runnable import Runnable
from langchain.prompts import PromptTemplate
from app.llm.openai_adapter import OpenAIAdapter
import logging

logger = logging.getLogger(__name__)

class SQLNode(Runnable):
    def __init__(self, llm: OpenAIAdapter):
        self.llm = llm
        self.prompt = PromptTemplate(
            template="""Given the following question about tourism data, generate an appropriate SQL query.
            The database has tables:
            - aoi_days_raw (columns: aoi_date, visitors JSONB with swissTourist and foreignTourist)
            - master_card (columns: txn_date, industry, txn_amt, txn_cnt, segment, geo_type, geo_name)

            Question: {question}

            Generate only the SQL query, no explanation:""",
            input_variables=["question"]
        )

    def invoke(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate SQL query from natural language question"""
        try:
            question = input_data.get("question")
            if not question:
                raise ValueError("No question provided in input")

            # Generate SQL query
            sql_query = self.llm.generate_sql(self.prompt.format(question=question))
            logger.info(f"Generated SQL query: {sql_query}")

            return {
                "sql_query": sql_query,
                "original_question": question,
                "error": None
            }

        except Exception as e:
            logger.error(f"Error in SQL generation: {str(e)}")
            return {
                "sql_query": None,
                "original_question": question,
                "error": str(e)
            } 