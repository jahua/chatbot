from typing import Dict, Any
from langchain.schema.runnable import Runnable
from langchain.prompts import PromptTemplate
from app.llm.openai_adapter import OpenAIAdapter
import logging
import json

logger = logging.getLogger(__name__)

class AnalysisNode(Runnable):
    def __init__(self, llm: OpenAIAdapter):
        self.llm = llm
        self.prompt = PromptTemplate(
            template="""Analyze the following tourism data results and provide insights.
            
            Original Question: {question}
            SQL Query Used: {sql_query}
            Data Results: {results}
            
            Provide a clear, concise analysis focusing on key trends and insights.
            Format your response in markdown with appropriate headers and bullet points.""",
            input_variables=["question", "sql_query", "results"]
        )

    def invoke(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate analysis from query results"""
        try:
            question = input_data.get("original_question")
            sql_query = input_data.get("sql_query")
            results = input_data.get("query_results")

            if not all([question, sql_query, results]):
                raise ValueError("Missing required input data")

            # Convert results to string format for prompt
            results_str = json.dumps(results, indent=2)

            # Generate analysis
            analysis = self.llm.generate_analysis(
                self.prompt.format(
                    question=question,
                    sql_query=sql_query,
                    results=results_str
                )
            )

            logger.info("Analysis generated successfully")

            return {
                **input_data,  # Pass through previous data
                "analysis": analysis,
                "analysis_error": None
            }

        except Exception as e:
            logger.error(f"Error generating analysis: {str(e)}")
            return {
                **input_data,
                "analysis": None,
                "analysis_error": str(e)
            } 