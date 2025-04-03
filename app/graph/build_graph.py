from typing import Dict, Any
from langgraph.graph import StateGraph
from app.graph.sql_node import SQLNode
from app.graph.query_executor import QueryExecutorNode
from app.graph.analysis_node import AnalysisNode
from app.llm.openai_adapter import OpenAIAdapter
from app.db.database import DatabaseService
import logging

logger = logging.getLogger(__name__)

def should_continue(state: Dict[str, Any]) -> str:
    """Determine next node based on state"""
    if state.get("error"):
        return "end"
    if state.get("execution_error"):
        return "end"
    if state.get("analysis_error"):
        return "end"
    return "continue"

def build_tourism_graph(llm: OpenAIAdapter, db: DatabaseService) -> StateGraph:
    """Build the tourism analysis graph"""
    
    # Create graph
    workflow = StateGraph()

    # Add nodes
    workflow.add_node("sql_generation", SQLNode(llm))
    workflow.add_node("query_execution", QueryExecutorNode(db))
    workflow.add_node("analysis", AnalysisNode(llm))

    # Set entry point
    workflow.set_entry_point("sql_generation")

    # Add edges
    workflow.add_edge("sql_generation", "query_execution")
    workflow.add_edge("query_execution", "analysis")

    # Add conditional edges
    workflow.add_conditional_edges(
        "sql_generation",
        should_continue,
        {
            "continue": "query_execution",
            "end": "end"
        }
    )
    
    workflow.add_conditional_edges(
        "query_execution",
        should_continue,
        {
            "continue": "analysis",
            "end": "end"
        }
    )

    workflow.add_conditional_edges(
        "analysis",
        should_continue,
        {
            "continue": "end",
            "end": "end"
        }
    )

    # Compile graph
    app = workflow.compile()
    
    return app 