from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.schemas.chat import ChatMessageCreate, ChatMessageResponse
from app.services.chat_service import ChatService
from app.agents.sql_agent import SQLAgent
from app.agents.visualization_agent import VisualizationAgent
from typing import Dict, Any
import json
import pandas as pd
from app.core.langsmith_config import get_traceable_decorator
import uuid
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/chat/message", response_model=ChatMessageResponse)
async def chat_message(
    request: Dict[str, Any],
    db: Session = Depends(get_db)
):
    try:
        # Log incoming request
        logger.debug(f"Received request: {request}")
        
        # Extract message data
        message = request.get("message", request.get("content", ""))
        model = request.get("model", "claude")
        session_id = request.get("session_id", str(uuid.uuid4()))
        
        if not message:
            raise HTTPException(status_code=400, detail="Message content is required")
        
        chat_service = ChatService(db)
        sql_agent = SQLAgent()
        visualization_agent = VisualizationAgent()
        
        # Process the message and get SQL query
        result = await chat_service.process_message(message, session_id)
        
        if not result["success"]:
            logger.error(f"Error processing message: {result['response']}")
            raise HTTPException(status_code=400, detail=result["response"])
        
        # Generate visualization if results are available
        visualization = None
        if result.get("data"):
            visualization = await visualization_agent.generate_visualization(result["data"])
        
        # Create chat message response
        response = {
            "content": result["response"],
            "role": "assistant",
            "model": model,
            "sql_query": result.get("sql_query"),
            "visualization": visualization,
            "data": result.get("data"),
            "created_at": datetime.utcnow().isoformat()
        }
        
        # Log response
        logger.debug(f"Sending response: {response}")
        return response
        
    except Exception as e:
        logger.error(f"Error in chat_message: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

def _determine_visualization_type(data: list, query: str) -> str:
    """Determine the best visualization type based on data and query"""
    if not data:
        return None
    
    # Convert data to DataFrame for analysis
    df = pd.DataFrame(data)
    
    # Check for time series data
    time_cols = [col for col in df.columns if any(time_term in col.lower() 
                for time_term in ['date', 'time', 'year', 'month', 'day'])]
    if time_cols:
        return "line"
    
    # Check for categorical data
    if len(df.columns) == 2 and df[df.columns[1]].dtype in ['int64', 'float64']:
        return "bar"
    
    # Check for percentage data
    if len(df.columns) == 2 and df[df.columns[1]].sum() == 100:
        return "pie"
    
    # Check for numerical correlation
    if len(df.columns) >= 2 and all(df[col].dtype in ['int64', 'float64'] for col in df.columns[:2]):
        return "scatter"
    
    # Default to bar chart
    return "bar"

def _get_viz_params(viz_type: str, data: list) -> Dict[str, Any]:
    """Get visualization parameters based on type and data"""
    df = pd.DataFrame(data)
    
    if viz_type == "line":
        time_col = next(col for col in df.columns if any(time_term in col.lower() 
                       for time_term in ['date', 'time', 'year', 'month', 'day']))
        value_col = next(col for col in df.columns if col != time_col)
        return {
            "x_col": time_col,
            "y_col": value_col,
            "title": f"Time Series: {value_col} over Time"
        }
    
    elif viz_type == "bar":
        return {
            "x_col": df.columns[0],
            "y_col": df.columns[1],
            "title": f"Bar Chart: {df.columns[1]} by {df.columns[0]}"
        }
    
    elif viz_type == "pie":
        return {
            "names_col": df.columns[0],
            "values_col": df.columns[1],
            "title": f"Distribution: {df.columns[1]} by {df.columns[0]}"
        }
    
    elif viz_type == "scatter":
        return {
            "x_col": df.columns[0],
            "y_col": df.columns[1],
            "color_col": df.columns[2] if len(df.columns) > 2 else None,
            "title": f"Scatter Plot: {df.columns[1]} vs {df.columns[0]}"
        }
    
    elif viz_type == "heatmap":
        return {
            "x_col": df.columns[0],
            "y_col": df.columns[1],
            "values_col": df.columns[2],
            "title": f"Heatmap: {df.columns[2]} by {df.columns[0]} and {df.columns[1]}"
        }
    
    return {} 