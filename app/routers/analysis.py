from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional
import logging
from ..utils.sql_generator import SQLGenerator
from ..utils.db_utils import execute_query
from ..utils.analysis_generator import generate_analysis_summary

logger = logging.getLogger(__name__)
router = APIRouter()

class AnalysisRequest(BaseModel):
    message: str

class AnalysisResponse(BaseModel):
    sql_query: str
    data: list
    analysis: str
    metadata: Dict[str, Any]

@router.post("/analyze", response_model=AnalysisResponse)
async def analyze_data(request: AnalysisRequest) -> AnalysisResponse:
    """
    Analyze data based on user message using intelligent routing
    """
    try:
        # Initialize SQL generator
        sql_generator = SQLGenerator()
        
        # Generate and validate SQL query
        query_info = sql_generator.generate_sql_query(request.message)
        validated_info = sql_generator.validate_query(query_info)
        
        if "error" in validated_info:
            raise HTTPException(status_code=400, detail=validated_info["error"])
        
        # Format the SQL query
        formatted_query = sql_generator.format_query(validated_info["query"])
        
        # Execute query
        result = await execute_query(formatted_query)
        
        if not result:
            raise HTTPException(status_code=404, detail="No data found for the given criteria")
        
        # Generate analysis summary
        analysis = generate_analysis_summary(result, validated_info["intent"])
        
        # Return response with query, data, analysis and metadata
        return AnalysisResponse(
            sql_query=formatted_query,
            data=result,
            analysis=analysis,
            metadata={
                "intent": validated_info["intent"].value if validated_info["intent"] else None,
                "time_range": validated_info["time_range"],
                "granularity": validated_info["granularity"].value if validated_info["granularity"] else None,
                "comparison_type": validated_info["comparison_type"],
                "table_info": validated_info["table_info"]
            }
        )
        
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error processing analysis request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 