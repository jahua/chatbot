from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.postgres import get_db, get_schema_info
from app.llm.claude_adapter import claude_adapter
from typing import Dict, Any

router = APIRouter()

@router.post("/message")
async def chat_message(
    message: Dict[str, str],
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Process a chat message and return SQL query results
    """
    try:
        # Get database schema information
        schema_info = get_schema_info(db)
        
        # Process the query using Claude
        result = await claude_adapter.process_query(
            schema_context=str(schema_info),
            user_query=message["message"]
        )
        
        # Execute the SQL query
        query_result = db.execute(result["sql_query"]).fetchall()
        
        # Generate natural language response
        response = await claude_adapter.generate_response(
            sql_query=result["sql_query"],
            query_result=query_result,
            user_query=message["message"]
        )
        
        return {
            "sql_query": result["sql_query"],
            "query_result": query_result,
            "response": response
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing query: {str(e)}"
        ) 