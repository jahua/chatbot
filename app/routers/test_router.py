from fastapi import APIRouter, Depends, HTTPException
import logging
import traceback
import pandas as pd
import plotly.express as px
from sqlalchemy.orm import Session
from app.db.database import get_dw_db

# Set up logging
logger = logging.getLogger(__name__)

# Create the router
router = APIRouter(
    prefix="/test",
    tags=["test"],
    responses={404: {"description": "Not found"}},
)

@router.get("/visualization")
async def test_visualization(db: Session = Depends(get_dw_db)):
    """
    Test endpoint that creates a visualization of Swiss vs International tourists for 2023.
    This is a simple endpoint for testing visualization capabilities.
    """
    try:
        logger.info("Running test visualization")
        
        # Sample SQL query to get Swiss and international tourists by month for 2023
        query = """
        SELECT 
            d.year,
            d.month,
            d.month_name,
            SUM(fv.swiss_tourists) as swiss_tourists,
            SUM(fv.foreign_tourists) as foreign_tourists
        FROM dw.fact_visitor fv
        JOIN dw.dim_date d ON fv.date_id = d.date_id
        WHERE d.year = 2023
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.month
        """
        
        # Execute the query
        result = db.execute(query)
        rows = result.fetchall()
        
        # Check if we got any data
        if not rows:
            logger.warning("No data returned for test visualization")
            return {"message": "No data available"}
            
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        
        # Create a visualization
        fig = px.bar(
            df, 
            x="month_name", 
            y=["swiss_tourists", "foreign_tourists"],
            title="Swiss vs International Tourists by Month (2023)",
            labels={"value": "Number of Tourists", "month_name": "Month"},
            barmode="group"
        )
        
        # Return JSON representation of the figure
        return {
            "visualization": fig.to_json(),
            "status": "success",
            "message": "Test visualization created successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating test visualization: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Error creating visualization: {str(e)}"
        ) 