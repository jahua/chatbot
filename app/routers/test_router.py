from fastapi import APIRouter, Depends, HTTPException
import logging
import traceback
import pandas as pd
import plotly.express as px
from sqlalchemy.orm import Session
from app.db.database import get_dw_db
from fastapi.responses import JSONResponse
from sqlalchemy.sql import text

# Set up logging
logger = logging.getLogger(__name__)

# Create the router
router = APIRouter(
    prefix="/test",
    tags=["test"],
    responses={404: {"description": "Not found"}},
)

@router.get("/visualization")
async def get_visualization(db: Session = Depends(get_dw_db)):
    """Generate a visualization of Swiss and foreign tourists."""
    try:
        logger.info("Generating visualization...")
        
        # Execute SQL query to get data for the visualization
        query = """
        SELECT 
            d.year,
            d.month,
            d.month_name, 
            SUM(fv.swiss_tourists) as swiss_tourists,
            SUM(fv.foreign_tourists) as foreign_tourists
        FROM dw.fact_visitor fv
        JOIN dw.dim_date d ON fv.date_id = d.date_id
        GROUP BY d.year, d.month, d.month_name
        ORDER BY d.year, d.month
        """
        
        result = db.execute(text(query))
        rows = result.fetchall()
        
        if not rows:
            logger.warning("No data returned from query")
            return {"error": "No data available"}
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(rows)
        
        # Determine year range for title
        year_min = df['year'].min()
        year_max = df['year'].max()
        year_range = f"{year_min}" if year_min == year_max else f"{year_min}-{year_max}"
        
        # Create visualization
        fig = px.bar(
            df, 
            x='month_name', 
            y=['swiss_tourists', 'foreign_tourists'],
            barmode='group',
            title=f'Swiss and International Tourists by Month ({year_range})',
            labels={
                'month_name': 'Month',
                'value': 'Number of Tourists',
                'variable': 'Tourist Type'
            },
            color_discrete_map={
                'swiss_tourists': '#1E88E5',
                'foreign_tourists': '#D81B60'
            },
            custom_data=['year']  # Include year in hover data
        )
        
        # Update layout
        fig.update_layout(
            legend_title="Tourist Origin",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            hovermode="x unified",
            xaxis=dict(
                categoryorder='array',
                categoryarray=[
                    'January', 'February', 'March', 'April', 'May', 'June',
                    'July', 'August', 'September', 'October', 'November', 'December'
                ]
            )
        )
        
        # Update hover template to show year
        fig.update_traces(
            hovertemplate="<b>%{y:,.0f}</b> tourists<br>Month: %{x}<br>Year: %{customdata[0]}"
        )
        
        return {"visualization": fig.to_json()}
    
    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"detail": f"Error creating visualization: {str(e)}"}
        ) 