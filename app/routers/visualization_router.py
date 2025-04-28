from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import json
from typing import Dict, Any, List, Optional

from app.db.database import get_db

router = APIRouter(prefix="/visualization", tags=["visualization"])

@router.get("/swiss_foreign_monthly")
async def visualize_swiss_foreign_monthly(year: int = 2023, db: Session = Depends(get_db)):
    """
    Generate a bar chart visualization comparing Swiss tourists and foreign tourists per month.
    Uses the RAG framework to retrieve data from the database.
    """
    try:
        # Execute SQL query to get monthly data for Swiss and foreign tourists
        query = text(f"""
            SELECT 
                d.month,
                d.month_name,
                SUM(f.swiss_tourists) as swiss_tourists,
                SUM(f.foreign_tourists) as foreign_tourists
            FROM dw.fact_visitor f
            JOIN dw.dim_date d ON f.date_id = d.date_id
            WHERE d.year = {year}
            GROUP BY d.month, d.month_name
            ORDER BY d.month
        """)
        
        result = db.execute(query).fetchall()
        
        if not result:
            raise HTTPException(status_code=404, detail="No tourist data found for the specified year")
        
        # Convert query result to DataFrame for easier manipulation
        df = pd.DataFrame(result)
        
        # Create a stacked bar chart with Plotly
        fig = go.Figure()
        
        # Add bars for Swiss tourists
        fig.add_trace(go.Bar(
            x=df['month_name'],
            y=df['swiss_tourists'],
            name='Swiss Tourists',
            marker_color='#1E88E5'
        ))
        
        # Add bars for foreign tourists
        fig.add_trace(go.Bar(
            x=df['month_name'],
            y=df['foreign_tourists'],
            name='International Tourists',
            marker_color='#D81B60'
        ))
        
        # Update layout with better styling
        fig.update_layout(
            title=f'Swiss and International Tourists by Month ({year})',
            xaxis_title='Month',
            yaxis_title='Number of Tourists',
            barmode='group',
            template='plotly_dark',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=50, r=50, t=60, b=50),
            height=500,
            width=900
        )
        
        # Add annotations for total values
        for i, row in df.iterrows():
            fig.add_annotation(
                x=row['month_name'],
                y=row['swiss_tourists'],
                text=f"{int(row['swiss_tourists']):,}",
                showarrow=False,
                yshift=10,
                font=dict(color="#1E88E5")
            )
            fig.add_annotation(
                x=row['month_name'],
                y=row['foreign_tourists'],
                text=f"{int(row['foreign_tourists']):,}",
                showarrow=False,
                yshift=10,
                font=dict(color="#D81B60")
            )
        
        # Return the chart as JSON
        return {"chart": json.loads(fig.to_json())}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating visualization: {str(e)}") 