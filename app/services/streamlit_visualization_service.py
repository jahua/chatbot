import streamlit as st
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
import altair as alt

logger = logging.getLogger(__name__)

class StreamlitVisualizationService:
    """Service for creating Streamlit-specific visualizations"""
    
    def create_visualization(self, results: List[Dict[str, Any]], query: str) -> Optional[Any]:
        """Create a Streamlit visualization based on query results."""
        if not results or not isinstance(results, list) or not results:
            st.warning("No data available for visualization")
            return None
            
        try:
            # Convert results to DataFrame
            df = pd.DataFrame(results)
            
            # Special case: Handle single value results
            if len(df) == 1 and len(df.columns) == 1:
                return self._create_single_value_display(df)
            
            # Determine visualization type
            viz_type = self._determine_visualization_type(query, df)
            
            # Create visualization based on type
            return self._create_visualization_by_type(df, query, viz_type)
            
        except Exception as e:
            logger.error(f"Error creating Streamlit visualization: {str(e)}")
            st.error(f"Error creating visualization: {str(e)}")
            return None
    
    def _determine_visualization_type(self, query: str, data: pd.DataFrame) -> str:
        """Determine the most appropriate Streamlit visualization type."""
        query_lower = query.lower()
        
        # Get data characteristics
        numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
        date_cols = [col for col in data.columns if pd.api.types.is_datetime64_any_dtype(data[col])]
        
        # Check for explicit visualization requests
        if any(term in query_lower for term in ["pie chart", "percentage", "proportion"]):
            return "pie"
        
        if any(term in query_lower for term in ["map", "location", "geographic"]):
            return "map"
        
        if any(term in query_lower for term in ["trend", "over time", "evolution"]) and date_cols:
            return "line"
        
        if any(term in query_lower for term in ["compare", "comparison", "versus", "vs"]):
            return "bar"
        
        # Default to table for small datasets
        if len(data) <= 10:
            return "table"
            
        # Default to bar chart for categorical comparisons
        return "bar"
    
    def _create_visualization_by_type(self, df: pd.DataFrame, query: str, viz_type: str) -> None:
        """Create and display the appropriate Streamlit visualization."""
        if viz_type == "table":
            return self._create_table(df)
        elif viz_type == "bar":
            return self._create_bar_chart(df, query)
        elif viz_type == "line":
            return self._create_line_chart(df, query)
        elif viz_type == "pie":
            return self._create_pie_chart(df, query)
        else:
            return self._create_table(df)  # Fallback to table
    
    def _create_single_value_display(self, df: pd.DataFrame) -> None:
        """Display a single value with appropriate formatting."""
        value = df.iloc[0, 0]
        col_name = df.columns[0]
        
        # Create a metric display
        st.metric(label=col_name, value=value)
    
    def _create_table(self, df: pd.DataFrame) -> None:
        """Create an interactive table visualization."""
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True
        )
    
    def _create_bar_chart(self, df: pd.DataFrame, query: str) -> None:
        """Create an interactive bar chart using Altair."""
        # Identify numeric columns for y-axis
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if not numeric_cols:
            st.warning("No numeric columns available for bar chart")
            return self._create_table(df)
        
        # Use the first non-numeric column as x-axis
        categorical_cols = [col for col in df.columns if col not in numeric_cols]
        if not categorical_cols:
            st.warning("No categorical columns available for bar chart")
            return self._create_table(df)
        
        x_col = categorical_cols[0]
        y_col = numeric_cols[0]
        
        # Create Altair chart
        chart = alt.Chart(df).mark_bar().encode(
            x=alt.X(f'{x_col}:N', sort='-y'),
            y=f'{y_col}:Q',
            tooltip=[x_col, y_col]
        ).properties(
            title=self._extract_title_from_query(query)
        ).interactive()
        
        st.altair_chart(chart, use_container_width=True)
    
    def _create_line_chart(self, df: pd.DataFrame, query: str) -> None:
        """Create an interactive line chart using Altair."""
        # Identify date column
        date_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if not date_cols or not numeric_cols:
            st.warning("Required columns not found for line chart")
            return self._create_table(df)
        
        x_col = date_cols[0]
        y_col = numeric_cols[0]
        
        # Create Altair chart
        chart = alt.Chart(df).mark_line().encode(
            x=f'{x_col}:T',
            y=f'{y_col}:Q',
            tooltip=[x_col, y_col]
        ).properties(
            title=self._extract_title_from_query(query)
        ).interactive()
        
        st.altair_chart(chart, use_container_width=True)
    
    def _create_pie_chart(self, df: pd.DataFrame, query: str) -> None:
        """Create a pie chart using Streamlit's native plotting."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        categorical_cols = [col for col in df.columns if col not in numeric_cols]
        
        if not numeric_cols or not categorical_cols:
            st.warning("Required columns not found for pie chart")
            return self._create_table(df)
        
        fig = {
            'data': [{
                'labels': df[categorical_cols[0]],
                'values': df[numeric_cols[0]],
                'type': 'pie',
            }],
            'layout': {'title': self._extract_title_from_query(query)}
        }
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _extract_title_from_query(self, query: str) -> str:
        """Extract a title from the query."""
        # Remove common question words and clean up
        question_words = ['what', 'when', 'where', 'which', 'how', 'why', 'show', 'display', 'tell']
        title = query.strip('?!.')
        title = ' '.join(word for word in title.split() 
                        if word.lower() not in question_words)
        return title.capitalize() 