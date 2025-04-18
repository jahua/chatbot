from typing import Dict, Any, Optional, List
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import io
import base64
from datetime import datetime, date
import decimal
from app.rag.debug_service import DebugService

logger = logging.getLogger(__name__)

class VisualizationService:
    def __init__(self, debug_service: Optional[DebugService] = None):
        """Initialize VisualizationService with optional debug service"""
        self.debug_service = debug_service
        logger.info("VisualizationService initialized successfully")
    
    def create_visualization(self, data: List[Dict[str, Any]], query_text: str) -> Optional[str]:
        """
        Create appropriate visualization based on data and query
        Returns base64 encoded image or None if visualization not possible
        """
        if not data or len(data) == 0:
            logger.warning("No data available for visualization")
            return None
        
        try:
            if self.debug_service:
                self.debug_service.start_step("visualization_creation", {
                    "data_rows": len(data),
                    "query_text": query_text
                })
            
            # Convert data to pandas DataFrame
            df = self._convert_to_dataframe(data)
            
            # Determine the best visualization type based on data and query
            viz_type = self._determine_visualization_type(df, query_text)
            
            # Create visualization based on type
            if viz_type == "bar":
                fig = self._create_bar_chart(df, query_text)
            elif viz_type == "line":
                fig = self._create_line_chart(df, query_text)
            elif viz_type == "pie":
                fig = self._create_pie_chart(df, query_text)
            elif viz_type == "geo":
                fig = self._create_geo_chart(df, query_text)
            else:
                # Default to table if no suitable visualization
                fig = self._create_table(df, query_text)
            
            # Convert Plotly figure to base64 image
            img_base64 = self._fig_to_base64(fig)
            
            if self.debug_service:
                self.debug_service.add_step_details({
                    "visualization_type": viz_type,
                    "visualization_size": len(img_base64) if img_base64 else 0
                })
                self.debug_service.end_step()
            
            return img_base64
            
        except Exception as e:
            logger.error(f"Error creating visualization: {str(e)}")
            if self.debug_service:
                self.debug_service.add_step_details({"error": str(e)})
                self.debug_service.end_step(error=e)
            return None
    
    def _convert_to_dataframe(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Convert list of dictionaries to pandas DataFrame with type conversion"""
        df = pd.DataFrame(data)
        
        # Convert types for better visualization
        for col in df.columns:
            # Convert datetime objects to appropriate format
            if df[col].dtype == 'object':
                # Check if column contains datetime objects
                if all(isinstance(x, (datetime, date)) for x in df[col].dropna()):
                    df[col] = pd.to_datetime(df[col])
                # Convert decimal objects to float
                elif all(isinstance(x, decimal.Decimal) for x in df[col].dropna()):
                    df[col] = df[col].astype(float)
        
        return df
    
    def _determine_visualization_type(self, df: pd.DataFrame, query_text: str) -> str:
        """Determine the most appropriate visualization type based on data and query"""
        query_lower = query_text.lower()
        num_rows = len(df)
        num_cols = len(df.columns)
        
        # Check number of columns to help determine chart type
        numeric_cols = df.select_dtypes(include=['number']).columns
        date_cols = df.select_dtypes(include=['datetime']).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        has_numeric = len(numeric_cols) > 0
        has_dates = len(date_cols) > 0
        has_categories = len(categorical_cols) > 0
        
        # For time series data
        if has_dates and has_numeric:
            return "line"
        
        # For comparison between categories
        if has_categories and has_numeric and len(df) <= 15:
            if "compare" in query_lower or "comparison" in query_lower:
                return "bar"
            elif any(term in query_lower for term in ["distribution", "breakdown", "portion", "percentage"]):
                return "pie"
            else:
                return "bar"
        
        # For geographic data
        if any(col in df.columns for col in ["region", "region_name", "country", "location"]):
            if "map" in query_lower or "geographic" in query_lower or "spatial" in query_lower:
                return "geo"
        
        # Default to bar for most numeric/categorical combinations
        if has_numeric and (has_categories or num_rows <= 20):
            return "bar"
        
        # Default to table for complex data
        return "table"
    
    def _create_bar_chart(self, df: pd.DataFrame, query_text: str) -> go.Figure:
        """Create a bar chart based on the data and query"""
        query_lower = query_text.lower()
        
        # Identify the most likely columns for x and y axes
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        # Use the first categorical column for x-axis if available
        x_col = categorical_cols[0] if categorical_cols else df.columns[0]
        
        # Use the first numeric column for y-axis if available, otherwise use the second column
        y_col = numeric_cols[0] if numeric_cols else df.columns[1] if len(df.columns) > 1 else df.columns[0]
        
        # Determine if highlighting is needed
        highlight = False
        highlight_col = None
        
        if "busiest" in query_lower or "highest" in query_lower or "top" in query_lower:
            highlight = True
            highlight_col = "is_highlighted"
            df[highlight_col] = False
            if not df.empty:
                max_idx = df[y_col].idxmax()
                df.loc[max_idx, highlight_col] = True
        
        # Create title based on query
        title = self._generate_title(query_text, x_col, y_col)
        
        # Create the chart
        if highlight and highlight_col in df.columns:
            colors = ['rgba(0, 123, 255, 0.8)' if row[highlight_col] else 'rgba(0, 123, 255, 0.3)' 
                     for _, row in df.iterrows()]
            fig = px.bar(df, x=x_col, y=y_col, title=title)
            fig.update_traces(marker_color=colors)
        else:
            fig = px.bar(df, x=x_col, y=y_col, title=title)
        
        # Format axis labels
        fig.update_layout(
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title()
        )
        
        # If x-axis has many values, angle the labels
        if len(df[x_col].unique()) > 5:
            fig.update_layout(xaxis_tickangle=-45)
        
        # Format y-axis for currency if relevant
        if any(term in y_col.lower() for term in ["amount", "spending", "revenue", "price", "cost"]):
            fig.update_layout(yaxis=dict(tickprefix='$', tickformat=',.0f'))
        elif "count" in y_col.lower() or "visitors" in y_col.lower() or "total" in y_col.lower():
            fig.update_layout(yaxis=dict(tickformat=',d'))
        
        return fig
    
    def _create_line_chart(self, df: pd.DataFrame, query_text: str) -> go.Figure:
        """Create a line chart based on the data and query"""
        # Identify the most likely columns for x and y axes
        date_cols = df.select_dtypes(include=['datetime']).columns.tolist()
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        # Use the first date column for x-axis if available
        x_col = date_cols[0] if date_cols else df.columns[0]
        
        # Use the first numeric column for y-axis if available
        y_col = numeric_cols[0] if numeric_cols else df.columns[1] if len(df.columns) > 1 else df.columns[0]
        
        # Check if we need multiple lines (by adding a color grouping)
        color_col = None
        for col in df.columns:
            if col not in [x_col, y_col] and len(df[col].unique()) <= 10:
                color_col = col
                break
        
        # Create title based on query
        title = self._generate_title(query_text, x_col, y_col)
        
        # Create the chart
        if color_col:
            fig = px.line(df, x=x_col, y=y_col, color=color_col, title=title,
                         markers=True)
        else:
            fig = px.line(df, x=x_col, y=y_col, title=title,
                         markers=True)
        
        # Format axis labels
        fig.update_layout(
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title=y_col.replace('_', ' ').title()
        )
        
        # Format y-axis for currency if relevant
        if any(term in y_col.lower() for term in ["amount", "spending", "revenue", "price", "cost"]):
            fig.update_layout(yaxis=dict(tickprefix='$', tickformat=',.0f'))
        elif "count" in y_col.lower() or "visitors" in y_col.lower() or "total" in y_col.lower():
            fig.update_layout(yaxis=dict(tickformat=',d'))
        
        return fig
    
    def _create_pie_chart(self, df: pd.DataFrame, query_text: str) -> go.Figure:
        """Create a pie chart based on the data and query"""
        # Identify the most likely columns for labels and values
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        # Use the first categorical column for labels
        label_col = categorical_cols[0] if categorical_cols else df.columns[0]
        
        # Use the first numeric column for values
        value_col = numeric_cols[0] if numeric_cols else df.columns[1] if len(df.columns) > 1 else df.columns[0]
        
        # Create title based on query
        title = self._generate_title(query_text, label_col, value_col, "Distribution")
        
        # Create the chart
        fig = px.pie(df, names=label_col, values=value_col, title=title)
        
        # Add percentage labels
        fig.update_traces(textposition='inside', textinfo='percent+label')
        
        return fig
    
    def _create_geo_chart(self, df: pd.DataFrame, query_text: str) -> go.Figure:
        """Create a geographical chart based on the data and query"""
        # This is just a placeholder - in a real implementation, 
        # you would need to map region names to coordinates or GeoJSON files
        
        # For now, create a bar chart as a fallback
        return self._create_bar_chart(df, query_text)
    
    def _create_table(self, df: pd.DataFrame, query_text: str) -> go.Figure:
        """Create a table visualization for complex data"""
        # Create a simple table using plotly
        fig = go.Figure(data=[go.Table(
            header=dict(
                values=list(df.columns),
                fill_color='paleturquoise',
                align='left'
            ),
            cells=dict(
                values=[df[col] for col in df.columns],
                fill_color='lavender',
                align='left'
            )
        )])
        
        # Set title
        title = f"Data for: {query_text}"
        fig.update_layout(title=title)
        
        return fig
    
    def _generate_title(self, query_text: str, x_col: str, y_col: str, chart_type: str = "") -> str:
        """Generate an appropriate title based on the query and data columns"""
        query_words = query_text.strip().split()
        
        # If query is very short, use the column names
        if len(query_words) <= 3:
            if chart_type:
                return f"{chart_type} of {y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}"
            else:
                return f"{y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}"
        
        # Use first 8 words of query if not too long
        if len(query_words) <= 10:
            return query_text.capitalize()
        
        # Otherwise use truncated query
        return " ".join(query_words[:8]) + "..."
    
    def _fig_to_base64(self, fig: go.Figure) -> str:
        """Convert a plotly figure to base64 encoded PNG"""
        try:
            # Create a BytesIO object to store the image
            img_bytes = io.BytesIO()
            
            # Write the figure as a PNG to the BytesIO object
            fig.write_image(img_bytes, format='png', width=800, height=500)
            
            # Reset the pointer to the beginning of the BytesIO object
            img_bytes.seek(0)
            
            # Encode the PNG as base64
            img_base64 = base64.b64encode(img_bytes.read()).decode('utf-8')
            
            return img_base64
        except Exception as e:
            logger.error(f"Error converting figure to base64: {str(e)}")
            return ""
    
    # Public methods that will be called by ChatService
    def create_bar_chart(self, data: List[Dict[str, Any]], query_text: str = "Visitor data") -> Optional[str]:
        """Create a bar chart visualization from the given data"""
        try:
            if self.debug_service:
                self.debug_service.start_step("bar_chart_creation", {
                    "data_rows": len(data),
                    "query_text": query_text
                })
            
            # Convert data to DataFrame
            df = self._convert_to_dataframe(data)
            
            # Create bar chart
            fig = self._create_bar_chart(df, query_text)
            
            # Convert to base64
            img_base64 = self._fig_to_base64(fig)
            
            if self.debug_service:
                self.debug_service.add_step_details({
                    "visualization_type": "bar",
                    "visualization_size": len(img_base64) if img_base64 else 0
                })
                self.debug_service.end_step()
            
            return img_base64
        except Exception as e:
            logger.error(f"Error creating bar chart: {str(e)}")
            if self.debug_service:
                self.debug_service.add_step_details({"error": str(e)})
                self.debug_service.end_step(error=e)
            return None
    
    def create_pie_chart(self, data: List[Dict[str, Any]], query_text: str = "Distribution data") -> Optional[str]:
        """Create a pie chart visualization from the given data"""
        try:
            if self.debug_service:
                self.debug_service.start_step("pie_chart_creation", {
                    "data_rows": len(data),
                    "query_text": query_text
                })
            
            # Convert data to DataFrame
            df = self._convert_to_dataframe(data)
            
            # Create pie chart
            fig = self._create_pie_chart(df, query_text)
            
            # Convert to base64
            img_base64 = self._fig_to_base64(fig)
            
            if self.debug_service:
                self.debug_service.add_step_details({
                    "visualization_type": "pie",
                    "visualization_size": len(img_base64) if img_base64 else 0
                })
                self.debug_service.end_step()
            
            return img_base64
        except Exception as e:
            logger.error(f"Error creating pie chart: {str(e)}")
            if self.debug_service:
                self.debug_service.add_step_details({"error": str(e)})
                self.debug_service.end_step(error=e)
            return None
    
    def create_line_chart(self, data: List[Dict[str, Any]], query_text: str = "Trend data") -> Optional[str]:
        """Create a line chart visualization from the given data"""
        try:
            if self.debug_service:
                self.debug_service.start_step("line_chart_creation", {
                    "data_rows": len(data),
                    "query_text": query_text
                })
            
            # Convert data to DataFrame
            df = self._convert_to_dataframe(data)
            
            # Create line chart
            fig = self._create_line_chart(df, query_text)
            
            # Convert to base64
            img_base64 = self._fig_to_base64(fig)
            
            if self.debug_service:
                self.debug_service.add_step_details({
                    "visualization_type": "line",
                    "visualization_size": len(img_base64) if img_base64 else 0
                })
                self.debug_service.end_step()
            
            return img_base64
        except Exception as e:
            logger.error(f"Error creating line chart: {str(e)}")
            if self.debug_service:
                self.debug_service.add_step_details({"error": str(e)})
                self.debug_service.end_step(error=e)
            return None
    
    def create_default_visualization(self, data: List[Dict[str, Any]], query_text: str = "Query results") -> Optional[str]:
        """Create a default visualization based on the data"""
        try:
            if self.debug_service:
                self.debug_service.start_step("default_visualization_creation", {
                    "data_rows": len(data),
                    "query_text": query_text
                })
            
            # Convert data to DataFrame
            df = self._convert_to_dataframe(data)
            
            # Determine best visualization type
            viz_type = self._determine_visualization_type(df, query_text)
            
            # Create appropriate visualization
            if viz_type == "bar":
                fig = self._create_bar_chart(df, query_text)
            elif viz_type == "line":
                fig = self._create_line_chart(df, query_text)
            elif viz_type == "pie":
                fig = self._create_pie_chart(df, query_text)
            elif viz_type == "geo":
                fig = self._create_geo_chart(df, query_text)
            else:
                fig = self._create_table(df, query_text)
            
            # Convert to base64
            img_base64 = self._fig_to_base64(fig)
            
            if self.debug_service:
                self.debug_service.add_step_details({
                    "visualization_type": viz_type,
                    "visualization_size": len(img_base64) if img_base64 else 0
                })
                self.debug_service.end_step()
            
            return img_base64
        except Exception as e:
            logger.error(f"Error creating default visualization: {str(e)}")
            if self.debug_service:
                self.debug_service.add_step_details({"error": str(e)})
                self.debug_service.end_step(error=e)
            return None
            
    def create_geo_chart(self, data: List[Dict[str, Any]], query_text: str = "Geographic data") -> Optional[str]:
        """Create a geographic visualization from the given data"""
        try:
            if self.debug_service:
                self.debug_service.start_step("geo_chart_creation", {
                    "data_rows": len(data),
                    "query_text": query_text
                })
            
            # Convert data to DataFrame
            df = self._convert_to_dataframe(data)
            
            # Create geo chart
            fig = self._create_geo_chart(df, query_text)
            
            # Convert to base64
            img_base64 = self._fig_to_base64(fig)
            
            if self.debug_service:
                self.debug_service.add_step_details({
                    "visualization_type": "geo",
                    "visualization_size": len(img_base64) if img_base64 else 0
                })
                self.debug_service.end_step()
            
            return img_base64
        except Exception as e:
            logger.error(f"Error creating geo chart: {str(e)}")
            if self.debug_service:
                self.debug_service.add_step_details({"error": str(e)})
                self.debug_service.end_step(error=e)
            return None 