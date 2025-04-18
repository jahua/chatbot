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
import json
import numpy as np
from ..utils.visualization import generate_visualization
import re
from io import BytesIO
from PIL import Image
from plotly.utils import PlotlyJSONEncoder

logger = logging.getLogger(__name__)

class VisualizationService:
    def __init__(self, debug_service: Optional[DebugService] = None):
        """Initialize VisualizationService with optional debug service"""
        self.debug_service = debug_service
        logger.info("VisualizationService initialized successfully")
    
    def create_visualization(self, results: List[Dict[str, Any]], query: str) -> Optional[Dict[str, Any]]:
        """Create visualization based on query results."""
        if not results or not isinstance(results, list) or not results:
            logger.warning("Cannot create visualization: empty or invalid results")
            return None
        
        try:
            debug_step = None
            if self.debug_service:
                debug_step = self.debug_service.start_step("visualization_creation", "Creating visualization based on query results")
            
            # Convert results to DataFrame
            df = pd.DataFrame(results)
            
            # Fast path for small result sets - return as a table
            if len(df) <= 3 and len(df.columns) <= 3:
                logger.info(f"Small result set ({len(df)} rows, {len(df.columns)} columns), returning as table")
                if self.debug_service and debug_step:
                    self.debug_service.end_step(debug_step, "Created table visualization for small result set")
                return self._create_table(df, query)
            
            # Determine visualization type based on query and data
            viz_type = self._determine_visualization_type(query, df)
            logger.info(f"Selected visualization type: {viz_type}")
            
            # Create visualization based on determined type
            if viz_type == "table":
                result = self._create_table(df, query)
            elif viz_type == "simple_line":
                result = self._create_simple_line_chart(df, query)
            elif viz_type == "simple_bar":
                result = self._create_simple_bar_chart(df, query)
            elif viz_type == "heatmap":
                result = self._create_heatmap(df, query)
            elif viz_type == "pie":
                result = self._create_pie_chart(df, query)
            elif viz_type == "geo":
                result = self._create_geo_chart(df, query)
            else:
                # Default to table
                result = self._create_table(df, query)
            
            if self.debug_service and debug_step:
                self.debug_service.end_step(debug_step, f"Created {viz_type} visualization")
            
            return result
        except Exception as e:
            logger.error(f"Error creating visualization: {str(e)}")
            if self.debug_service and debug_step:
                self.debug_service.end_step(debug_step, f"Error creating visualization: {str(e)}", error=True)
            # Return table as fallback
            try:
                return self._create_table(pd.DataFrame(results), query)
            except:
                return self._create_fallback_visualization(results, query, str(e))
    
    def _determine_visualization_type(self, query: str, data: pd.DataFrame) -> str:
        """Determine the most appropriate visualization type based on the query and data."""
        
        # Normalize query for keyword matching
        query_lower = query.lower()
        
        # Get basic data characteristics
        num_rows = len(data)
        num_cols = len(data.columns)
        
        # Identify numeric and date/time columns
        numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
        date_cols = [col for col in data.columns if self._is_date_column(data[col])]
        
        # Log data characteristics for debugging
        logger.debug(f"Data shape: {data.shape}, numeric columns: {len(numeric_cols)}, date columns: {len(date_cols)}")
        
        # Check for explicit visualization requests in the query
        if "show table" in query_lower or "as table" in query_lower or "in table" in query_lower:
            return "table"
        
        if any(term in query_lower for term in ["pie chart", "percentage", "proportion", "distribution"]):
            return "pie"
        
        if any(term in query_lower for term in ["map", "location", "geographic", "spatial", "region", "canton", "switzerland"]):
            return "geo"
        
        if any(term in query_lower for term in ["correlation", "heatmap", "heat map", "relationship between"]) and len(numeric_cols) >= 2:
            return "heatmap"
        
        # Check for time series/trend indicators
        time_trend_indicators = ["trend", "over time", "evolution", "changes", "growth", "decline", 
                                 "year", "month", "day", "weekly", "monthly", "yearly", "annual"]
        
        if (date_cols and any(term in query_lower for term in time_trend_indicators)) or \
           any(col for col in data.columns if "date" in str(col).lower() or "time" in str(col).lower() or "year" in str(col).lower()):
            return "simple_line"
        
        # Check for categorical comparisons
        comparison_indicators = ["compare", "comparison", "versus", "vs", "difference", "ranking", "rank", "top", "bottom"]
        if (len(numeric_cols) >= 1 and num_cols <= 10 and num_rows <= 20) or \
           any(term in query_lower for term in comparison_indicators):
            return "simple_bar"
        
        # Default visualization based on data characteristics
        if num_rows <= 20 and num_cols <= 10:
            return "table"
        
        if len(numeric_cols) >= 1 and len(data.columns) <= 10:
            return "simple_bar"
        
        # Fallback to table for complex data
        return "table"
    
    def _create_time_series(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a time series visualization"""
        try:
            # Find date/time columns
            time_cols = [col for col in df.columns if self._is_date_column(df[col])]
            
            if not time_cols:
                logger.warning("No time columns found for time series")
                return self._create_default_visualization(df, query)
            
            # Use the first time column as x-axis
            x_col = time_cols[0]
            
            # Find numeric columns for y-axis
            numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
            numeric_cols = [col for col in numeric_cols if col != x_col]
            
            if not numeric_cols:
                logger.warning("No numeric columns found for time series")
                return self._create_default_visualization(df, query)
            
            # Use the first numeric column as y-axis
            y_col = numeric_cols[0]
            
            # Create the plot
            fig = px.line(df, x=x_col, y=y_col, title=f"{y_col} over {x_col}")
            
            # Add markers for better visibility
            fig.update_traces(mode='lines+markers')
            
            # Improve layout
            fig.update_layout(
                margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
            
            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }
        except Exception as e:
            logger.error(f"Error creating time series: {str(e)}")
            return self._create_default_visualization(df, query)
    
    def _create_bar_chart(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a bar chart visualization"""
        try:
            # Simple heuristic: if we have 2 columns, use the first as x and second as y
            # If more columns, try to find a categorical and a numerical column
            if len(df.columns) == 2:
                x_col, y_col = df.columns[0], df.columns[1]
            else:
                # Find categorical columns (string or object type)
                cat_cols = df.select_dtypes(include=['object', 'string', 'category']).columns.tolist()
                # Find numeric columns
                num_cols = df.select_dtypes(include=np.number).columns.tolist()
                
                if cat_cols and num_cols:
                    x_col, y_col = cat_cols[0], num_cols[0]
                elif len(num_cols) >= 2:
                    x_col, y_col = num_cols[0], num_cols[1]
                else:
                    # Fallback
                    x_col, y_col = df.columns[0], df.columns[1] if len(df.columns) > 1 else df.columns[0]
            
            # Limit to top 10 items for readability
            if len(df) > 10:
                # Sort by the y column and take top 10
                df = df.sort_values(by=y_col, ascending=False).head(10)
            
            # Create the plot
            fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}")
            
            # Improve layout
            fig.update_layout(
                margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
            
            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }
        except Exception as e:
            logger.error(f"Error creating bar chart: {str(e)}")
            return self._create_default_visualization(df, query)
    
    def _create_pie_chart(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a pie chart visualization"""
        try:
            # Simple heuristic: if we have 2 columns, use the first as labels and second as values
            if len(df.columns) == 2:
                labels_col, values_col = df.columns[0], df.columns[1]
            else:
                # Find categorical columns (string or object type)
                cat_cols = df.select_dtypes(include=['object', 'string', 'category']).columns.tolist()
                # Find numeric columns
                num_cols = df.select_dtypes(include=np.number).columns.tolist()
                
                if cat_cols and num_cols:
                    labels_col, values_col = cat_cols[0], num_cols[0]
                else:
                    # Fallback
                    labels_col, values_col = df.columns[0], df.columns[1] if len(df.columns) > 1 else df.columns[0]
            
            # Limit to top 8 items for readability in pie charts
            if len(df) > 8:
                # Take the top 7 items plus "Others"
                top_df = df.sort_values(by=values_col, ascending=False).head(7)
                others_value = df.sort_values(by=values_col, ascending=False).iloc[7:][values_col].sum()
                others_df = pd.DataFrame({labels_col: ['Others'], values_col: [others_value]})
                df = pd.concat([top_df, others_df])
            
            # Create the plot
            fig = px.pie(df, names=labels_col, values=values_col, title=f"Distribution of {values_col}")
            
            # Improve layout
            fig.update_layout(
                margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
            
            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }
        except Exception as e:
            logger.error(f"Error creating pie chart: {str(e)}")
            return self._create_default_visualization(df, query)
    
    def _create_geo_chart(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a geographical visualization"""
        try:
            # Look for columns that might contain location information
            loc_cols = [col for col in df.columns if any(term in str(col).lower() for term in ["canton", "region", "city", "location", "area", "country"])]
            
            # Find numeric column for values
            num_cols = df.select_dtypes(include=np.number).columns.tolist()
            
            if not loc_cols or not num_cols:
                logger.warning("No suitable location or numeric columns found for geo chart")
                return self._create_default_visualization(df, query)
            
            # Use first location column and first numeric column
            loc_col = loc_cols[0]
            value_col = num_cols[0]
            
            # Create a basic choropleth map (placeholder - would need proper geo data)
            fig = px.choropleth(
                df,
                locations=loc_col,  # This would need to be properly formatted ISO codes
                color=value_col,
                title=f"{value_col} by {loc_col}",
                # Placeholder - real implementation would need proper geo data
                scope="europe"
            )
            
            # If choropleth isn't suitable, create a fallback scatter_geo
            if len(df) <= 10:  # For small datasets, fallback to scatter
                fig = px.scatter_geo(
                    df,
                    locations=loc_col,
                    size=value_col,
                    title=f"{value_col} by {loc_col}",
                    scope="europe"
                )
            
            # Improve layout
            fig.update_layout(
                margin=dict(l=0, r=0, t=30, b=0),
                geo=dict(
                    showland=True,
                    landcolor="rgb(243, 243, 243)",
                    countrycolor="rgb(204, 204, 204)"
                )
            )
            
            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }
        except Exception as e:
            logger.error(f"Error creating geo chart: {str(e)}")
            return self._create_default_visualization(df, query)
    
    def _create_default_visualization(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a default visualization based on data characteristics"""
        try:
            # If we have few rows, return a table
            if len(df) <= 10:
                return self._create_table(df, query)
            
            # Check if we have numeric columns
            num_cols = df.select_dtypes(include=np.number).columns.tolist()
            if num_cols:
                # We have numeric data, try a bar chart
                return self._create_simple_bar_chart(df, query)
            else:
                # No numeric data, return a table
                return self._create_table(df, query)
        except Exception as e:
            logger.error(f"Error creating default visualization: {str(e)}")
            return self._create_table(df, query)
    
    def _create_simple_bar_chart(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a simple Plotly bar chart."""
        try:
            # Limit rows for readability
            if len(df) > 20:
                df = df.head(20)
            
            # Find categorical column for x-axis (non-numeric with few unique values)
            categorical_cols = [col for col in df.columns if col not in df.select_dtypes(include=[np.number]).columns]
            
            # If no categorical columns, use index
            if not categorical_cols:
                x_col = df.index.name or 'index'
                df = df.reset_index()
            else:
                # Choose categorical column with the least unique values
                unique_counts = [(col, df[col].nunique()) for col in categorical_cols]
                unique_counts.sort(key=lambda x: x[1])
                x_col = unique_counts[0][0] if unique_counts else df.columns[0]
            
            # Get numeric columns for y-axis
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            # Limit to at most 3 y columns for readability
            y_cols = numeric_cols[:3]
            
            if not y_cols:
                # No numeric columns, create count-based bar chart
                value_counts = df[x_col].value_counts().reset_index()
                value_counts.columns = [x_col, 'count']
                
                fig = go.Figure(go.Bar(
                    x=value_counts[x_col],
                    y=value_counts['count'],
                    text=value_counts['count'],
                    textposition='auto'
                ))
                
                fig.update_layout(
                    title=f"Count of {x_col}",
                    xaxis_title=str(x_col),
                    yaxis_title="Count",
                    template="plotly_white"
                )
            else:
                # Create a grouped bar chart for multiple y columns
                fig = go.Figure()
                
                for y_col in y_cols:
                    fig.add_trace(go.Bar(
                        x=df[x_col],
                        y=df[y_col],
                        name=str(y_col)
                    ))
                
                fig.update_layout(
                    title=f"{', '.join(str(col) for col in y_cols)} by {x_col}",
                    xaxis_title=str(x_col),
                    yaxis_title="Value",
                    legend_title="Metrics",
                    barmode='group',
                    template="plotly_white"
                )
            
            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }
        
        except Exception as e:
            logger.error(f"Error creating bar chart: {str(e)}", exc_info=True)
            return {"type": "table", "data": df.to_dict(orient="records")}
    
    def _create_simple_line_chart(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a simple Plotly line chart."""
        try:
            # Find the best x column (prefer date/time columns)
            date_cols = [col for col in df.columns if self._is_date_column(df[col])]
            
            # If no date columns, look for columns that might be dates based on name
            if not date_cols:
                potential_date_cols = [col for col in df.columns if any(term in str(col).lower() 
                                                                      for term in ["date", "time", "year", "month", "day"])]
                date_cols = potential_date_cols
            
            # If still no date columns, use the first column as x
            x_col = date_cols[0] if date_cols else df.columns[0]
            
            # Get numeric columns for y-axis (exclude the x column if it's numeric)
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if x_col in numeric_cols:
                numeric_cols.remove(x_col)
            
            # Limit to at most 5 y columns for readability
            y_cols = numeric_cols[:5]
            
            # Create plotly figure
            fig = go.Figure()
            
            for y_col in y_cols:
                fig.add_trace(go.Scatter(
                    x=df[x_col],
                    y=df[y_col],
                    mode='lines+markers',
                    name=str(y_col)
                ))
            
            # Update layout
            fig.update_layout(
                title=f"Trends in {', '.join(str(col) for col in y_cols)}",
                xaxis_title=str(x_col),
                yaxis_title="Value",
                legend_title="Metrics",
                template="plotly_white"
            )
            
            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }
        
        except Exception as e:
            logger.error(f"Error creating line chart: {str(e)}", exc_info=True)
            return {"type": "table", "data": df.to_dict(orient="records")}

    def _create_heatmap(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a Plotly heatmap for correlation analysis."""
        try:
            # Get numeric columns for correlation
            numeric_df = df.select_dtypes(include=[np.number])
            
            # If not enough numeric columns, return table
            if len(numeric_df.columns) < 2:
                return {"type": "table", "data": df.to_dict(orient="records")}
            
            # Calculate correlation matrix
            corr_matrix = numeric_df.corr()
            
            # Create heatmap
            fig = go.Figure(data=go.Heatmap(
                z=corr_matrix.values,
                x=corr_matrix.columns,
                y=corr_matrix.index,
                colorscale='RdBu_r',
                zmin=-1,
                zmax=1,
                cmid=0,
                text=np.round(corr_matrix.values, 2),
                hoverinfo='text'
            ))
            
            fig.update_layout(
                title="Correlation Heatmap",
                xaxis_title="Features",
                yaxis_title="Features",
                template="plotly_white"
            )
            
            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }
        
        except Exception as e:
            logger.error(f"Error creating heatmap: {str(e)}", exc_info=True)
            return {"type": "table", "data": df.to_dict(orient="records")}
    
    def _is_date_column(self, column: pd.Series) -> bool:
        """Check if a column contains date/time data."""
        # Check if the column is already a datetime type
        if pd.api.types.is_datetime64_any_dtype(column):
            return True
        
        # If it's an object type, check if the values are datetime objects
        if pd.api.types.is_object_dtype(column):
            non_null_values = [x for x in column if x is not None and not pd.isna(x)]
            if non_null_values and all(isinstance(x, (datetime, date)) for x in non_null_values):
                return True
            
            # Try parsing a sample of the values
            try:
                sample = non_null_values[:5] if len(non_null_values) > 5 else non_null_values
                if sample and all(isinstance(pd.to_datetime(x), (pd.Timestamp)) for x in sample):
                    return True
            except:
                pass
        
        return False
    
    def _extract_title_from_query(self, query: str) -> Optional[str]:
        """Extract a title from the query for the visualization."""
        # Remove SQL keywords and common phrases
        keywords = ["select", "from", "where", "group by", "order by", "having", "show me", "display", "visualize"]
        clean_query = query.lower()
        for keyword in keywords:
            clean_query = clean_query.replace(keyword, "")
        
        # Split into words and take first 8 words
        words = clean_query.split()[:8]
        if not words:
            return None
        
        # Create a simple title
        title = " ".join(words).strip().capitalize()
        if title.endswith("."):
            title = title[:-1]
        
        return title
    
    def _create_fallback_visualization(self, results: List[Dict[str, Any]], query: str, error_msg: str) -> Dict[str, Any]:
        """Create a fallback visualization when all else fails."""
        try:
            logger.warning(f"Falling back to table visualization due to error: {error_msg}")
            
            # Try to convert to DataFrame
            try:
                df = pd.DataFrame(results)
                # Limit to first 50 rows for display
                df = df.head(50)
                table_data = df.to_dict(orient="records")
            except Exception:
                # If conversion fails, pass the raw results
                table_data = results[:50]  # Limit to first 50 items
            
            return {
                "type": "table",
                "data": table_data,
                "error": error_msg
            }
        except Exception as e:
            logger.error(f"Error creating fallback visualization: {str(e)}")
            # Ultimate fallback - return minimal data
            return {
                "type": "table",
                "data": [],
                "error": f"Visualization failed: {error_msg}. Additional error: {str(e)}"
            }
    
    def _create_table(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a table visualization from the data"""
        return {
            "type": "table",
            "data": json.loads(df.to_json(orient="records", date_format="iso"))
        } 