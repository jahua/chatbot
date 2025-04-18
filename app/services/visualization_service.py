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
            
            # Create a more user-friendly visualization for empty results
            try:
                # Create a Plotly figure with a centered "No Data Found" message
                fig = go.Figure()
                
                # Add a centered text annotation
                fig.add_annotation(
                    x=0.5,
                    y=0.5,
                    text="<b>No Data Found</b>",
                    font=dict(size=24, color="#666666"),
                    showarrow=False
                )
                
                # Add a subtitle with a suggestion
                fig.add_annotation(
                    x=0.5,
                    y=0.4,
                    text="Try modifying your query or selecting a different time period",
                    font=dict(size=14, color="#888888"),
                    showarrow=False
                )
                
                # Clean up the layout
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    height=300,
                    xaxis=dict(visible=False),
                    yaxis=dict(visible=False),
                    margin=dict(l=20, r=20, t=20, b=20)
                )
                
                # Return as plotly_json for a cleaner empty state
                return {
                    "type": "plotly_json",
                    "data": json.loads(json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)),
                    "no_data": True,
                    "query": query
                }
            except Exception as e:
                logger.error(f"Error creating empty visualization: {str(e)}")
                # Fall back to the simple message
                return {
                    "type": "no_data",
                    "data": {
                        "message": "No data was found for this query.",
                        "query": query
                    }
                }
        
        try:
            debug_step = None
            if self.debug_service:
                debug_step = self.debug_service.start_step("visualization_creation", "Creating visualization based on query results")
            
            # Convert results to DataFrame
            df = pd.DataFrame(results)
            
            # Special case: Handle single value results (1 row, 1 column)
            if len(df) == 1 and len(df.columns) == 1:
                logger.info(f"Single value result detected: row={len(df)}, col={len(df.columns)}")
                if self.debug_service and debug_step:
                    self.debug_service.end_step(debug_step, "Created single value visualization")
                return self._create_single_value_visualization(df, query)
            
            # Fast path for small result sets - return as a table
            if len(df) <= 3 and len(df.columns) <= 3:
                logger.info(f"Small result set ({len(df)} rows, {len(df.columns)} columns), returning as table")
                if self.debug_service and debug_step:
                    self.debug_service.end_step(debug_step, "Created table visualization for small result set")
                return self._create_table(df, query)
            
            # Enhanced: Use hybrid visualization type selection
            viz_type = self._hybrid_visualization_selection(query, df)
            logger.info(f"Selected visualization type using hybrid approach: {viz_type}")
            
            # Create visualization based on determined type with enhanced error handling
            result = self._create_visualization_with_fallbacks(df, query, viz_type)
            
            if self.debug_service and debug_step:
                self.debug_service.end_step(debug_step, f"Created visualization (primary or fallback)")
            
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
    
    def _hybrid_visualization_selection(self, query: str, data: pd.DataFrame) -> str:
        """Enhanced hybrid approach for visualization type selection combining heuristics and data-driven patterns.
        
        This method incorporates the intelligence from both rule-based and data pattern-based approaches.
        """
        try:
            # First, try to intelligently analyze the data patterns
            viz_type = self._select_viz_by_data_patterns(data, query)
            if viz_type:
                logger.info(f"Data pattern analysis selected visualization type: {viz_type}")
                return viz_type
                
            # If data pattern approach couldn't determine a type, fall back to rule-based
            logger.info("Falling back to rule-based visualization selection")
            return self._determine_visualization_type(query, data)
            
        except Exception as e:
            logger.warning(f"Error in hybrid visualization selection: {str(e)}")
            # Fall back to traditional rule-based approach
            return self._determine_visualization_type(query, data)
    
    def _select_viz_by_data_patterns(self, df: pd.DataFrame, query: str) -> Optional[str]:
        """Select visualization type based on data patterns and characteristics"""
        try:
            # Get basic data characteristics
            num_rows = len(df)
            num_cols = len(df.columns)
            
            # Identify key column types
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            categorical_cols = df.select_dtypes(include=['object', 'string', 'category']).columns.tolist()
            date_cols = [col for col in df.columns if self._is_date_column(df[col])]
            
            # Log the data profile
            logger.debug(f"Data profile - rows: {num_rows}, cols: {num_cols}, " +
                       f"numeric: {len(numeric_cols)}, categorical: {len(categorical_cols)}, " +
                       f"dates: {len(date_cols)}")
            
            # Check for key patterns
            
            # Time series pattern: Date column + numeric columns with many rows
            if date_cols and numeric_cols and num_rows > 5:
                # Time series analysis
                return "simple_line"
                
            # Comparison pattern: Few categorical items with numeric values
            if categorical_cols and numeric_cols and len(df[categorical_cols[0]].unique()) <= 10 and num_rows <= 15:
                # Comparison across categories
                return "simple_bar"
                
            # Distribution pattern: Single categorical with percentages or counts
            if (len(categorical_cols) == 1 and len(numeric_cols) == 1 and 
                len(df[categorical_cols[0]].unique()) <= 10 and 
                ('percent' in query.lower() or 'distribution' in query.lower() or 'share' in query.lower())):
                return "pie"
                
            # Geographical pattern: Contains region, country, or location columns
            geo_keywords = ['region', 'country', 'location', 'city', 'state', 'canton']
            has_geo_column = any(any(geo_kw in col.lower() for geo_kw in geo_keywords) for col in df.columns)
            if has_geo_column:
                return "geo"
                
            # Correlation pattern: Multiple numeric columns
            if len(numeric_cols) >= 3 and 'correlation' in query.lower():
                return "heatmap"
                
            # Tabular data: Complex data or explicitly requested table
            if num_cols > 5 or 'table' in query.lower():
                return "table"
                
            # No clear pattern detected
            return None
            
        except Exception as e:
            logger.warning(f"Error analyzing data patterns: {str(e)}")
            return None
    
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
        """Create a geographical visualization based on the data"""
        try:
            # Check if we have geographic data
            logger.info(f"Attempting to create geo chart with columns: {df.columns.tolist()}")
            
            # Look for region/location columns
            region_cols = [col for col in df.columns if any(kw in col.lower() for kw in 
                          ['region', 'location', 'city', 'canton', 'country', 'place'])]
            
            if not region_cols:
                logger.warning("No geographic columns found for geo visualization")
                return self._create_default_visualization(df, query)
            
            # Use the first region column
            region_col = region_cols[0]
            
            # Find value columns (numeric)
            numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
            if not numeric_cols:
                logger.warning("No numeric columns found for geo visualization")
                return self._create_default_visualization(df, query)
            
            # Use the first numeric column for values
            value_col = numeric_cols[0]
            
            # Enhanced: Check if we have lat/long coordinates for a map
            has_coords = any(c in df.columns for c in ['latitude', 'longitude', 'lat', 'long', 'lng'])
            
            if has_coords:
                return self._create_map_visualization(df, region_col, value_col, query)
            else:
                # Create region comparison bar chart
                return self._create_region_comparison(df, region_col, value_col, query)
            
        except Exception as e:
            logger.error(f"Error creating geo chart: {str(e)}")
            return self._create_default_visualization(df, query)

    def _create_map_visualization(self, df: pd.DataFrame, region_col: str, value_col: str, query: str) -> Dict[str, Any]:
        """Create a map-based visualization when coordinates are available"""
        try:
            # Identify lat/long columns
            lat_col = next((c for c in df.columns if c.lower() in ['latitude', 'lat']), None)
            lng_col = next((c for c in df.columns if c.lower() in ['longitude', 'long', 'lng']), None)
            
            if not lat_col or not lng_col:
                logger.warning("Couldn't identify latitude/longitude columns")
                return self._create_region_comparison(df, region_col, value_col, query)
            
            # Create a scatter mapbox
            fig = go.Figure()
            
            # Create hover text with rich information
            hover_text = []
            for _, row in df.iterrows():
                region_name = row[region_col]
                value = row[value_col]
                
                # Include additional information in hover if available
                additional_info = ""
                for col in df.columns:
                    if col not in [region_col, value_col, lat_col, lng_col]:
                        additional_info += f"<br>{col}: {row[col]}"
                
                hover_text.append(f"<b>{region_name}</b><br>{value_col}: {value}{additional_info}")
            
            # Calculate marker sizes based on values
            max_value = df[value_col].max() if not df[value_col].empty else 1
            min_value = df[value_col].min() if not df[value_col].empty else 0
            size_range = (20, 50)  # Min and max marker sizes
            
            if max_value > min_value:
                # Normalize to the size range
                sizes = ((df[value_col] - min_value) / (max_value - min_value)) * (size_range[1] - size_range[0]) + size_range[0]
            else:
                # If all values are the same, use the middle size
                sizes = [size_range[0] + (size_range[1] - size_range[0])/2] * len(df)
            
            # Add markers to the map
            fig.add_trace(go.Scattermapbox(
                lat=df[lat_col],
                lon=df[lng_col],
                mode='markers',
                marker=dict(
                    size=sizes,
                    color=df[value_col],
                    colorscale='Viridis',
                    opacity=0.7,
                    colorbar=dict(title=value_col)
                ),
                text=hover_text,
                hoverinfo='text'
            ))
            
            # Set map center
            center_lat = df[lat_col].mean()
            center_lng = df[lng_col].mean()
            
            # Configure the mapbox layout
            fig.update_layout(
                title=self._extract_title_from_query(query) or f"{value_col} by {region_col}",
                mapbox=dict(
                    style='open-street-map',  # Use open street map style which doesn't require a token
                    center=dict(lat=center_lat, lon=center_lng),
                    zoom=7
                ),
                margin=dict(l=0, r=0, t=40, b=0)
            )
            
            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }
            
        except Exception as e:
            logger.error(f"Error creating map visualization: {str(e)}")
            return self._create_region_comparison(df, region_col, value_col, query)
    
    def _create_region_comparison(self, df: pd.DataFrame, region_col: str, value_col: str, query: str) -> Dict[str, Any]:
        """Create a bar chart comparing regions when coordinates aren't available"""
        try:
            # Limit to top 15 regions for readability
            if len(df) > 15:
                df = df.sort_values(by=value_col, ascending=False).head(15)
            
            # Group data by region if there are multiple rows per region
            if len(df[region_col].unique()) < len(df):
                grouped_df = df.groupby(region_col)[value_col].sum().reset_index()
            else:
                grouped_df = df
            
            # Create a bar chart comparing regions
            fig = px.bar(
                grouped_df, 
                x=region_col, 
                y=value_col,
                title=self._extract_title_from_query(query) or f"{value_col} by {region_col}",
                color=value_col,
                color_continuous_scale='Viridis'
            )
            
            # Improve layout
            fig.update_layout(
                margin=dict(l=20, r=20, t=40, b=20),
                xaxis_title=region_col,
                yaxis_title=value_col
            )
            
            # Adjust x-axis labels if they're long
            if any(len(str(region)) > 10 for region in grouped_df[region_col]):
                fig.update_layout(
                    xaxis=dict(
                        tickangle=45,
                        tickmode='array',
                        tickvals=list(range(len(grouped_df))),
                        ticktext=grouped_df[region_col].astype(str).str.slice(0, 15).tolist()
                    )
                )
            
            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }
            
        except Exception as e:
            logger.error(f"Error creating region comparison: {str(e)}")
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
    
    def _create_simple_bar_chart(self, df: pd.DataFrame, title: str = None) -> dict:
        # Identify numeric columns for y-axis
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        spending_cols = [col for col in numeric_cols if any(term in col.lower() for term in ['spending', 'amount', 'total'])]
        
        # Use spending column if available, otherwise use first numeric column
        y_column = spending_cols[0] if spending_cols else numeric_cols[0]
        
        # Identify categorical columns for x-axis, preferring region or location
        categorical_cols = df.select_dtypes(include=['object']).columns
        region_cols = [col for col in categorical_cols if any(term in col.lower() for term in ['region', 'location', 'city'])]
        x_column = region_cols[0] if region_cols else categorical_cols[0]
        
        # Sort values by spending in descending order
        df_sorted = df.sort_values(by=y_column, ascending=False)
        
        # Create the bar chart
        fig = {
            "data": [{
                "type": "bar",
                "x": df_sorted[x_column].tolist(),
                "y": df_sorted[y_column].tolist(),
                "hovertemplate": f"{x_column}: %{{x}}<br>{y_column}: %{{y:,.2f}} CHF<extra></extra>",
            }],
            "layout": {
                "title": title or f"{y_column} by {x_column}",
                "xaxis": {
                    "title": x_column.replace('_', ' ').title(),
                    "tickangle": 45,
                    "automargin": True
                },
                "yaxis": {
                    "title": f"{y_column.replace('_', ' ').title()} (CHF)",
                    "automargin": True
                },
                "margin": {"t": 50, "l": 50, "r": 20, "b": 100},
                "showlegend": False,
                "template": "plotly"
            }
        }
        
        return fig
    
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

    def _create_visualization_with_fallbacks(self, df: pd.DataFrame, query: str, primary_viz_type: str) -> Dict[str, Any]:
        """Create visualization with fallback options if primary type fails."""
        try:
            # Define visualization methods and their fallback paths
            viz_methods = {
                'geo': self._create_geo_chart,
                'bar': self._create_simple_bar_chart,
                'line': self._create_simple_line_chart,
                'pie': self._create_pie_chart,
                'heatmap': self._create_heatmap,
                'table': self._create_table
            }
            
            # Define fallback paths for each visualization type
            fallback_paths = {
                'geo': ['bar', 'table'],
                'bar': ['line', 'table'],
                'line': ['bar', 'table'],
                'pie': ['bar', 'table'],
                'heatmap': ['bar', 'table'],
                'table': []  # No fallbacks for table
            }
            
            # Try primary visualization type
            logger.info(f"Attempting primary visualization type: {primary_viz_type}")
            if primary_viz_type in viz_methods:
                try:
                    result = viz_methods[primary_viz_type](df, query)
                    if result:
                        logger.info(f"Successfully created {primary_viz_type} visualization")
                        return result
                except Exception as e:
                    logger.warning(f"Failed to create {primary_viz_type} visualization: {str(e)}")
            
            # Try fallbacks if primary fails
            if primary_viz_type in fallback_paths:
                for fallback_type in fallback_paths[primary_viz_type]:
                    logger.info(f"Attempting fallback visualization type: {fallback_type}")
                    try:
                        result = viz_methods[fallback_type](df, query)
                        if result:
                            logger.info(f"Successfully created fallback {fallback_type} visualization")
                            return result
                    except Exception as e:
                        logger.warning(f"Failed to create {fallback_type} visualization: {str(e)}")
            
            # If all attempts fail, create a simple error visualization
            logger.error("All visualization attempts failed, creating error visualization")
            return self._create_fallback_visualization(df.to_dict('records'), query, "Failed to create visualization")
            
        except Exception as e:
            logger.error(f"Error in visualization creation: {str(e)}")
            return self._create_fallback_visualization(df.to_dict('records'), query, str(e))

    def _create_single_value_visualization(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a visualization for a single value result (1 row, 1 column)"""
        try:
            # Extract the value and column name
            column_name = df.columns[0]
            value = df.iloc[0, 0]
            
            # Create a plotly figure with a single value display
            fig = go.Figure()
            
            # Add a centered text annotation with the value
            fig.add_annotation(
                x=0.5,
                y=0.5,
                text=f"<b>{value}</b>",
                font=dict(size=36),
                showarrow=False
            )
            
            # Add a subtitle with the column name
            fig.add_annotation(
                x=0.5,
                y=0.3,
                text=f"{column_name}",
                font=dict(size=18),
                showarrow=False
            )
            
            # Clean up the layout
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=200,
                xaxis=dict(visible=False),
                yaxis=dict(visible=False),
                margin=dict(l=20, r=20, t=20, b=20)
            )
            
            # Extract title from query
            title = self._extract_title_from_query(query)
            if title:
                fig.update_layout(title=title)
            
            # Return as plotly_json
            return {
                "type": "plotly_json",
                "data": json.loads(json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)),
                "single_value": True,
                "column_name": column_name,
                "value": value
            }
        except Exception as e:
            logger.error(f"Error creating single value visualization: {str(e)}")
            # Fall back to table visualization
            return self._create_table(df, query) 