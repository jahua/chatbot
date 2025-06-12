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
from sqlalchemy import text
from sqlalchemy.orm import Session
import traceback

logger = logging.getLogger(__name__)


class StreamlitVisualizationService:
    def __init__(self):
        self.chart_cache = {}

    def create_visualization(
            self, data: Any, context: str = "") -> Optional[Dict[str, Any]]:
        """Create visualization based on data type and context."""
        try:
            # Convert data to DataFrame if needed
            if isinstance(data, list) and len(data) > 0:
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                return None

            # Get column types
            numeric_cols = df.select_dtypes(
                include=['float64', 'int64']).columns
            date_cols = [
                col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
            text_cols = df.select_dtypes(include=['object']).columns

            # Determine chart type based on data structure and context
            if len(df.columns) == 2 and len(numeric_cols) == 1:
                # Create pie chart for distribution data
                fig = px.pie(
                    df,
                    values=numeric_cols[0],
                    names=df.columns[0] if df.columns[0] != numeric_cols[0] else df.columns[1],
                    title="Distribution Analysis",
                    template="plotly_dark",
                    hole=0.4)
                chart_type = "pie"

            elif date_cols and numeric_cols.any():
                # Create line chart for time series
                date_col = date_cols[0]
                fig = px.line(
                    df,
                    x=date_col,
                    y=numeric_cols,
                    title="Time Series Analysis",
                    template="plotly_dark"
                )
                chart_type = "line"

            elif len(numeric_cols) >= 1 and len(text_cols) >= 1:
                # Create bar chart for categorical comparison
                fig = px.bar(
                    df,
                    x=text_cols[0],
                    y=numeric_cols[0],
                    title="Comparative Analysis",
                    template="plotly_dark"
                )
                chart_type = "bar"

            else:
                # Default to table view
                return {"type": "table", "data": df.to_dict('records')}

            # Enhance chart appearance
            fig.update_layout(
                height=500,
                margin=dict(l=60, r=40, t=60, b=60),
                plot_bgcolor='rgba(17, 17, 17, 0.1)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(size=13, color='white'),
                title=dict(
                    font=dict(size=16),
                    x=0.5,
                    xanchor='center'
                ),
                showlegend=True,
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01,
                    bgcolor='rgba(0,0,0,0.3)',
                    bordercolor='rgba(255,255,255,0.2)',
                    borderwidth=1
                )
            )

            # Add interactive features
            fig.update_traces(
                hovertemplate="<b>%{x}</b><br>Value: %{y:,.0f}<br><extra></extra>")

            # Cache the visualization
            cache_key = f"{context}_{len(data)}"
            self.chart_cache[cache_key] = {
                "type": chart_type,
                "figure": fig.to_json(),
                "data": df.to_dict('records')
            }

            return {
                "type": chart_type,
                "figure": fig.to_json(),
                "data": df.to_dict('records')
            }

        except Exception as e:
            print(f"Error creating visualization: {str(e)}")
            return None

    def get_cached_visualization(
            self, context: str, data_length: int) -> Optional[Dict[str, Any]]:
        """Retrieve cached visualization if available."""
        cache_key = f"{context}_{data_length}"
        return self.chart_cache.get(cache_key)


class VisualizationService:
    def __init__(self, debug_service: Optional[DebugService] = None, db: Optional[Session] = None):
        """Initialize VisualizationService with optional debug service and database session"""
        self.debug_service = debug_service
        self.db = db
        logger.info("VisualizationService initialized successfully")

    def create_visualization(
            self, results: List[Dict[str, Any]], query: str) -> Optional[Dict[str, Any]]:
        """Create visualization based on query results."""
        
        # --- Explicit Check for Pie Chart Request ---
        if ("pie chart" in query.lower() and 
            ("spending" in query.lower() or "distribution" in query.lower()) and 
            results and isinstance(results, list) and len(results) > 0):
            
            logger.info("Attempting to create requested pie chart for spending/distribution.")
            try:
                df = pd.DataFrame(results)
                # Ensure suitable columns exist (e.g., category and numeric value)
                if len(df.columns) >= 2:
                     # Try calling the specific pie chart creation method
                     pie_chart_viz = self._create_pie_chart(df, query)
                     if pie_chart_viz and isinstance(pie_chart_viz, dict):
                         logger.info(f"Successfully created pie chart visualization with type: {pie_chart_viz.get('type')}")
                         return pie_chart_viz
                     else:
                         logger.warning(f"_create_pie_chart failed or returned non-dict: {type(pie_chart_viz)}, falling back.")
                else:
                     logger.warning("Data not suitable for pie chart (needs >= 2 columns), falling back.")
            except Exception as pie_err:
                 logger.error(f"Error during explicit pie chart creation: {pie_err}", exc_info=True)
                 # Fall through to default logic if explicit creation fails

        # --- Existing Check for Monthly Tourist Comparison ---
        if ("swiss" in query.lower() and "tourist" in query.lower() and 
            ("international" in query.lower() or "foreign" in query.lower()) and 
            "month" in query.lower() and 
            ("bar" in query.lower() or "chart" in query.lower() or "visuali" in query.lower())):
            
            logger.info("Creating monthly tourist comparison visualization")
            # Ensure data is suitable before calling
            if results and isinstance(results, list) and len(results) > 0:
                try:
                    monthly_comp_viz = self.create_monthly_tourist_comparison(results, query)
                    if monthly_comp_viz and isinstance(monthly_comp_viz, dict):
                        logger.info(f"Successfully created monthly tourist comparison visualization with type: {monthly_comp_viz.get('type')}")
                        return monthly_comp_viz
                    else:
                        logger.warning(f"create_monthly_tourist_comparison failed or returned non-dict: {type(monthly_comp_viz)}, falling back.")
                except Exception as comp_err:
                    logger.error(f"Error during monthly tourist comparison creation: {comp_err}", exc_info=True)
            else:
                 logger.warning("No data for monthly tourist comparison, falling back.")
        
        # --- Fallback/Default Logic --- 
        if not results or not isinstance(results, list) or not results:
            logger.warning(
                "Cannot create visualization: empty or invalid results")

            # Create a more user-friendly visualization for empty results
            try:
                # Create a Plotly figure with a centered "No Data Found"
                # message
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
                    font=dict(
                        size=14,
                        color="#888888"),
                    showarrow=False)

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
                    "data": json.loads(
                        json.dumps(
                            fig.to_dict(),
                            cls=PlotlyJSONEncoder)),
                    "no_data": True,
                    "query": query}
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
                debug_step = self.debug_service.start_step(
                    "visualization_creation", "Creating visualization based on query results")

            # Convert results to DataFrame
            df = pd.DataFrame(results)
            logger.debug(f"Initial DataFrame dtypes:\n{df.dtypes}")

            # ---> ADDED: Attempt numeric conversion for common value columns <---            
            potential_value_cols = [
                col for col in df.columns 
                if 'spending' in col.lower() or 
                   'visitors' in col.lower() or 
                   'amount' in col.lower() or 
                   'count' in col.lower() or 
                   'value' in col.lower() or
                   'total' in col.lower() or 
                   'sum' in col.lower() or 
                   'avg' in col.lower()
            ]
            
            for col in potential_value_cols:
                if df[col].dtype == 'object': # Only attempt if currently object type
                    try:
                        original_dtype = df[col].dtype
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        # Log only if dtype actually changed
                        if df[col].dtype != original_dtype:
                             logger.info(f"Converted column '{col}' from {original_dtype} to {df[col].dtype}")
                    except Exception as conv_err:
                        logger.warning(f"Could not convert column '{col}' to numeric: {conv_err}")
            logger.debug(f"DataFrame dtypes after numeric conversion attempt:\n{df.dtypes}")
            # ---> END ADDED SECTION <---

            # Special case: Handle single value results (1 row, 1 column)
            if len(df) == 1 and len(df.columns) == 1:
                logger.info(
                    f"Single value result detected: row={len(df)}, col={len(df.columns)}")
                # --- ADD LOGGING --- 
                logger.debug("Calling _create_single_value_visualization")
                single_value_viz = self._create_single_value_visualization(df, query)
                logger.debug(f"_create_single_value_visualization returned type: {single_value_viz.get('type') if isinstance(single_value_viz, dict) else type(single_value_viz)}")
                return single_value_viz # Return directly
                # --- END LOGGING ---

            # Fast path for small result sets - return as a table
            if len(df) <= 3 and len(df.columns) <= 3:
                logger.info(
                    f"Small result set ({len(df)} rows, {len(df.columns)} columns), returning as table")
                if self.debug_service and debug_step:
                    self.debug_service.end_step(
                        debug_step, "Created table visualization for small result set")
                return self._create_table(df, query)

            # Enhanced: Use hybrid visualization type selection
            viz_type = self._hybrid_visualization_selection(query, df)
            logger.info(
                f"Selected visualization type using hybrid approach: {viz_type}")

            # Create visualization based on determined type with enhanced error
            # handling
            result = self._create_visualization_with_fallbacks(
                df, query, viz_type)

            if self.debug_service and debug_step:
                self.debug_service.end_step(
                    debug_step, f"Created visualization (primary or fallback)")

            return result
        except Exception as e:
            # --- ADD LOGGING --- 
            logger.error(f"Exception in create_visualization main try block: {str(e)}", exc_info=True)
            # --- END LOGGING --- 
            if self.debug_service and debug_step:
                self.debug_service.end_step(
                    debug_step, f"Error creating visualization: {str(e)}", error=True)
            # Return table as fallback
            try:
                # --- ADD LOGGING --- 
                logger.warning("Attempting fallback table creation in create_visualization except block.")
                fallback_table = self._create_table(pd.DataFrame(results), query)
                logger.warning(f"Fallback table created with type: {fallback_table.get('type')}")
                return fallback_table
                # --- END LOGGING --- 
            except BaseException:
                # --- ADD LOGGING --- 
                logger.error("Fallback table creation FAILED.", exc_info=True)
                final_fallback = self._create_fallback_visualization(results, query, str(e))
                logger.error(f"Returning final fallback of type: {final_fallback.get('type')}")
                return final_fallback
                # --- END LOGGING --- 

    def _hybrid_visualization_selection(
            self, query: str, data: pd.DataFrame) -> str:
        """Select visualization type using a hybrid approach of query analysis and data pattern recognition"""
        try:
            query_lower = query.lower()
            
            # First check for explicit line chart requests
            if any(term in query_lower for term in ["line chart", "line graph", "trend line"]):
                logger.info("Detected explicit line chart request")
                return "time_series"
            
            # Check for trend-related terms that suggest line charts
            if any(term in query_lower for term in ["trend", "over time", "evolution", "pattern"]) and not any(term in query_lower for term in ["by month", "by year", "monthly", "yearly"]):
                logger.info("Detected trend-related terms suggesting line chart")
                return "time_series"
            
            # Special case for Swiss vs International tourists comparison
            if (('swiss' in query_lower and 'international' in query_lower or 
                 'swiss' in query_lower and 'foreign' in query_lower) and
                'tourist' in query_lower and
                'month' in query_lower and
                'swiss_tourists' in data.columns and
                'foreign_tourists' in data.columns):
                
                logger.info("Detected Swiss vs International tourists comparison request. Using bar chart.")
                return "bar"
                
            # Check if the query explicitly mentions a visualization type
            for viz_type, keywords in {
                "bar": ["bar chart", "bar graph", "histogram", "column chart"],
                "line": ["line chart", "line graph", "trend chart", "timeseries", "time series"],
                "pie": ["pie chart", "donut chart", "breakdown", "distribution"],
                "geo": ["map", "geographic", "spatial", "region map", "area map", "location"]
            }.items():
                for kw in keywords:
                    if kw in query_lower:
                        logger.info(f"Selected {viz_type} chart based on query keyword: {kw}")
                        return "time_series" if viz_type == "line" else viz_type
            
            # Try to determine type from data patterns
            data_based_type = self._select_viz_by_data_patterns(data, query)
            if data_based_type:
                logger.info(f"Selected {data_based_type} chart based on data patterns")
                return data_based_type
            
            # Fallback to traditional type selection
            fallback_type = self._determine_visualization_type(query, data)
            logger.info(f"Selected {fallback_type} chart as fallback")
            return fallback_type
            
        except Exception as e:
            logger.error(f"Error selecting visualization type: {str(e)}")
            return "table"  # Default to table if something goes wrong

    def _select_viz_by_data_patterns(
            self,
            df: pd.DataFrame,
            query: str) -> Optional[str]:
        """Select visualization type based on data patterns and characteristics"""
        try:
            # Get basic data characteristics
            num_rows = len(df)
            num_cols = len(df.columns)
            query_lower = query.lower()

            # Identify key column types
            numeric_cols = df.select_dtypes(
                include=[np.number]).columns.tolist()
            categorical_cols = df.select_dtypes(
                include=['object', 'string', 'category']).columns.tolist()
            date_cols = [
                col for col in df.columns if self._is_date_column(
                    df[col])]

            # Log the data profile
            logger.debug(
                f"Data profile - rows: {num_rows}, cols: {num_cols}, " +
                f"numeric: {len(numeric_cols)}, categorical: {len(categorical_cols)}, " +
                f"dates: {len(date_cols)}")

            # Time series pattern: Date column + numeric columns
            if date_cols and numeric_cols:
                logger.debug(f"Time series pattern matched (rows={num_rows})")
                # For monthly or yearly comparisons without trend focus, bar charts often work better
                if (any(term in query_lower for term in ["by month", "by year", "monthly", "yearly", "per month", "per year"]) and
                    not any(term in query_lower for term in ["trend", "over time", "evolution", "pattern", "line chart"])):
                    logger.debug("Choosing BAR chart for monthly/yearly comparison.")
                    return "bar"
                # Use line chart for time series with trend focus
                logger.debug("Choosing TIME_SERIES chart for time pattern.")
                return "time_series"

            # Check for bar chart keywords
            bar_chart_keywords = ["comparison", "compare", "rank", "ranking", "distribution"]
            has_bar_chart_keywords = any(keyword in query_lower for keyword in bar_chart_keywords)
            
            # Prioritize bar charts for specific data patterns
            if categorical_cols and numeric_cols and len(df[categorical_cols[0]].unique()) <= 20:
                # If there's any indication of comparison, use bar chart
                if has_bar_chart_keywords:
                    return "bar"
                # For a small number of categories, bar is often better for comparison
                if len(df[categorical_cols[0]].unique()) <= 10:
                    return "bar"

            # Distribution pattern: Single categorical with percentages or counts
            if (len(categorical_cols) == 1 and len(numeric_cols) == 1 and
                len(df[categorical_cols[0]].unique()) <= 10 and
                    ('percent' in query_lower or 'distribution' in query_lower or 'share' in query_lower)):
                return "pie"

            return None
            
        except Exception as e:
            logger.error(f"Error in _select_viz_by_data_patterns: {str(e)}")
            return None

    def _determine_visualization_type(
            self, query: str, data: pd.DataFrame) -> str:
        """Determine the most appropriate visualization type based on query and data characteristics"""
        query_lower = query.lower()
        
        # Get data characteristics
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        date_cols = [col for col in data.columns if self._is_date_column(data[col])]
        
        # Check for explicit line chart requests
        if "line chart" in query_lower or "line graph" in query_lower:
            if date_cols:
                return "time_series"
        
        # Check for time series/trend indicators
        time_trend_indicators = [
            "trend",
            "over time",
            "evolution",
            "changes",
            "growth",
            "decline",
            "pattern",
            "flow"
        ]

        if any(indicator in query_lower for indicator in time_trend_indicators):
            if date_cols:
                return "time_series"

        # Check for categorical comparisons
        comparison_indicators = [
            "compare",
            "comparison",
            "versus",
            "vs",
            "difference",
            "ranking",
            "rank",
            "top",
            "bottom"
        ]
        
        if any(indicator in query_lower for indicator in comparison_indicators):
            return "bar"

        # Default visualization based on data characteristics
        if date_cols and numeric_cols.any():
            # Default to time series for date-based data unless explicitly requesting monthly/yearly comparison
            if not any(term in query_lower for term in ["by month", "by year", "monthly", "yearly"]):
                return "time_series"
            return "bar"

        if len(numeric_cols) >= 1:
            return "bar"

        # Fallback to table for complex data
        return "table"

    def _create_time_series(self, df: pd.DataFrame,
                            query: str) -> Dict[str, Any]:
        """Create a time series visualization."""
        try:
            # Identify date and numeric columns
            # Find the most likely date column (prefer those with 'date', 'time', 'week', 'month', 'year')
            potential_date_cols = [col for col in df.columns 
                                   if self._is_date_column(df[col])]
            if not potential_date_cols:
                 raise ValueError("No date/datetime column found for time series.")
            
            # Simple heuristic: pick the first one found
            x_col = potential_date_cols[0] 
            logger.debug(f"Identified date column for time series: {x_col}")
            
            # Identify all numeric columns EXCEPT potentially the date column if it was numeric (e.g., year)
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            # Ensure the selected date column isn't accidentally included if it's also numeric
            y_cols = [col for col in numeric_cols if col != x_col]
            
            if not y_cols:
                raise ValueError("No suitable numeric columns found for time series values.")

            logger.debug(f"Using date column: {x_col}, value columns: {y_cols}")
            
            # Sort by date column
            df_sorted = df.sort_values(by=x_col)

            # Determine if we're dealing with hourly data
            is_hourly = 'hour' in query.lower() or (
                pd.api.types.is_datetime64_any_dtype(df_sorted[x_col]) and 
                df_sorted[x_col].dt.hour.nunique() > 1
            )

            # Create line chart - px.line handles multiple y-columns automatically
            logger.debug(f"Creating line chart with px.line(x='{x_col}', y={y_cols})")
            fig = px.line(
                df_sorted,
                x=x_col,
                y=y_cols, # Pass list of numeric columns
                title=self._extract_title_from_query(query) or "Time Series Trend",
                template="plotly_dark", # Use dark template
                markers=True # Add markers for fewer data points
            )

            # Update layout with specific formatting for hourly data
            if is_hourly:
                fig.update_xaxes(
                    tickformat="%Y-%m-%d %H:00",
                    dtick="H1",  # Show every hour
                    tickangle=45
                )

            # Enhance layout
            fig.update_layout(
                margin=dict(l=20, r=20, t=60, b=40),
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                xaxis_title="Time",
                yaxis_title="Number of Visitors"
            )

            return {
                "type": "plotly_json",
                "data": json.loads(json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)),
                "raw_data": df.to_dict('records')
            }

        except Exception as e:
            logger.error(f"Error creating time series: {str(e)}")
            return self._create_fallback_visualization(
                df.to_dict('records'), query, str(e))

    def _create_bar_chart(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a bar chart visualization"""
        try:
            # ---> ADDED LOGGING <-----
            logger.debug(f"_create_bar_chart received data. Dtypes:\n{df.dtypes}")
            logger.debug(f"First 5 rows:\n{df.head().to_string()}")
            
            # Check if we have year and quarter columns
            if 'year' in df.columns and 'quarter' in df.columns:
                # Create a combined period label
                df['period'] = df['year'].astype(str) + ' Q' + df['quarter'].astype(str)
                x_col = 'period'
                
                # Sort by year and quarter
                df = df.sort_values(['year', 'quarter'])
            else:
                # Continue with the original implementation for other cases
                # Identify numeric and categorical columns
                numeric_cols = df.select_dtypes(include=['float64', 'int64', 'float32', 'int32', 'int16', 'float16']).columns
                categorical_cols = df.select_dtypes(include=['object', 'string', 'category']).columns

                logger.debug(f"Identified numeric columns: {numeric_cols.tolist()}")
                logger.debug(f"Identified categorical columns: {categorical_cols.tolist()}")

                if len(numeric_cols) == 0:
                    logger.error("No numeric columns identified in DataFrame for bar chart.")
                    raise ValueError("No numeric columns found for bar chart values")

                # Use first categorical column for x-axis, first numeric for y-axis
                x_col = categorical_cols[0] if len(categorical_cols) > 0 else df.columns[0]

            # Get the numeric column for the y-axis
            y_col = next((col for col in df.columns if col not in [x_col, 'year', 'quarter'] 
                         and df[col].dtype in ['float64', 'int64', 'float32', 'int32', 'int16', 'float16']), None)
            
            if not y_col:
                logger.error("No suitable numeric column found for y-axis")
                raise ValueError("No suitable numeric column found for y-axis")
                
            logger.debug(f"Using x_col: {x_col}, y_col: {y_col}")

            # Create bar chart with enhanced styling
            fig = px.bar(
                df,
                x=x_col,
                y=y_col,
                title=self._extract_title_from_query(query) or f"Distribution of {self._format_column_name(y_col)}",
                template="plotly_dark",
                text=df[y_col].apply(lambda x: f"{x:,.0f}"),  # Add value labels
                height=500
            )

            # Enhance layout
            fig.update_layout(
                margin=dict(l=20, r=20, t=60, b=40),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(248,249,250,1)",
                font=dict(family="Arial, Helvetica, sans-serif", size=12),
                showlegend=False,
                xaxis=dict(
                    title=self._format_column_name(x_col),
                    tickangle=0,  # Keep labels horizontal for quarters
                    gridcolor="rgba(230,230,230,0.5)"
                ),
                yaxis=dict(
                    title=self._format_column_name(y_col),
                    gridcolor="rgba(230,230,230,0.5)",
                    tickformat=",.0f"
                ),
                bargap=0.2,
                title=dict(
                    font=dict(size=16, family="Arial, Helvetica, sans-serif", color="#333333"),
                    x=0.5,
                    xanchor='center'
                )
            )

            # Update traces for better hover info and appearance
            fig.update_traces(
                hovertemplate=f"{x_col}: %{{x}}<br>{y_col}: %{{y:,.0f}}<extra></extra>",
                marker_line_width=0,
                opacity=0.9,
                textposition='outside'
            )

            return {
                "type": "plotly_json",
                "data": json.loads(json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)),
                "raw_data": df.to_dict('records')
            }

        except Exception as e:
            logger.error(f"Error creating bar chart: {str(e)}")
            return self._create_fallback_visualization(df.to_dict('records'), query, str(e))

    def _create_pie_chart(self, df: pd.DataFrame,
                          query: str) -> Optional[Dict[str, Any]]:
        """Create a pie chart visualization."""
        try:
            logger.info(f"Attempting _create_pie_chart with columns: {df.columns.tolist()}")
            # Identify the numeric column for values
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) == 0:
                logger.warning("No numeric columns found for pie chart values.")
                return None

            # Use the first numeric column for values
            value_col = numeric_cols[0]
            logger.info(f"Identified value column for pie chart: {value_col}")

            # Identify the first non-numeric column for names (prefer object/string)
            potential_name_cols = df.select_dtypes(include=['object', 'string', 'category']).columns
            if len(potential_name_cols) > 0:
                 name_col = potential_name_cols[0]
            else:
                 # Fallback: use the first column that isn't the value column
                 name_col = next((col for col in df.columns if col != value_col), None)
            
            if not name_col:
                 logger.warning("Could not identify a suitable name column for pie chart.")
                 return None

            logger.info(f"Identified name column for pie chart: {name_col}")

            # Sort values in descending order for better visualization
            df_sorted = df.sort_values(by=value_col, ascending=False)
            
            # Limit slices for readability if necessary (e.g., top 10 + Other)
            max_slices = 10 
            if len(df_sorted) > max_slices:
                logger.info(f"More than {max_slices} slices, grouping smaller ones into 'Other'.")
                df_top = df_sorted.head(max_slices - 1)
                other_sum = df_sorted.iloc[max_slices - 1:][value_col].sum()
                df_other = pd.DataFrame([{name_col: 'Other', value_col: other_sum}])
                df_viz = pd.concat([df_top, df_other], ignore_index=True)
            else:
                df_viz = df_sorted

            # Create pie chart
            fig = px.pie(
                df_viz, # Use potentially grouped data
                values=value_col,
                names=name_col,
                title=self._extract_title_from_query(query) or f"Distribution by {self._format_column_name(name_col)}",
                template="plotly_dark",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Pastel)

            # Enhance layout
            fig.update_layout(
                margin=dict(l=20, r=20, t=60, b=20),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(17,17,17,0.1)",
                font=dict(color="white", size=12),
                showlegend=True, # Show legend for pie
                legend=dict(
                    traceorder="reversed", # Match typical pie order
                    font=dict(size=11)
                ),
                title=dict(
                    font=dict(size=16)
                )
            )

            # Update traces for better hover info and text display
            fig.update_traces(
                textposition='inside',
                textinfo='percent', # Show percentage inside
                hoverinfo='label+percent+value',
                hovertemplate="<b>%{label}</b><br>Value: %{value:,.0f}<br>Percentage: %{percent:.1%}<extra></extra>",
                textfont=dict(
                    size=11,
                    color="white"),
                marker=dict(line=dict(color='#000000', width=1)) # Add line between slices
                )
            
            logger.info(f"Successfully created pie chart figure for {name_col} / {value_col}")

            # Convert the figure to JSON properly
            fig_dict = fig.to_dict()
            fig_json = json.dumps(fig_dict, cls=PlotlyJSONEncoder)
            
            # Return with the proper structure
            return {
                "type": "plotly_json",
                "data": json.loads(fig_json),  # Use the properly serialized JSON 
                "raw_data": df.to_dict('records')
            }

        except Exception as e:
            logger.error(f"Error creating pie chart: {str(e)}", exc_info=True)
            # Return None if pie chart creation fails
            return None

    def _create_geo_chart(self, df: pd.DataFrame,
                          query: str) -> Dict[str, Any]:
        """Create a geographical visualization based on the data"""
        try:
            # Check if we have geographic data
            logger.info(
                f"Attempting to create geo chart with columns: {df.columns.tolist()}")

            # Look for region/location columns
            region_cols = [
                col for col in df.columns if any(
                    kw in col.lower() for kw in [
                        'region',
                        'location',
                        'city',
                        'canton',
                        'country',
                        'place'])]

            if not region_cols:
                logger.warning(
                    "No geographic columns found for geo visualization")
                return self._create_default_visualization(df, query)

            # Use the first region column
            region_col = region_cols[0]

            # Find value columns (numeric)
            numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
            if not numeric_cols:
                logger.warning(
                    "No numeric columns found for geo visualization")
                return self._create_default_visualization(df, query)

            # Use the first numeric column for values
            value_col = numeric_cols[0]

            # Enhanced: Check if we have lat/long coordinates for a map
            has_coords = any(
                c in df.columns for c in [
                    'latitude',
                    'longitude',
                    'lat',
                    'long',
                    'lng'])

            if has_coords:
                return self._create_map_visualization(
                    df, region_col, value_col, query)
            else:
                # Create region comparison bar chart
                return self._create_region_comparison(
                    df, region_col, value_col, query)

        except Exception as e:
            logger.error(f"Error creating geo chart: {str(e)}")
            return self._create_default_visualization(df, query)

    def _create_map_visualization(self,
                                  df: pd.DataFrame,
                                  region_col: str,
                                  value_col: str,
                                  query: str) -> Dict[str,
                                                      Any]:
        """Create a map-based visualization when coordinates are available"""
        try:
            # Identify lat/long columns
            lat_col = next(
                (c for c in df.columns if c.lower() in [
                    'latitude', 'lat']), None)
            lng_col = next(
                (c for c in df.columns if c.lower() in [
                    'longitude', 'long', 'lng']), None)

            if not lat_col or not lng_col:
                logger.warning("Couldn't identify latitude/longitude columns")
                return self._create_region_comparison(
                    df, region_col, value_col, query)

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

                hover_text.append(
                    f"<b>{region_name}</b><br>{value_col}: {value}{additional_info}")

            # Calculate marker sizes based on values
            max_value = df[value_col].max() if not df[value_col].empty else 1
            min_value = df[value_col].min() if not df[value_col].empty else 0
            size_range = (20, 50)  # Min and max marker sizes

            if max_value > min_value:
                # Normalize to the size range
                sizes = ((df[value_col] - min_value) / (max_value - min_value)
                         ) * (size_range[1] - size_range[0]) + size_range[0]
            else:
                # If all values are the same, use the middle size
                sizes = [size_range[0] +
                         (size_range[1] - size_range[0]) / 2] * len(df)

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
                title=self._extract_title_from_query(
                    query) or f"{value_col} by {region_col}",
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
            return self._create_region_comparison(
                df, region_col, value_col, query)

    def _create_region_comparison(self,
                                  df: pd.DataFrame,
                                  region_col: str,
                                  value_col: str,
                                  query: str) -> Dict[str,
                                                      Any]:
        """Create a bar chart comparing regions when coordinates aren't available"""
        try:
            # Limit to top 15 regions for readability
            if len(df) > 15:
                df = df.sort_values(by=value_col, ascending=False).head(15)

            # Group data by region if there are multiple rows per region
            if len(df[region_col].unique()) < len(df):
                grouped_df = df.groupby(region_col)[
                    value_col].sum().reset_index()
            else:
                grouped_df = df

            # Create a bar chart comparing regions
            fig = px.bar(
                grouped_df,
                x=region_col,
                y=value_col,
                title=self._extract_title_from_query(query) or f"{value_col} by {region_col}",
                color=value_col,
                color_continuous_scale='Viridis')

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
                        tickangle=45, tickmode='array', tickvals=list(
                            range(
                                len(grouped_df))), ticktext=grouped_df[region_col].astype(str).str.slice(
                            0, 15).tolist()))

            return {
                "type": "plotly",
                "data": json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)
            }

        except Exception as e:
            logger.error(f"Error creating region comparison: {str(e)}")
            return self._create_default_visualization(df, query)

    def _create_default_visualization(
            self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
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

    def _create_simple_bar_chart(
            self,
            df: pd.DataFrame,
            title: str = None) -> dict:
        # Identify numeric columns for y-axis
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        spending_cols = [
            col for col in numeric_cols if any(
                term in col.lower() for term in [
                    'spending', 'amount', 'total', 'value', 'count', 'sum'])]

        # Use spending column if available, otherwise use first numeric column
        y_column = spending_cols[0] if spending_cols else numeric_cols[0]

        # Identify categorical columns for x-axis, preferring region or
        # location
        categorical_cols = df.select_dtypes(include=['object']).columns
        region_cols = [
            col for col in categorical_cols if any(
                term in col.lower() for term in [
                    'region', 'location', 'city', 'country', 'category', 'name', 'type'])]
        x_column = region_cols[0] if region_cols else categorical_cols[0]

        # Sort values by spending in descending order for better visualization
        df_sorted = df.sort_values(by=y_column, ascending=False)
        
        # Limit to top 20 values if there are too many categories
        if len(df_sorted) > 20:
            df_sorted = df_sorted.head(20)
            suffix = " (Top 20)"
        else:
            suffix = ""

        # Create the enhanced bar chart
        fig = {
            "data": [{
                "type": "bar",
                "x": df_sorted[x_column].tolist(),
                "y": df_sorted[y_column].tolist(),
                "hovertemplate": f"{x_column}: %{{x}}<br>{y_column}: %{{y:,.2f}}<extra></extra>",
                "marker": {
                    "color": df_sorted[y_column].tolist(), 
                    "colorscale": "Blues",
                },
                "text": df_sorted[y_column].tolist(),
                "texttemplate": "%{y:,.0f}",
                "textposition": "outside",
            }],
            "layout": {
                "title": (title or f"{self._format_column_name(y_column)} by {self._format_column_name(x_column)}") + suffix,
                "xaxis": {
                    "title": self._format_column_name(x_column),
                    "tickangle": 45 if len(df_sorted) > 5 else 0,
                    "automargin": True,
                    "gridcolor": "rgba(230,230,230,0.5)",
                },
                "yaxis": {
                    "title": f"{self._format_column_name(y_column)}",
                    "automargin": True,
                    "gridcolor": "rgba(230,230,230,0.5)",
                    "zeroline": True,
                    "zerolinecolor": "rgba(0,0,0,0.2)",
                },
                "margin": {"t": 60, "l": 50, "r": 20, "b": 100},
                "showlegend": False,
                "template": "plotly_white",
                "hoverlabel": {"bgcolor": "white", "font": {"size": 12}},
                "bargap": 0.2,
                "height": 500,
            }
        }

        # ---> FIX: Return the standard structure <-----
        return {
            "type": "plotly_json",
            "data": json.loads(json.dumps(fig, cls=PlotlyJSONEncoder)), # fig is already the dict here
            "raw_data": df_sorted.to_dict('records')
        }
        # ---> END FIX <-----

    def _create_simple_line_chart(
            self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a simple Plotly line chart."""
        try:
            # Find the best x column (prefer date/time columns)
            date_cols = [
                col for col in df.columns if self._is_date_column(
                    df[col])]

            # If no date columns, look for columns that might be dates based on
            # name
            if not date_cols:
                potential_date_cols = [
                    col for col in df.columns if any(
                        term in str(col).lower() for term in [
                            "date", "time", "year", "month", "day"])]
                date_cols = potential_date_cols

            # If still no date columns, use the first column as x
            x_col = date_cols[0] if date_cols else df.columns[0]

            # Get numeric columns for y-axis (exclude the x column if it's
            # numeric)
            numeric_cols = df.select_dtypes(
                include=[np.number]).columns.tolist()
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
            non_null_values = [
                x for x in column if x is not None and not pd.isna(x)]
            if non_null_values and all(isinstance(
                    x, (datetime, date)) for x in non_null_values):
                return True

            # Try parsing a sample of the values
            try:
                sample = non_null_values[:5] if len(
                    non_null_values) > 5 else non_null_values
                if sample and all(
                    isinstance(
                        pd.to_datetime(x),
                        (pd.Timestamp)) for x in sample):
                    return True
            except BaseException:
                pass

        return False

    def _extract_title_from_query(self, query: str) -> Optional[str]:
        """Extract a title from the query for the visualization."""
        # Remove SQL keywords and common phrases
        keywords = [
            "select",
            "from",
            "where",
            "group by",
            "order by",
            "having",
            "show me",
            "display",
            "visualize"]
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

    def _create_fallback_visualization(
            self, results: List[Dict[str, Any]], query: str, error_msg: str) -> Dict[str, Any]:
        """Create a fallback visualization when all else fails."""
        try:
            logger.warning(
                f"Falling back to table visualization due to error: {error_msg}")

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
                "error": f"Visualization failed: {error_msg}. Additional error: {str(e)}"}

    def _create_table(self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a table visualization from the data"""
        return {
            "type": "table",
            "data": json.loads(df.to_json(orient="records", date_format="iso"))
        }

    def _create_visualization_with_fallbacks(
            self, df: pd.DataFrame, query: str, primary_viz_type: str) -> Dict[str, Any]:
        """Create visualization with fallback options if primary type fails."""
        try:
            # Try primary visualization type first
            if primary_viz_type == "time_series":
                result = self._create_time_series(df, query)
            elif primary_viz_type == "bar":
                result = self._create_bar_chart(df, query)
            elif primary_viz_type == "pie":
                result = self._create_pie_chart(df, query)
            elif primary_viz_type == "geo":
                result = self._create_geo_chart(df, query)
            elif primary_viz_type == "heatmap":
                result = self._create_heatmap(df, query)
            else:
                result = self._create_default_visualization(df, query)

            # Ensure proper formatting for frontend
            if result and result.get("type") == "plotly_json":
                fig_dict = result.get("data", {})
                # First, ensure fig_dict is a dictionary, not a string
                if isinstance(fig_dict, str):
                    try:
                        fig_dict = json.loads(fig_dict)
                        logger.debug("Converted string fig_dict to dictionary")
                    except json.JSONDecodeError:
                        logger.error("Failed to parse fig_dict string as JSON")
                        fig_dict = {} # Use empty dict as fallback

                # Update layout for consistent styling if possible
                if isinstance(fig_dict, dict) and "layout" in fig_dict:
                    layout = fig_dict["layout"]
                    # Ensure layout is a dictionary before updating
                    if isinstance(layout, str):
                        try:
                            layout = json.loads(layout)
                            logger.debug("Converted string layout to dictionary")
                        except json.JSONDecodeError:
                            logger.error("Failed to parse layout string as JSON, creating new layout")
                            layout = {} # Use empty dict as fallback
                    
                    # Now that we're sure layout is a dict, update it
                    if isinstance(layout, dict):
                        layout.update({
                            "template": "plotly_dark",
                            "paper_bgcolor": "rgba(0,0,0,0)",
                            "plot_bgcolor": "rgba(17,17,17,0.1)",
                            "font": {"color": "white", "size": 12},
                            "margin": {"l": 60, "r": 40, "t": 60, "b": 60},
                            "showlegend": True,
                            "legend": {
                                "bgcolor": "rgba(0,0,0,0.3)",
                                "bordercolor": "rgba(255,255,255,0.2)",
                                "borderwidth": 1,
                                "font": {"size": 11}
                            }
                        })
                        fig_dict["layout"] = layout
                    else:
                        logger.warning(f"Could not update layout, it is not a dictionary: {type(layout)}")

                # Ensure the data is properly JSON serialized
                try:
                    result["data"] = json.loads(
                        json.dumps(fig_dict, cls=PlotlyJSONEncoder))
                except Exception as json_err:
                    logger.error(f"Error serializing figure data: {json_err}", exc_info=True)
                    # Provide a minimal valid JSON object instead of failing completely
                    result["data"] = {"error": "Failed to serialize visualization data"}

            # ---> ADD LOGGING BEFORE FINAL RETURN <---            
            final_type = result.get('type') if isinstance(result, dict) else type(result)
            final_keys = result.keys() if isinstance(result, dict) else 'N/A'
            logger.debug(f"_create_visualization_with_fallbacks returning. Type: {final_type}, Keys: {final_keys}")
            # ---> END LOGGING <--- 
            return result

        except Exception as e:
            logger.error(
                f"Error in primary visualization, trying fallback: {str(e)}", exc_info=True)
            try:
                # Try simple bar chart as first fallback
                return self._create_simple_bar_chart(df)
            except Exception as fallback_err:
                logger.error(f"Simple bar chart fallback failed: {fallback_err}", exc_info=True)
                # If all else fails, return as table
                return self._create_table(df, query)

    def _create_single_value_visualization(
            self, df: pd.DataFrame, query: str) -> Dict[str, Any]:
        """Create a visualization for a single value result (1 row, 1 column)"""
        # --- ADD LOGGING --- 
        logger.debug("Inside _create_single_value_visualization")
        # --- END LOGGING --- 
        try:
            # Extract the value and column name
            column_name = df.columns[0]
            value = df.iloc[0, 0]
            logger.debug(f"Extracted single value: {value} (type: {type(value)}) for column: {column_name}")

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
            result_dict = {
                "type": "plotly_json",
                "data": json.loads(
                    json.dumps(
                        fig.to_dict(),
                        cls=PlotlyJSONEncoder)),
                "single_value": True,
                "column_name": column_name,
                "value": value
            }
            # --- ADD LOGGING --- 
            logger.debug("_create_single_value_visualization returning successfully.")
            return result_dict
            # --- END LOGGING --- 
        except Exception as e:
            # --- MODIFIED LOGGING --- 
            logger.error(
                f"Error creating single value visualization: {str(e)}", exc_info=True)
            # --- END LOGGING --- 
            # Fall back to table visualization
            # --- ADD LOGGING --- 
            logger.warning("Attempting fallback table creation from _create_single_value_visualization except block.")
            fallback_table = self._create_table(df, query)
            logger.warning(f"Fallback table created with type: {fallback_table.get('type')}")
            return fallback_table
            # --- END LOGGING ---

    def get_spending_heatmap_query():
        return """
        SELECT
            r.region_name,
            SUM(s.total_amount) as total_spending,
            COUNT(s.fact_id) as transaction_count,
            AVG(s.avg_transaction) as avg_transaction_amount,
            r.latitude,
            r.longitude
        FROM dw.fact_spending s
        JOIN dw.dim_region r ON s.region_id = r.region_id
        GROUP BY r.region_name, r.latitude, r.longitude
        ORDER BY total_spending DESC;
        """

    def _format_column_name(self, column_name):
        """Format column name for better readability in visualizations"""
        if not isinstance(column_name, str):
            return "Value"
            
        # Replace underscores with spaces
        formatted = column_name.replace('_', ' ')
        
        # Capitalize each word
        formatted = ' '.join(word.capitalize() for word in formatted.split())
        
        return formatted

    def create_monthly_tourist_comparison(self, data: List[Dict[str, Any]], query: str) -> Dict[str, Any]:
        """Create a bar chart comparing Swiss and international tourists by month"""
        try:
            logger.info(f"Creating monthly tourist comparison with data: {len(data)} rows")
            
            # If data already includes monthly data for Swiss and foreign tourists, use it
            if data and isinstance(data, list) and len(data) > 0:
                # Check if we have the necessary columns in the data
                first_item = data[0]
                has_month = 'month' in first_item or 'month_name' in first_item
                has_tourist_data = (
                    ('swiss_tourists' in first_item or 'total_swiss_tourists' in first_item) and 
                    ('foreign_tourists' in first_item or 'total_international_tourists' in first_item or 'total_foreign_tourists' in first_item)
                )
                
                if has_month and has_tourist_data:
                    df = pd.DataFrame(data)
                    logger.info(f"Using provided data for visualization with columns: {df.columns.tolist()}")
                else:
                    # Data doesn't have the right columns, fetch from database
                    logger.info("Data lacks required columns, fetching from database")
                    df = self._fetch_monthly_tourist_data()
            else:
                # No data provided, fetch from database
                logger.info("No data provided, fetching from database")
                df = self._fetch_monthly_tourist_data()
            
            if df is None or df.empty:
                logger.warning("No data available for monthly tourist comparison")
                return None
            
            # Create a grouped bar chart with Plotly
            fig = go.Figure()
            
            # Sort by month number if available
            if 'month' in df.columns and not df['month'].isna().all():
                df = df.sort_values('month')
            
            # Get x-axis values (either month_name or month)
            x_values = df['month_name'] if 'month_name' in df.columns else df['month']
            
            # Determine column names for Swiss and foreign tourists (handle different naming conventions)
            swiss_col = None
            foreign_col = None
            
            # Check for swiss tourists column
            for col in ['swiss_tourists', 'total_swiss_tourists']:
                if col in df.columns:
                    swiss_col = col
                    break
            
            # Check for foreign/international tourists column
            for col in ['foreign_tourists', 'total_foreign_tourists', 'total_international_tourists', 'international_tourists']:
                if col in df.columns:
                    foreign_col = col
                    break
            
            if not swiss_col or not foreign_col:
                logger.error(f"Required columns not found. Available columns: {df.columns.tolist()}")
                return None
            
            logger.info(f"Using columns: {swiss_col} and {foreign_col}")
            
            # Add bars for Swiss tourists
            fig.add_trace(go.Bar(
                x=x_values,
                y=df[swiss_col],
                name='Swiss Tourists',
                marker_color='#1E88E5',
                text=df[swiss_col].apply(lambda x: f"{int(x):,}"),
                textposition='outside'
            ))
            
            # Add bars for foreign/international tourists
            fig.add_trace(go.Bar(
                x=x_values,
                y=df[foreign_col],
                name='International Tourists',
                marker_color='#D81B60',
                text=df[foreign_col].apply(lambda x: f"{int(x):,}"),
                textposition='outside'
            ))
            
            # Extract year from query or use 2023 as default
            year_match = re.search(r'\b20\d{2}\b', query)
            year = year_match.group(0) if year_match else '2023'
            
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
                font=dict(size=14)
            )
            
            # Format the numbers properly
            fig.update_yaxes(tickformat=",d")
            
            # Return the figure as plotly_json for better frontend compatibility
            return {
                "type": "plotly_json",
                "data": json.loads(json.dumps(fig.to_dict(), cls=PlotlyJSONEncoder)),
                "raw_data": df.to_dict('records'),
                "single_value": False,
                "column_name": "tourists_comparison"
            }
        except Exception as e:
            logger.error(f"Error creating monthly tourist comparison: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    def _fetch_monthly_tourist_data(self, year: int = 2023) -> pd.DataFrame:
        """Fetch monthly Swiss and international tourist data from the database"""
        try:
            if self.db is None:
                logger.warning("No database session available for fetching monthly tourist data")
                return pd.DataFrame()
            
            # Execute SQL query to get monthly data
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
            
            result = self.db.execute(query).fetchall()
            
            if not result:
                logger.warning(f"No tourist data found for year {year}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(result)
            return df
            
        except Exception as e:
            logger.error(f"Error fetching monthly tourist data: {str(e)}")
            return pd.DataFrame()
