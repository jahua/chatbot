import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import matplotlib.pyplot as plt
import base64
from io import BytesIO
import json
from .intent_parser import QueryIntent
import plotly.graph_objects as go
import traceback
import numpy as np
from decimal import Decimal

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        import datetime
        
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            try:
                return base64.b64encode(obj).decode('utf-8')
            except:
                return str(obj)
        elif hasattr(obj, 'tolist'):
            return obj.tolist()
        elif hasattr(obj, 'to_dict'):
            return obj.to_dict()
        return super().default(obj)

def figure_to_base64(fig) -> Optional[str]:
    """Convert a matplotlib figure to base64 encoded string"""
    try:
        if fig is None:
            return None
            
        # Save figure to a temporary buffer.
        buf = BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', dpi=300)
        plt.close(fig)  # Close the figure to free memory
        
        # Encode the bytes as base64
        buf.seek(0)
        img_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        buf.close()
        
        return img_base64
        
    except Exception as e:
        logger.error(f"Error converting figure to base64: {str(e)}")
        return None

def generate_visualization(data: List[Dict[str, Any]], intent: QueryIntent) -> Dict[str, Any]:
    """Generate a visualization based on the intent and data"""
    try:
        # Detect and handle empty data
        if not data or len(data) == 0:
            logger.warning("No data available for visualization")
            return None
        
        # Route to specific visualization based on intent
        if intent == QueryIntent.VISITOR_COUNT:
            return _generate_visitor_count_visualization(data)
        elif intent == QueryIntent.PEAK_PERIOD:
            return _generate_peak_period_visualization(data)
        elif intent == QueryIntent.SPENDING_ANALYSIS:
            return _generate_spending_visualization(data)
        elif intent == QueryIntent.TREND_ANALYSIS:
            return _generate_trend_visualization(data)
        else:
            # Default visualization based on data structure
            return _generate_default_visualization(data)
    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}")
        return None

def create_visualization(data: List[Dict[Any, Any]], query: str) -> Optional[plt.Figure]:
    """Create visualization based on query results and type"""
    try:
        if not data:
            return None
            
        df = pd.DataFrame(data)
        
        # Create figure and axis
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Determine visualization type based on data and query
        if 'week_start' in df.columns:
            # Weekly pattern visualization
            df['week_start'] = pd.to_datetime(df['week_start'])
            ax.bar(df['week_start'], df['total_swiss_visitors'], label='Swiss Tourists', color='blue', alpha=0.7)
            ax.bar(df['week_start'], df['total_foreign_visitors'], bottom=df['total_swiss_visitors'],
                  label='Foreign Tourists', color='red', alpha=0.7)
            ax.set_xlabel('Week')
            ax.set_ylabel('Number of Visitors')
            ax.set_title('Weekly Visitor Patterns')
            
        elif len(df) <= 10 and 'aoi_date' in df.columns:
            # Top days visualization
            df['aoi_date'] = pd.to_datetime(df['aoi_date'])
            if 'swiss_tourists' in df.columns and 'foreign_tourists' in df.columns:
                ax.bar(df['aoi_date'], df['swiss_tourists'], label='Swiss Tourists', color='blue', alpha=0.7)
                ax.bar(df['aoi_date'], df['foreign_tourists'], bottom=df['swiss_tourists'],
                      label='Foreign Tourists', color='red', alpha=0.7)
            elif 'total_swiss_tourists' in df.columns and 'total_foreign_tourists' in df.columns:
                ax.bar(df['aoi_date'], df['total_swiss_tourists'], label='Swiss Tourists', color='blue', alpha=0.7)
                ax.bar(df['aoi_date'], df['total_foreign_tourists'], bottom=df['total_swiss_tourists'],
                      label='Foreign Tourists', color='red', alpha=0.7)
            else:
                ax.bar(df['aoi_date'], df['total_visitors'], color='blue', alpha=0.7)
            ax.set_xlabel('Date')
            ax.set_ylabel('Number of Visitors')
            ax.set_title('Top Tourism Days')
            
        elif 'month' in df.columns:
            # Monthly pattern visualization
            df['month'] = pd.to_datetime(df['month'])
            ax.plot(df['month'], df['swiss_tourists'], label='Swiss Tourists', marker='o')
            ax.plot(df['month'], df['foreign_tourists'], label='Foreign Tourists', marker='o')
            ax.set_xlabel('Month')
            ax.set_ylabel('Number of Visitors')
            ax.set_title('Monthly Visitor Patterns')
            
        else:
            # Default time series visualization
            if 'aoi_date' in df.columns:
                df['aoi_date'] = pd.to_datetime(df['aoi_date'])
                ax.plot(df['aoi_date'], df['total_visitors'], color='blue')
                ax.set_xlabel('Date')
                ax.set_ylabel('Number of Visitors')
                ax.set_title('Visitor Trends')
        
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)
        
        # Add legend if multiple series
        if len(ax.get_legend_handles_labels()[0]) > 0:
            ax.legend()
        
        # Adjust layout to prevent label cutoff
        plt.tight_layout()
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating visualization: {str(e)}")
        return None 

def _generate_spending_visualization(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate visualization for spending analysis data"""
    try:
        # Extract key data for visualization
        industries = [d.get('industry', 'Unknown') for d in data]
        spending_amounts = [float(d.get('total_spending', 0)) for d in data]
        percentages = [float(d.get('percentage_of_total', 0)) for d in data] if 'percentage_of_total' in data[0] else None
        
        # Create a bar chart with Plotly
        fig = go.Figure()
        
        # Add main bar for spending amounts
        fig.add_trace(go.Bar(
            x=industries,
            y=spending_amounts,
            name='Total Spending',
            marker_color='#4472C4'
        ))
        
        # Configure the layout
        fig.update_layout(
            title='Total Spending by Industry',
            xaxis_title='Industry',
            yaxis_title='Total Spending',
            template='plotly_white',
            height=500,
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
        # Format y-axis for currency values
        fig.update_yaxes(tickprefix='$', tickformat=',.0f')
        
        # Add a text annotation with percentage for each bar
        if percentages:
            for i, (industry, amt, pct) in enumerate(zip(industries, spending_amounts, percentages)):
                fig.add_annotation(
                    x=industry,
                    y=amt,
                    text=f"{pct}%",
                    showarrow=False,
                    yshift=10,
                    font=dict(size=12)
                )
        
        # Convert to JSON
        return fig.to_dict()
    except Exception as e:
        logger.error(f"Error generating spending visualization: {str(e)}")
        logger.error(traceback.format_exc())
        return None 

def _generate_visitor_count_visualization(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate visualization for visitor count data"""
    try:
        # Convert to pandas DataFrame for easier manipulation
        df = pd.DataFrame(data)
        
        logger.debug(f"Visitor count data columns: {df.columns.tolist()}")
        logger.debug(f"First row: {df.iloc[0].to_dict() if not df.empty else 'No data'}")
        
        # Handle missing columns
        if 'swiss_tourists' not in df.columns:
            logger.warning("Missing 'swiss_tourists' column in data")
            return None
            
        if 'foreign_tourists' not in df.columns:
            logger.warning("Missing 'foreign_tourists' column in data")
            return None
            
        if 'total_visitors' not in df.columns:
            # Calculate total if not present
            df['total_visitors'] = df['swiss_tourists'] + df['foreign_tourists']
        
        # Convert date string to datetime if it's not already
        if 'date' in df.columns:
            if df['date'].dtype == 'object':
                try:
                    df['date'] = pd.to_datetime(df['date'])
                except Exception as e:
                    logger.error(f"Error converting date: {str(e)}")
                    # If conversion fails, create a simple index
                    df['date'] = pd.RangeIndex(start=0, stop=len(df), step=1)
        else:
            # If no date column, create a simple index
            df['date'] = pd.RangeIndex(start=0, stop=len(df), step=1)
            
        # Create a line chart with Plotly
        fig = go.Figure()
        
        # Add lines for Swiss, foreign, and total tourists
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['swiss_tourists'],
            name='Swiss Tourists',
            mode='lines+markers',
            marker=dict(size=8),
            line=dict(width=2, color='blue')
        ))
        
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['foreign_tourists'],
            name='Foreign Tourists',
            mode='lines+markers',
            marker=dict(size=8),
            line=dict(width=2, color='red')
        ))
        
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['total_visitors'],
            name='Total Visitors',
            mode='lines+markers',
            marker=dict(size=8),
            line=dict(width=3, color='green')
        ))
        
        # Configure the layout
        fig.update_layout(
            title='Visitor Count Trends',
            xaxis_title='Date',
            yaxis_title='Number of Visitors',
            template='plotly_white',
            height=500,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
        # Return the figure as a dictionary
        return fig.to_dict()
    except Exception as e:
        logger.error(f"Error generating visitor count visualization: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def _generate_peak_period_visualization(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate visualization for peak period data"""
    try:
        # Extract key data for visualization
        dates = [d.get('date', '') for d in data]
        swiss_tourists = [float(d.get('swiss_tourists', 0)) for d in data]
        foreign_tourists = [float(d.get('foreign_tourists', 0)) for d in data]
        
        # Create a stacked bar chart with Plotly
        fig = go.Figure()
        
        # Add bars for Swiss tourists
        fig.add_trace(go.Bar(
            x=dates,
            y=swiss_tourists,
            name='Swiss Tourists',
            marker_color='blue'
        ))
        
        # Add bars for foreign tourists
        fig.add_trace(go.Bar(
            x=dates,
            y=foreign_tourists,
            name='Foreign Tourists',
            marker_color='red'
        ))
        
        # Configure the layout
        fig.update_layout(
            title='Peak Tourism Periods',
            xaxis_title='Date',
            yaxis_title='Number of Visitors',
            barmode='stack',
            template='plotly_white',
            height=500,
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
        # Return the figure as a dictionary
        return fig.to_dict()
    except Exception as e:
        logger.error(f"Error generating peak period visualization: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def _generate_trend_visualization(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate visualization for trend analysis data"""
    try:
        # Convert to pandas DataFrame for easier manipulation
        df = pd.DataFrame(data)
        
        # Convert date string to datetime if it's not already
        if 'date' in df.columns and isinstance(df['date'][0], str):
            df['date'] = pd.to_datetime(df['date'])
            
        # Create a line chart with Plotly
        fig = go.Figure()
        
        # Add lines for Swiss, foreign, and total tourists
        if 'swiss_tourists' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['swiss_tourists'],
                name='Swiss Tourists',
                mode='lines',
                line=dict(width=2, color='blue')
            ))
        
        if 'foreign_tourists' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['foreign_tourists'],
                name='Foreign Tourists',
                mode='lines',
                line=dict(width=2, color='red')
            ))
        
        if 'total_visitors' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['total_visitors'],
                name='Total Visitors',
                mode='lines',
                line=dict(width=3, color='green')
            ))
        
        # Configure the layout
        fig.update_layout(
            title='Visitor Trend Analysis',
            xaxis_title='Date',
            yaxis_title='Number of Visitors',
            template='plotly_white',
            height=500,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
        # Return the figure as a dictionary
        return fig.to_dict()
    except Exception as e:
        logger.error(f"Error generating trend visualization: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def _generate_default_visualization(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate a default visualization when intent-specific visualization is not available"""
    try:
        # Convert to pandas DataFrame for easier manipulation
        df = pd.DataFrame(data)
        
        # Handle date columns
        date_columns = [col for col in df.columns if col.lower() in ('date', 'aoi_date', 'txn_date')]
        if date_columns:
            date_col = date_columns[0]
            if isinstance(df[date_col][0], str):
                df[date_col] = pd.to_datetime(df[date_col])
                
            # Find numeric columns for visualization
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            
            # Create a basic line chart
            fig = go.Figure()
            
            for col in numeric_cols[:3]:  # Limit to first 3 numeric columns
                fig.add_trace(go.Scatter(
                    x=df[date_col],
                    y=df[col],
                    name=col.replace('_', ' ').title(),
                    mode='lines+markers'
                ))
            
            # Configure the layout
            fig.update_layout(
                title='Data Visualization',
                xaxis_title=date_col.replace('_', ' ').title(),
                yaxis_title='Value',
                template='plotly_white',
                height=500,
                margin=dict(l=50, r=50, t=80, b=50)
            )
            
        else:
            # If no date column, create a bar chart of the first numeric column
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            if not numeric_cols:
                return None
                
            # Use the first string column as x-axis if available
            x_col = next((col for col in df.columns if df[col].dtype == 'object'), df.index.name or 'index')
            y_col = numeric_cols[0]
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df[x_col] if x_col in df.columns else df.index,
                y=df[y_col],
                name=y_col.replace('_', ' ').title()
            ))
            
            # Configure the layout
            fig.update_layout(
                title='Data Visualization',
                xaxis_title=x_col.replace('_', ' ').title(),
                yaxis_title=y_col.replace('_', ' ').title(),
                template='plotly_white',
                height=500,
                margin=dict(l=50, r=50, t=80, b=50)
            )
        
        # Return the figure as a dictionary
        return fig.to_dict()
    except Exception as e:
        logger.error(f"Error generating default visualization: {str(e)}")
        logger.error(traceback.format_exc())
        return None 