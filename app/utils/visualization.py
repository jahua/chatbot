import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import matplotlib.pyplot as plt
import base64
from io import BytesIO
import json
from app.utils.intent_parser import QueryIntent

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        import datetime
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
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

def generate_visualization(data: List[Dict[Any, Any]], intent=None) -> Optional[Any]:
    """Generate visualization based on data and intent (wrapper for compatibility)"""
    try:
        # Map intent to query type for visualization
        query = ""
        if intent == QueryIntent.VISITOR_COUNT:
            query = "visitor count"
        elif intent == QueryIntent.PEAK_PERIOD:
            query = "peak tourism periods"
        elif intent == QueryIntent.SPENDING_ANALYSIS:
            query = "spending analysis"
        elif intent == QueryIntent.VISITOR_COMPARISON:
            query = "visitor comparison"
        elif intent == QueryIntent.TREND_ANALYSIS:
            query = "visitor trends"
            
        # Create the visualization using existing function
        fig = create_visualization(data, query)
        
        # Convert figure to base64 for API response
        if fig:
            return figure_to_base64(fig)
        return None
    except Exception as e:
        logger.error(f"Error generating visualization: {str(e)}")
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