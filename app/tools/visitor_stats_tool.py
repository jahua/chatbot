from typing import Dict, Any
from langchain.tools import Tool
from app.db.database import DatabaseService
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class VisitorStatsTool:
    def __init__(self, db: DatabaseService):
        self.db = db

    def get_visitor_stats(self, timeframe: str) -> Dict[str, Any]:
        """Get visitor statistics for a given timeframe"""
        try:
            query = """
                SELECT 
                    DATE_TRUNC(%s, aoi_date) as period,
                    SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                    SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                    SUM((visitors->>'swissTourist')::numeric + (visitors->>'foreignTourist')::numeric) as total_visitors
                FROM data_lake.aoi_days_raw
                WHERE aoi_date >= CURRENT_DATE - INTERVAL '1 year'
                GROUP BY period
                ORDER BY period;
            """
            
            results = self.db.execute_query(query, [timeframe])
            df = pd.DataFrame(results)
            
            return {
                "data": results,
                "dataframe": df,
                "summary": f"Retrieved visitor statistics grouped by {timeframe}"
            }
            
        except Exception as e:
            logger.error(f"Error getting visitor stats: {str(e)}")
            return {
                "error": str(e)
            }

def create_visitor_stats_tool(db: DatabaseService) -> Tool:
    """Create a LangChain tool for visitor statistics"""
    visitor_stats = VisitorStatsTool(db)
    
    return Tool(
        name="GetVisitorStats",
        func=visitor_stats.get_visitor_stats,
        description="""Get visitor statistics for Swiss and foreign tourists.
        Input should be a timeframe ('day', 'week', 'month', 'quarter', 'year').
        Returns visitor counts grouped by the specified timeframe.""",
        return_direct=True
    ) 