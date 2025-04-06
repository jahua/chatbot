from typing import Dict, Any, List, Optional
from enum import Enum
import re
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class QueryIntent(Enum):
    VISITOR_COUNT = "visitor_count"
    VISITOR_COMPARISON = "visitor_comparison"
    SPENDING_ANALYSIS = "spending_analysis"
    CORRELATION_ANALYSIS = "correlation_analysis"
    PEAK_PERIOD = "peak_period"
    TREND_ANALYSIS = "trend_analysis"

class TimeGranularity(Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    SEASON = "season"
    YEAR = "year"

class IntentParser:
    """
    Parses user queries to determine intent and extract relevant parameters
    for SQL query generation.
    """
    
    def __init__(self):
        """Initialize the intent parser with pattern definitions"""
        # Define season date ranges
        self.seasons = {
            "spring": ("03-20", "06-20"),
            "summer": ("06-21", "09-22"),
            "autumn": ("09-23", "12-20"),
            "winter": ("12-21", "03-19")
        }
        
        # Define patterns for time expressions
        self.time_patterns = {
            "month": r"(january|february|march|april|may|june|july|august|september|october|november|december)",
            "season": r"(spring|summer|autumn|winter|fall)",
            "year": r"(20\d{2})",
            "week": r"week (\d{1,2})",
            "date_range": r"between .+ and .+",
            "specific_date": r"(on|at) .+"
        }
        
        # Define patterns for comparison expressions
        self.comparison_patterns = {
            "swiss_foreign": r"(swiss|domestic).+(foreign|international)",
            "time_comparison": r"compare.+between",
            "trend": r"(trend|pattern|change)",
            "peak": r"(peak|busiest|most)"
        }
    
    def parse_query_intent(self, query: str) -> Dict[str, Any]:
        """
        Parse the query to determine intent and extract parameters
        Returns a dictionary containing intent and relevant parameters
        """
        try:
            query = query.lower()
            
            # Initialize result
            result = {
                "intent": None,
                "time_range": self._extract_time_range(query),
                "granularity": self._determine_granularity(query),
                "comparison_type": None,
                "sql_components": {}
            }
            
            # Determine primary intent
            if any(pattern in query for pattern in ["compare", "vs", "versus", "ratio"]):
                result["intent"] = QueryIntent.VISITOR_COMPARISON
                result["comparison_type"] = self._determine_comparison_type(query)
                
            elif any(pattern in query for pattern in ["spend", "transaction", "purchase"]):
                result["intent"] = QueryIntent.SPENDING_ANALYSIS
                
            elif any(pattern in query for pattern in ["peak", "busiest", "most"]):
                result["intent"] = QueryIntent.PEAK_PERIOD
                
            elif any(pattern in query for pattern in ["trend", "pattern", "over time"]):
                result["intent"] = QueryIntent.TREND_ANALYSIS
                
            else:
                result["intent"] = QueryIntent.VISITOR_COUNT
            
            # Generate SQL components based on intent
            result["sql_components"] = self._generate_sql_components(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error parsing query intent: {str(e)}")
            return {"intent": None, "error": str(e)}
    
    def _extract_time_range(self, query: str) -> Dict[str, str]:
        """Extract time range information from the query"""
        time_range = {"start_date": None, "end_date": None}
        
        # Check for year
        year_match = re.search(r"20\d{2}", query)
        year = year_match.group() if year_match else "2023"  # Default to 2023
        
        # Check for season
        for season, (start_month, end_month) in self.seasons.items():
            if season in query:
                time_range["start_date"] = f"{year}-{start_month}"
                time_range["end_date"] = f"{year}-{end_month}"
                if season == "winter" and start_month > end_month:
                    time_range["end_date"] = f"{int(year)+1}-{end_month}"
                return time_range
        
        # Check for month
        month_names = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12"
        }
        for month_name, month_num in month_names.items():
            if month_name in query:
                time_range["start_date"] = f"{year}-{month_num}-01"
                # Calculate end date based on month
                if month_num == "12":
                    time_range["end_date"] = f"{int(year)+1}-01-01"
                else:
                    next_month = f"{int(month_num)+1:02d}"
                    time_range["end_date"] = f"{year}-{next_month}-01"
                return time_range
        
        # Default to full year if no specific time range found
        time_range["start_date"] = f"{year}-01-01"  # January 1st
        time_range["end_date"] = f"{int(year)+1}-01-01"  # January 1st of next year
        
        return time_range
    
    def _determine_granularity(self, query: str) -> TimeGranularity:
        """Determine the time granularity for the query"""
        query = query.lower()
        
        # Check for specific time units
        if any(term in query for term in ["daily", "day", "days"]):
            return TimeGranularity.DAY
        elif any(term in query for term in ["weekly", "week", "weeks"]):
            return TimeGranularity.WEEK
        elif any(term in query for term in ["monthly", "month", "months"]):
            return TimeGranularity.MONTH
        elif any(term in query for term in ["season", "seasonal", "spring", "summer", "autumn", "winter", "fall"]):
            return TimeGranularity.SEASON
        elif any(term in query for term in ["yearly", "year", "years"]):
            return TimeGranularity.YEAR
        
        # Default to month for period analysis
        if any(term in query for term in ["period", "peak", "trend", "pattern"]):
            return TimeGranularity.MONTH
        
        # Default to day for specific date queries
        return TimeGranularity.DAY
    
    def _determine_comparison_type(self, query: str) -> str:
        """Determine the type of comparison requested"""
        if any(pattern in query for pattern in ["swiss", "foreign", "domestic", "international"]):
            return "visitor_type"
        elif "spending" in query:
            return "spending"
        else:
            return "time"
    
    def _generate_sql_components(self, parsed_intent: Dict[str, Any]) -> Dict[str, str]:
        """Generate SQL components based on parsed intent"""
        components = {}
        
        # Determine date grouping based on granularity and intent
        if parsed_intent["intent"] == QueryIntent.PEAK_PERIOD:
            # For peak periods, use monthly grouping unless specifically asked for daily
            if parsed_intent.get("granularity") == TimeGranularity.DAY:
                date_grouping = "aoi_date"
            else:
                date_grouping = "DATE_TRUNC('month', aoi_date)"
        else:
            # For other queries, use standard granularity mapping
            granularity = parsed_intent.get("granularity", TimeGranularity.DAY)
            if granularity == TimeGranularity.DAY:
                date_grouping = "aoi_date"
            elif granularity == TimeGranularity.WEEK:
                date_grouping = "DATE_TRUNC('week', aoi_date)"
            elif granularity == TimeGranularity.MONTH:
                date_grouping = "DATE_TRUNC('month', aoi_date)"
            elif granularity == TimeGranularity.SEASON:
                date_grouping = "DATE_TRUNC('month', aoi_date)"
            elif granularity == TimeGranularity.YEAR:
                date_grouping = "DATE_TRUNC('year', aoi_date)"
            else:
                date_grouping = "aoi_date"
        
        # Build WHERE clause
        where_parts = []
        if parsed_intent["time_range"]["start_date"]:
            where_parts.append(f"aoi_date >= '{parsed_intent['time_range']['start_date']}'")
        if parsed_intent["time_range"]["end_date"]:
            where_parts.append(f"aoi_date < '{parsed_intent['time_range']['end_date']}'")
        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        
        # Generate components based on intent
        if parsed_intent["intent"] == QueryIntent.VISITOR_COMPARISON:
            components["select_clause"] = f"""
                SELECT 
                    {date_grouping} as date,
                    SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                    SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                    SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) as total_visitors,
                    CAST(100.0 * SUM((visitors->>'swissTourist')::numeric) / NULLIF(SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric), 0) AS DECIMAL(5,2)) as swiss_percentage,
                    CAST(100.0 * SUM((visitors->>'foreignTourist')::numeric) / NULLIF(SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric), 0) AS DECIMAL(5,2)) as foreign_percentage
            """
            components["from_clause"] = "FROM data_lake.aoi_days_raw"
            components["where_clause"] = where_clause
            components["group_by_clause"] = "GROUP BY date"
            components["order_by_clause"] = "ORDER BY date"
            
        elif parsed_intent["intent"] == QueryIntent.PEAK_PERIOD:
            # For peak periods, we want to identify the top periods and their relative rankings
            components["select_clause"] = f"""
                WITH period_stats AS (
                    SELECT 
                        {date_grouping}::date as date,
                        SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                        SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                        SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) as total_visitors,
                        RANK() OVER (ORDER BY SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) DESC) as period_rank
                    FROM data_lake.aoi_days_raw
                    {where_clause}
                    GROUP BY date
                )
                SELECT 
                    date,
                    swiss_tourists,
                    foreign_tourists,
                    total_visitors,
                    period_rank,
                    CAST(100.0 * total_visitors / SUM(total_visitors) OVER () AS DECIMAL(5,2)) as percentage_of_total
                FROM period_stats
                WHERE period_rank <= 10
                ORDER BY period_rank
            """
            components["from_clause"] = ""  # Already included in CTE
            components["where_clause"] = ""  # Already included in CTE
            components["group_by_clause"] = ""  # Not needed due to CTE
            components["order_by_clause"] = ""  # Already included in final SELECT
            
        elif parsed_intent["intent"] == QueryIntent.SPENDING_ANALYSIS:
            components["select_clause"] = """
                SELECT 
                    industry,
                    SUM(txn_amt) as total_spending,
                    COUNT(*) as transaction_count,
                    AVG(txn_amt) as average_transaction,
                    CAST(100.0 * SUM(txn_amt) / SUM(SUM(txn_amt)) OVER () AS DECIMAL(5,2)) as percentage_of_total_spending
            """
            components["from_clause"] = "FROM data_lake.master_card"
            components["where_clause"] = where_clause.replace("aoi_date", "txn_date")
            components["group_by_clause"] = "GROUP BY industry"
            components["order_by_clause"] = "ORDER BY total_spending DESC"
            
        else:  # Default to visitor count with trend analysis
            components["select_clause"] = f"""
                SELECT 
                    {date_grouping} as date,
                    SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                    SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                    SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric) as total_visitors,
                    LAG(SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric)) OVER (ORDER BY {date_grouping}) as previous_period_visitors,
                    CAST(100.0 * (
                        (SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric)) - 
                        LAG(SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric)) OVER (ORDER BY {date_grouping})
                    ) / NULLIF(LAG(SUM((visitors->>'swissTourist')::numeric) + SUM((visitors->>'foreignTourist')::numeric)) OVER (ORDER BY {date_grouping}), 0) AS DECIMAL(5,2)) as growth_rate
            """
            components["from_clause"] = "FROM data_lake.aoi_days_raw"
            components["where_clause"] = where_clause
            components["group_by_clause"] = "GROUP BY date"
            components["order_by_clause"] = "ORDER BY date"
        
        return components 