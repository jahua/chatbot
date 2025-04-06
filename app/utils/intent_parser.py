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
    
    def parse_query_intent(self, user_message: str) -> Dict[str, Any]:
        """
        Parse the user's message to determine the query intent, time range, and granularity
        Return a dictionary with the parsed intent information
        """
        try:
            # Convert message to lowercase for easier matching
            message = user_message.lower()
            
            # Initialize the result dictionary
            result = {
                "intent": QueryIntent.VISITOR_COUNT,  # Default intent
                "time_range": {},
                "granularity": self._detect_time_granularity(message),
                "comparison_type": None
            }
            
            # Detect spending analysis intent
            spending_keywords = ["spending", "spend", "transaction", "purchase", "revenue"]
            spending_match = any(keyword in message for keyword in spending_keywords)
            
            if spending_match:
                result["intent"] = QueryIntent.SPENDING_ANALYSIS
                
                # Check for industry focus for spending analysis
                if "industry" in message and ("highest" in message or "top" in message):
                    # Store the original message for context
                    result["original_message"] = user_message
            
            # If message contains "peak" keywords, set intent to peak period analysis
            peak_keywords = ["peak", "busiest", "most visited", "most popular", "highest attendance", "most tourists"]
            if any(keyword in message for keyword in peak_keywords):
                result["intent"] = QueryIntent.PEAK_PERIOD
            
            # Extract time range information (existing code)
            result["time_range"] = self._extract_time_range(message)
            
            # Generate SQL components for the intent
            result["sql_components"] = self._generate_sql_components(message, result["intent"], result["time_range"], result["granularity"])
            
            return result
        except Exception as e:
            logger.error(f"Error parsing query intent: {str(e)}")
            return {"error": f"Failed to parse your question: {str(e)}. Please try phrasing it differently."}
    
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
    
    def _detect_time_granularity(self, message: str) -> TimeGranularity:
        """Detect the time granularity from the message"""
        if any(term in message for term in ["weekly", "week", "weeks"]):
            return TimeGranularity.WEEK
        elif any(term in message for term in ["monthly", "month", "months"]):
            return TimeGranularity.MONTH
        elif any(term in message for term in ["yearly", "year", "annual"]):
            return TimeGranularity.YEAR
        elif any(term in message for term in ["season", "seasonal", "spring", "summer", "fall", "winter"]):
            return TimeGranularity.SEASON
        else:
            return TimeGranularity.DAY  # Default to daily granularity
    
    def _determine_comparison_type(self, query: str) -> str:
        """Determine the type of comparison requested"""
        if any(pattern in query for pattern in ["swiss", "foreign", "domestic", "international"]):
            return "visitor_type"
        elif "spending" in query:
            return "spending"
        else:
            return "time"
    
    def _generate_sql_components(self, message: str, intent: QueryIntent, time_range: Dict[str, str], granularity: TimeGranularity) -> Dict[str, str]:
        """Generate SQL components based on intent and parameters"""
        components = {}
        
        if intent == QueryIntent.SPENDING_ANALYSIS:
            components["select_clause"] = """
                SELECT 
                    industry,
                    SUM(txn_amt) as total_spending,
                    COUNT(*) as transaction_count,
                    AVG(txn_amt) as average_transaction,
                    CAST(100.0 * SUM(txn_amt) / SUM(SUM(txn_amt)) OVER () AS DECIMAL(5,2)) as percentage_of_total_spending
            """
            components["from_clause"] = "FROM data_lake.master_card"
            if time_range.get("start_date") and time_range.get("end_date"):
                components["where_clause"] = f"WHERE txn_date >= '{time_range['start_date']}' AND txn_date < '{time_range['end_date']}'"
            components["group_by_clause"] = "GROUP BY industry"
            components["order_by_clause"] = "ORDER BY total_spending DESC"
            
        elif intent == QueryIntent.VISITOR_COUNT:
            date_expression = self._get_date_expression_for_granularity(granularity)
            components["select_clause"] = f"""
                SELECT 
                    {date_expression} as date,
                    SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                    SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                    SUM((visitors->>'swissTourist')::numeric + (visitors->>'foreignTourist')::numeric) as total_visitors
            """
            components["from_clause"] = "FROM data_lake.aoi_days_raw"
            if time_range.get("start_date") and time_range.get("end_date"):
                components["where_clause"] = f"WHERE aoi_date >= '{time_range['start_date']}' AND aoi_date < '{time_range['end_date']}'"
            components["group_by_clause"] = "GROUP BY date"
            components["order_by_clause"] = "ORDER BY date"
            
        elif intent == QueryIntent.PEAK_PERIOD:
            date_expression = self._get_date_expression_for_granularity(granularity)
            components["select_clause"] = f"""
                SELECT 
                    {date_expression} as date,
                    SUM((visitors->>'swissTourist')::numeric) as swiss_tourists,
                    SUM((visitors->>'foreignTourist')::numeric) as foreign_tourists,
                    SUM((visitors->>'swissTourist')::numeric + (visitors->>'foreignTourist')::numeric) as total_visitors
            """
            components["from_clause"] = "FROM data_lake.aoi_days_raw"
            if time_range.get("start_date") and time_range.get("end_date"):
                components["where_clause"] = f"WHERE aoi_date >= '{time_range['start_date']}' AND aoi_date < '{time_range['end_date']}'"
            components["group_by_clause"] = "GROUP BY date"
            components["order_by_clause"] = "ORDER BY total_visitors DESC"
            components["limit_clause"] = "LIMIT 10"
            
        return components
        
    def _get_date_expression_for_granularity(self, granularity: TimeGranularity) -> str:
        """Get the appropriate SQL expression for the date based on granularity"""
        if granularity == TimeGranularity.DAY:
            return "aoi_date"
        elif granularity == TimeGranularity.WEEK:
            return "DATE_TRUNC('week', aoi_date)"
        elif granularity == TimeGranularity.MONTH:
            return "DATE_TRUNC('month', aoi_date)"
        elif granularity == TimeGranularity.SEASON:
            return "DATE_TRUNC('quarter', aoi_date)"
        elif granularity == TimeGranularity.YEAR:
            return "DATE_TRUNC('year', aoi_date)"
        else:
            return "aoi_date" 