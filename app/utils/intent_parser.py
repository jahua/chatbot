from typing import Dict, Any, List, Optional, Tuple
from enum import Enum, auto
import re
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)

class QueryIntent(Enum):
    """Enum for different query intents"""
    VISITOR_COMPARISON = "visitor_comparison"
    PEAK_PERIOD = "peak_period"
    SPENDING = "spending"
    TREND = "trend"
    REGION_ANALYSIS = "region_analysis"
    HOTSPOT_DETECTION = "hotspot_detection"
    SPATIAL_PATTERN = "spatial_pattern"
    INDUSTRY_ANALYSIS = "industry_analysis"
    VISITOR_COUNT = "visitor_count"
    GEO_SPATIAL = "geo_spatial"
    SPENDING_ANALYSIS = "spending_analysis"
    TREND_ANALYSIS = "trend_analysis"
    CORRELATION_ANALYSIS = "correlation_analysis"
    DEMOGRAPHIC_ANALYSIS = "demographic_analysis"
    UNKNOWN = "unknown"
    
    def __str__(self):
        return self.value

class TimeGranularity(Enum):
    HOUR = "hour"
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
        
        self.intent_patterns: Dict[QueryIntent, List[Tuple[str, float]]] = {
            QueryIntent.VISITOR_COUNT: [
                (r"\b(how many|number of|count of|total)\b.*\b(visitors?|tourists?|people)\b", 0.8),
                (r"\b(visitor|tourist)\b.*\b(statistics|numbers|figures)\b", 0.7),
                (r"\b(swiss|foreign)\b.*\b(tourists?|visitors?)\b", 0.9)
            ],
            QueryIntent.SPENDING_ANALYSIS: [
                (r"\b(spending|expenditure|amount|money)\b", 0.7),
                (r"\b(industry|sector)\b.*\b(spending|revenue|sales)\b", 0.9),
                (r"\b(highest|most|top)\b.*\b(spending|expenditure)\b", 0.8),
                (r"\b(transaction|purchase)\b.*\b(amount|value|size)\b", 0.7)
            ],
            QueryIntent.PEAK_PERIOD: [
                (r"\b(busiest|peak|highest)\b.*\b(time|period|season|week|month)\b", 0.9),
                (r"\b(when|which)\b.*\b(most|highest)\b.*\b(visitors?|tourists?)\b", 0.8),
                (r"\b(popular|busy)\b.*\b(times?|periods?|seasons?)\b", 0.7)
            ],
            QueryIntent.DEMOGRAPHIC_ANALYSIS: [
                (r"\b(demographics?|population)\b.*\b(breakdown|distribution|profile)\b", 0.9),
                (r"\b(age|gender)\b.*\b(groups?|distribution|ratio)\b", 0.8),
                (r"\b(who|what type of people)\b.*\b(visits?|comes?)\b", 0.7)
            ]
        }
        
        self.intent_keywords: Dict[QueryIntent, Dict[str, float]] = {
            QueryIntent.VISITOR_COUNT: {
                "visitor": 0.6, "tourist": 0.6, "people": 0.4,
                "count": 0.5, "number": 0.5, "total": 0.5,
                "swiss": 0.3, "foreign": 0.3
            },
            QueryIntent.SPENDING_ANALYSIS: {
                "spending": 0.7, "expenditure": 0.7, "amount": 0.6,
                "industry": 0.5, "sector": 0.5, "revenue": 0.6,
                "transaction": 0.5, "purchase": 0.5
            },
            QueryIntent.PEAK_PERIOD: {
                "busiest": 0.7, "peak": 0.7, "highest": 0.6,
                "period": 0.5, "season": 0.5, "week": 0.4,
                "month": 0.4, "time": 0.3
            },
            QueryIntent.DEMOGRAPHIC_ANALYSIS: {
                "demographic": 0.8, "population": 0.7,
                "age": 0.6, "gender": 0.6, "group": 0.4,
                "distribution": 0.5, "profile": 0.5
            }
        }
    
    def parse_query_intent(self, message: str) -> Dict[str, Any]:
        """Parse user message to determine intent and extract parameters"""
        try:
            message = message.lower()
            
            # Check for geospatial queries first
            geo_keywords = ['map', 'region', 'hotspot', 'spatial', 'geographic', 'location', 'area', 
                           'place', 'zone', 'territory', 'district', 'canton', 'where', 'ticino', 
                           'zurich', 'lugano', 'show me']
                           
            if any(keyword in message for keyword in geo_keywords):
                if 'hotspot' in message or 'busiest' in message or 'most visited' in message:
                    intent = QueryIntent.HOTSPOT_DETECTION
                elif ('region' in message or 'area' in message) and not any(term in message for term in ['pattern', 'distribution']):
                    intent = QueryIntent.REGION_ANALYSIS
                elif 'pattern' in message or 'distribution' in message or 'spread' in message:
                    intent = QueryIntent.SPATIAL_PATTERN
                else:
                    intent = QueryIntent.GEO_SPATIAL
                
                # Extract region information
                region_info = self._extract_region_info(message)
                
                return {
                    'intent': intent,
                    'time_range': self._extract_time_range(message),
                    'granularity': self._detect_time_granularity(message),
                    'region_info': region_info
                }
            
            # Check for spending/transaction queries
            spending_keywords = ['spending', 'spend', 'transaction', 'purchase', 'mastercard', 'txn', 
                                'amount', 'payment', 'money', 'currency', 'dollar', 'euro', 'chf', 
                                'bought', 'price', 'cost', 'expense', 'expenditure', 'industry']
                                
            if any(keyword in message for keyword in spending_keywords):
                return {
                    'intent': QueryIntent.SPENDING_ANALYSIS,
                    'time_range': self._extract_time_range(message),
                    'granularity': self._detect_time_granularity(message)
                }
            
            # Handle other query types
            if 'peak' in message or 'busiest' in message or 'top day' in message or 'most visitors' in message:
                intent = QueryIntent.PEAK_PERIOD
            elif 'compare' in message or 'versus' in message or 'vs' in message or 'difference between' in message:
                intent = QueryIntent.VISITOR_COMPARISON
            elif 'trend' in message or 'over time' in message or 'pattern' in message or 'change' in message:
                intent = QueryIntent.TREND_ANALYSIS
            else:
                intent = QueryIntent.VISITOR_COUNT
            
            return {
                'intent': intent,
                'time_range': self._extract_time_range(message),
                'granularity': self._detect_time_granularity(message),
                'comparison_type': self._determine_comparison_type(message)
            }
            
        except Exception as e:
            logger.error(f"Error parsing query intent: {str(e)}")
            return {'intent': QueryIntent.VISITOR_COUNT}
    
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
        if any(term in message for term in ["hourly", "hour", "hours", "per hour"]):
            return TimeGranularity.HOUR
        elif any(term in message for term in ["weekly", "week", "weeks"]):
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
    
    def _extract_region_info(self, query: str) -> Dict[str, str]:
        """Extract region information from the query"""
        region_info = {"region_name": None, "region_type": None}
        
        # Look for common region types followed by names
        region_patterns = [
            (r"(?:in|of|for|at)\s+(?:the\s+)?(\w+(?:\s+\w+){0,3}?)\s+(?:region|area|district|canton)", "region"),
            (r"(?:city|town)\s+of\s+(\w+(?:\s+\w+){0,2})", "city"),
            (r"(?:in|of|for|at)\s+(?:the\s+)?(\w+(?:\s+\w+){0,2})", "region")  # Generic fallback
        ]
        
        # Common Swiss regions
        known_regions = {
            "ticino": "canton",
            "tessin": "canton",
            "zurich": "canton",
            "zürich": "canton",
            "lucerne": "canton",
            "luzern": "canton",
            "geneva": "canton",
            "lugano": "city",
            "locarno": "city",
            "bellinzona": "city",
            "ascona": "city",
            "swiss alps": "region",
            "alps": "region", 
            "switzerland": "country"
        }
        
        # First check if any known regions are mentioned
        for region, region_type in known_regions.items():
            if region.lower() in query.lower():
                region_info["region_name"] = region
                region_info["region_type"] = region_type
                return region_info
        
        # If no known regions, try pattern matching
        for pattern, region_type in region_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                region_info["region_name"] = match.group(1).strip()
                region_info["region_type"] = region_type
                return region_info
        
        # Default to Ticino if no region found
        region_info["region_name"] = "Ticino"
        region_info["region_type"] = "canton"
        return region_info
    
    def _get_date_expression_for_granularity(self, granularity: TimeGranularity) -> str:
        """Get the appropriate SQL date expression for a given granularity"""
        if granularity == TimeGranularity.DAY:
            return "aoi_date::date"
        elif granularity == TimeGranularity.WEEK:
            return "date_trunc('week', aoi_date)::date"
        elif granularity == TimeGranularity.MONTH:
            return "date_trunc('month', aoi_date)::date"
        elif granularity == TimeGranularity.SEASON:
            # For seasons, we use a more complex expression with CASE statements
            return """
                CASE 
                    WHEN EXTRACT(MONTH FROM aoi_date) BETWEEN 3 AND 5 THEN CONCAT(EXTRACT(YEAR FROM aoi_date), '-Spring')
                    WHEN EXTRACT(MONTH FROM aoi_date) BETWEEN 6 AND 8 THEN CONCAT(EXTRACT(YEAR FROM aoi_date), '-Summer')
                    WHEN EXTRACT(MONTH FROM aoi_date) BETWEEN 9 AND 11 THEN CONCAT(EXTRACT(YEAR FROM aoi_date), '-Fall')
                    ELSE CONCAT(EXTRACT(YEAR FROM aoi_date), '-Winter')
                END
            """
        elif granularity == TimeGranularity.YEAR:
            return "EXTRACT(YEAR FROM aoi_date)::text"
        else:
            return "aoi_date::date"

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

    def detect_intent(self, query: str) -> Tuple[QueryIntent, float]:
        """
        Detect the intent of a query using pattern matching and keyword analysis
        
        Args:
            query: The natural language query string
            
        Returns:
            Tuple of (QueryIntent, confidence_score)
        """
        query = query.lower().strip()
        
        # Check pattern matches first
        pattern_scores: Dict[QueryIntent, float] = {}
        for intent, patterns in self.intent_patterns.items():
            max_pattern_score = 0.0
            for pattern, score in patterns:
                if re.search(pattern, query):
                    max_pattern_score = max(max_pattern_score, score)
            if max_pattern_score > 0:
                pattern_scores[intent] = max_pattern_score
        
        # If we have strong pattern matches, use the highest scoring one
        if pattern_scores:
            best_intent = max(pattern_scores.items(), key=lambda x: x[1])
            if best_intent[1] >= 0.8:
                return best_intent
        
        # Fall back to keyword analysis
        keyword_scores: Dict[QueryIntent, float] = {}
        query_words = set(query.split())
        
        for intent, keywords in self.intent_keywords.items():
            score = 0.0
            matches = 0
            for word, weight in keywords.items():
                if word in query_words:
                    score += weight
                    matches += 1
            if matches > 0:
                # Normalize score by number of matches
                keyword_scores[intent] = score / matches
        
        # Combine pattern and keyword scores
        combined_scores: Dict[QueryIntent, float] = {}
        all_intents = set(pattern_scores.keys()) | set(keyword_scores.keys())
        
        for intent in all_intents:
            pattern_score = pattern_scores.get(intent, 0.0)
            keyword_score = keyword_scores.get(intent, 0.0)
            # Weight pattern matches more heavily
            combined_scores[intent] = (pattern_score * 0.7) + (keyword_score * 0.3)
        
        if combined_scores:
            best_intent = max(combined_scores.items(), key=lambda x: x[1])
            if best_intent[1] >= 0.5:
                return best_intent
        
        return (QueryIntent.UNKNOWN, 0.0)

    def get_temporal_context(self, query: str) -> Dict[str, str]:
        """Extract temporal context from the query"""
        temporal_context = {}
        
        # Extract season
        seasons = {"spring": r"\b(spring)\b", 
                  "summer": r"\b(summer)\b",
                  "fall": r"\b(fall|autumn)\b", 
                  "winter": r"\b(winter)\b"}
        
        for season, pattern in seasons.items():
            if re.search(pattern, query.lower()):
                temporal_context["season"] = season
                
        # Extract year
        year_match = re.search(r"\b(20\d{2})\b", query)
        if year_match:
            temporal_context["year"] = year_match.group(1)
            
        # Extract month
        months = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12
        }
        
        for month_name, month_num in months.items():
            if re.search(fr"\b{month_name}\b", query.lower()):
                temporal_context["month"] = str(month_num)
                
        return temporal_context

    def get_geographic_context(self, query: str) -> Optional[str]:
        """Extract geographic context from the query"""
        # Add region extraction logic based on your available regions
        regions = ["zurich", "geneva", "bern", "basel", "lucerne"]
        for region in regions:
            if re.search(fr"\b{region}\b", query.lower()):
                return region
        return None

    def get_demographic_context(self, query: str) -> Dict[str, str]:
        """Extract demographic context from the query"""
        demographic_context = {}
        
        # Age groups
        age_patterns = {
            "young": r"\b(young|youth|teenagers?)\b",
            "adult": r"\b(adults?|middle-aged)\b",
            "senior": r"\b(seniors?|elderly|older)\b"
        }
        
        for age_group, pattern in age_patterns.items():
            if re.search(pattern, query.lower()):
                demographic_context["age_group"] = age_group
                
        # Gender
        gender_patterns = {
            "male": r"\b(males?|men)\b",
            "female": r"\b(females?|women)\b"
        }
        
        for gender, pattern in gender_patterns.items():
            if re.search(pattern, query.lower()):
                demographic_context["gender"] = gender
                
        return demographic_context 