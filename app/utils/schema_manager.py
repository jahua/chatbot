from typing import Dict, List, Optional, Tuple
import logging
from .intent_parser import IntentParser, QueryIntent
from .sql_template_manager import SQLTemplateManager

logger = logging.getLogger(__name__)

class SchemaManager:
    """
    Manages database schema information and coordinates between intent parsing
    and SQL query generation
    """
    
    def __init__(self):
        """Initialize schema manager with intent parser and SQL template manager"""
        self.intent_parser = IntentParser()
        self.sql_manager = SQLTemplateManager()
        
        # Define schema metadata
        self.tables = {
            "fact_visitor": {
                "description": "Contains visitor count data",
                "joins": ["dim_date", "dim_region", "dim_visitor"],
                "metrics": ["visitor_count"]
            },
            "fact_spending": {
                "description": "Contains spending data by industry",
                "joins": ["dim_date", "dim_industry", "dim_region"],
                "metrics": ["amount"]
            },
            "dim_date": {
                "description": "Date dimension with various time hierarchies",
                "attributes": ["year", "month", "season", "date"]
            },
            "dim_region": {
                "description": "Geographic regions",
                "attributes": ["region_name", "region_type"]
            },
            "dim_visitor": {
                "description": "Visitor demographics",
                "attributes": ["age_group", "gender", "nationality"]
            },
            "dim_industry": {
                "description": "Industry categories",
                "attributes": ["industry_name", "industry_type"]
            }
        }
        
        # Define common analysis patterns
        self.analysis_patterns = {
            QueryIntent.VISITOR_COUNT: {
                "required_tables": ["fact_visitor"],
                "common_joins": ["dim_date", "dim_region"],
                "typical_metrics": ["visitor_count"],
                "typical_dimensions": ["year", "month", "region_name"]
            },
            QueryIntent.SPENDING_ANALYSIS: {
                "required_tables": ["fact_spending"],
                "common_joins": ["dim_date", "dim_industry"],
                "typical_metrics": ["total_spending"],
                "typical_dimensions": ["industry_name", "year"]
            },
            QueryIntent.PEAK_PERIOD: {
                "required_tables": ["fact_visitor"],
                "common_joins": ["dim_date"],
                "typical_metrics": ["visitor_count"],
                "typical_dimensions": ["date"]
            },
            QueryIntent.DEMOGRAPHIC_ANALYSIS: {
                "required_tables": ["fact_visitor"],
                "common_joins": ["dim_visitor"],
                "typical_metrics": ["visitor_count"],
                "typical_dimensions": ["age_group", "gender"]
            }
        }

    def process_query(self, query: str) -> Tuple[str, Dict[str, str]]:
        """
        Process a natural language query to generate SQL
        
        Args:
            query: Natural language query string
            
        Returns:
            Tuple of (generated SQL query, debug information)
        """
        # Detect query intent and extract context
        intent, confidence = self.intent_parser.detect_intent(query)
        logger.info(f"Detected intent: {intent} with confidence: {confidence}")
        
        if confidence < 0.6:
            logger.warning(f"Low confidence in intent detection: {confidence}")
        
        # Extract various contexts
        temporal_context = self.intent_parser.get_temporal_context(query)
        geographic_context = self.intent_parser.get_geographic_context(query)
        demographic_context = self.intent_parser.get_demographic_context(query)
        
        # Combine contexts
        context = {
            **temporal_context,
            **geographic_context,
            **demographic_context
        }
        
        # Get required joins based on intent and context
        required_joins = self._determine_required_joins(intent, context)
        
        # Generate SQL query
        sql_query = self.sql_manager.generate_sql(
            intent=intent,
            context=context,
            required_joins=required_joins
        )
        
        # Prepare debug information
        debug_info = {
            "intent": intent.name,
            "confidence": str(confidence),
            "context": context,
            "required_joins": required_joins
        }
        
        return sql_query, debug_info

    def _determine_required_joins(self, intent: QueryIntent, 
                                context: Dict[str, str]) -> List[str]:
        """
        Determine which table joins are required based on intent and context
        
        Args:
            intent: Query intent
            context: Query context dictionary
            
        Returns:
            List of table names that need to be joined
        """
        required_joins = set()
        
        # Add common joins for the intent
        if intent in self.analysis_patterns:
            required_joins.update(self.analysis_patterns[intent]["common_joins"])
        
        # Add joins based on context
        if any(key in context for key in ["year", "month", "season"]):
            required_joins.add("dim_date")
            
        if "region" in context:
            required_joins.add("dim_region")
            
        if any(key in context for key in ["age_group", "gender"]):
            required_joins.add("dim_visitor")
            
        if "industry" in context:
            required_joins.add("dim_industry")
            
        return list(required_joins)

    def get_table_info(self, table_name: str) -> Dict[str, any]:
        """Get metadata for a specific table"""
        return self.tables.get(table_name, {})

    def get_analysis_pattern(self, intent: QueryIntent) -> Dict[str, any]:
        """Get common analysis pattern for a specific intent"""
        return self.analysis_patterns.get(intent, {}) 