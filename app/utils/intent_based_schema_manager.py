from typing import Dict, Any, List, Optional, Set
import logging
from enum import Enum
from dataclasses import dataclass
from .intent_parser import QueryIntent

logger = logging.getLogger(__name__)

@dataclass
class TableContext:
    """Represents context for a table including its relationships and usage patterns"""
    name: str
    schema: str
    description: str
    primary_keys: List[str]
    columns: Dict[str, Dict[str, Any]]
    relationships: List[Dict[str, str]]
    common_joins: List[Dict[str, Any]]
    usage_patterns: Dict[str, List[str]]

class SchemaCategory(Enum):
    """Categories of schema information"""
    VISITOR_METRICS = "visitor_metrics"
    SPENDING_METRICS = "spending_metrics"
    TEMPORAL = "temporal"
    GEOGRAPHIC = "geographic"
    DEMOGRAPHIC = "demographic"
    INDUSTRY = "industry"

class IntentBasedSchemaManager:
    """
    Intelligent schema manager that provides context-aware schema information
    based on query intent and patterns.
    """
    
    def __init__(self):
        """Initialize the schema manager with table definitions and relationships"""
        self.tables: Dict[str, TableContext] = {}
        self.intent_patterns: Dict[QueryIntent, Set[SchemaCategory]] = {}
        self._initialize_schema()
        self._initialize_intent_patterns()
        
    def _initialize_schema(self):
        """Initialize schema with detailed table contexts"""
        # Initialize fact_visitor table
        self.tables["fact_visitor"] = TableContext(
            name="fact_visitor",
            schema="dw",
            description="Main fact table for visitor metrics",
            primary_keys=["fact_id"],
            columns={
                "fact_id": {"type": "integer", "description": "Primary key"},
                "date_id": {"type": "integer", "description": "Foreign key to dim_date"},
                "region_id": {"type": "integer", "description": "Foreign key to dim_region"},
                "total_visitors": {"type": "numeric", "description": "Total visitor count"},
                "swiss_tourists": {"type": "numeric", "description": "Count of Swiss tourists"},
                "foreign_tourists": {"type": "numeric", "description": "Count of foreign tourists"},
                "swiss_locals": {"type": "numeric", "description": "Count of Swiss locals"},
                "foreign_workers": {"type": "numeric", "description": "Count of foreign workers"},
                "swiss_commuters": {"type": "numeric", "description": "Count of Swiss commuters"},
                "demographics": {
                    "type": "jsonb",
                    "description": "Demographic information",
                    "json_fields": {
                        "age_groups": "Distribution across age groups",
                        "gender_ratio": "Male/female ratio"
                    }
                }
            },
            relationships=[
                {"table": "dim_date", "key": "date_id", "type": "many_to_one"},
                {"table": "dim_region", "key": "region_id", "type": "many_to_one"}
            ],
            common_joins=[
                {
                    "table": "dim_date",
                    "conditions": ["f.date_id = d.date_id"],
                    "common_filters": ["d.year", "d.month", "d.season"]
                },
                {
                    "table": "dim_region",
                    "conditions": ["f.region_id = r.region_id"],
                    "common_filters": ["r.region_name", "r.region_type"]
                }
            ],
            usage_patterns={
                "visitor_counts": [
                    "SUM(swiss_tourists + foreign_tourists) as total_tourists",
                    "SUM(total_visitors) as total_visitors"
                ],
                "tourist_ratio": [
                    "CAST(SUM(swiss_tourists) AS FLOAT) / NULLIF(SUM(swiss_tourists + foreign_tourists), 0) * 100 as swiss_tourist_percentage"
                ]
            }
        )

        # Initialize fact_spending table
        self.tables["fact_spending"] = TableContext(
            name="fact_spending",
            schema="dw",
            description="Main fact table for spending metrics",
            primary_keys=["fact_id"],
            columns={
                "fact_id": {"type": "integer", "description": "Primary key"},
                "date_id": {"type": "integer", "description": "Foreign key to dim_date"},
                "region_id": {"type": "integer", "description": "Foreign key to dim_region"},
                "industry_id": {"type": "integer", "description": "Foreign key to dim_industry"},
                "transaction_count": {"type": "integer", "description": "Number of transactions"},
                "total_amount": {"type": "numeric", "description": "Total spending amount"},
                "avg_transaction": {"type": "numeric", "description": "Average transaction amount"}
            },
            relationships=[
                {"table": "dim_date", "key": "date_id", "type": "many_to_one"},
                {"table": "dim_region", "key": "region_id", "type": "many_to_one"},
                {"table": "dim_industry", "key": "industry_id", "type": "many_to_one"}
            ],
            common_joins=[
                {
                    "table": "dim_date",
                    "conditions": ["fs.date_id = d.date_id"],
                    "common_filters": ["d.year", "d.month", "d.season"]
                },
                {
                    "table": "dim_industry",
                    "conditions": ["fs.industry_id = i.industry_id"],
                    "common_filters": ["i.industry_name", "i.industry_category"]
                }
            ],
            usage_patterns={
                "total_spending": [
                    "SUM(total_amount) as total_spending",
                    "AVG(avg_transaction) as average_transaction"
                ],
                "spending_distribution": [
                    "CAST(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER () AS DECIMAL(5,2)) as percentage_of_total"
                ]
            }
        )

    def _initialize_intent_patterns(self):
        """Initialize mapping between query intents and relevant schema categories"""
        self.intent_patterns = {
            QueryIntent.VISITOR_COUNT: {
                SchemaCategory.VISITOR_METRICS,
                SchemaCategory.TEMPORAL,
                SchemaCategory.GEOGRAPHIC
            },
            QueryIntent.SPENDING_ANALYSIS: {
                SchemaCategory.SPENDING_METRICS,
                SchemaCategory.INDUSTRY,
                SchemaCategory.TEMPORAL
            },
            QueryIntent.PEAK_PERIOD: {
                SchemaCategory.VISITOR_METRICS,
                SchemaCategory.TEMPORAL
            },
            QueryIntent.DEMOGRAPHIC_ANALYSIS: {
                SchemaCategory.VISITOR_METRICS,
                SchemaCategory.DEMOGRAPHIC
            }
        }

    def get_schema_for_intent(self, intent: QueryIntent, query: str) -> Dict[str, Any]:
        """
        Get relevant schema information based on query intent and content
        
        Args:
            intent: The detected query intent
            query: The original query text
            
        Returns:
            Dictionary containing relevant schema information
        """
        # Get relevant categories for this intent
        categories = self.intent_patterns.get(intent, set())
        
        # Build schema context
        schema_context = {
            "tables": {},
            "relationships": [],
            "common_patterns": {}
        }
        
        # Add relevant tables based on categories
        for table_name, table_context in self.tables.items():
            if self._is_table_relevant(table_context, categories, query):
                schema_context["tables"][table_name] = {
                    "schema": table_context.schema,
                    "description": table_context.description,
                    "columns": table_context.columns,
                    "common_joins": table_context.common_joins
                }
                
                # Add relationships for this table
                schema_context["relationships"].extend(table_context.relationships)
                
                # Add common usage patterns
                schema_context["common_patterns"].update(table_context.usage_patterns)
        
        return schema_context

    def _is_table_relevant(self, table: TableContext, categories: Set[SchemaCategory], query: str) -> bool:
        """Determine if a table is relevant for the given categories and query"""
        # Check if table contains columns relevant to the categories
        if SchemaCategory.VISITOR_METRICS in categories and any(
            col for col in table.columns if "tourist" in col or "visitor" in col
        ):
            return True
            
        if SchemaCategory.SPENDING_METRICS in categories and any(
            col for col in table.columns if "amount" in col or "transaction" in col
        ):
            return True
            
        if SchemaCategory.DEMOGRAPHIC in categories and "demographics" in table.columns:
            return True
            
        # Check query keywords against table description and column names
        query_terms = set(query.lower().split())
        table_terms = set(table.description.lower().split())
        column_terms = {col.lower() for col in table.columns.keys()}
        
        return bool(query_terms & (table_terms | column_terms))

    def get_join_conditions(self, tables: List[str]) -> List[str]:
        """Get the necessary join conditions for a set of tables"""
        joins = []
        for table_name in tables:
            if table_name in self.tables:
                table = self.tables[table_name]
                for join in table.common_joins:
                    joins.append(join["conditions"][0])
        return joins

    def get_common_patterns(self, intent: QueryIntent) -> Dict[str, List[str]]:
        """Get common SQL patterns for a given intent"""
        categories = self.intent_patterns.get(intent, set())
        patterns = {}
        
        for table in self.tables.values():
            if self._is_table_relevant(table, categories, ""):
                patterns.update(table.usage_patterns)
                
        return patterns 