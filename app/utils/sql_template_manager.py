from typing import Dict, List, Optional
from enum import Enum
import logging
from .intent_parser import QueryIntent

logger = logging.getLogger(__name__)

class SQLTemplate:
    """Represents a SQL template with placeholders for dynamic values"""
    
    def __init__(self, base_query: str, required_tables: List[str], 
                 optional_joins: Dict[str, str], grouping_options: List[str]):
        self.base_query = base_query
        self.required_tables = required_tables
        self.optional_joins = optional_joins
        self.grouping_options = grouping_options

class SQLTemplateManager:
    """Manages SQL templates and generates queries based on intent and context"""
    
    def __init__(self):
        """Initialize SQL templates for different query intents"""
        self.templates: Dict[QueryIntent, SQLTemplate] = {
            QueryIntent.VISITOR_COUNT: SQLTemplate(
                base_query="""
                    SELECT {select_clause}
                    FROM fact_visitor v
                    {join_clause}
                    WHERE 1=1
                    {where_clause}
                    {group_by_clause}
                    {having_clause}
                    {order_by_clause}
                """,
                required_tables=["fact_visitor"],
                optional_joins={
                    "dim_date": "JOIN dim_date d ON v.date_id = d.date_id",
                    "dim_region": "JOIN dim_region r ON v.region_id = r.region_id",
                    "dim_visitor": "JOIN dim_visitor vis ON v.visitor_id = vis.visitor_id"
                },
                grouping_options=["d.year", "d.month", "d.season", "r.region_name", 
                                "vis.age_group", "vis.gender"]
            ),
            
            QueryIntent.SPENDING_ANALYSIS: SQLTemplate(
                base_query="""
                    SELECT {select_clause}
                    FROM fact_spending s
                    {join_clause}
                    WHERE 1=1
                    {where_clause}
                    {group_by_clause}
                    {having_clause}
                    {order_by_clause}
                """,
                required_tables=["fact_spending"],
                optional_joins={
                    "dim_date": "JOIN dim_date d ON s.date_id = d.date_id",
                    "dim_industry": "JOIN dim_industry i ON s.industry_id = i.industry_id",
                    "dim_region": "JOIN dim_region r ON s.region_id = r.region_id"
                },
                grouping_options=["d.year", "d.month", "d.season", "i.industry_name", 
                                "r.region_name"]
            ),
            
            QueryIntent.PEAK_PERIOD: SQLTemplate(
                base_query="""
                    WITH daily_stats AS (
                        SELECT {select_clause}
                        FROM fact_visitor v
                        {join_clause}
                        WHERE 1=1
                        {where_clause}
                        GROUP BY {group_by_clause}
                    )
                    SELECT *
                    FROM daily_stats
                    {having_clause}
                    {order_by_clause}
                """,
                required_tables=["fact_visitor"],
                optional_joins={
                    "dim_date": "JOIN dim_date d ON v.date_id = d.date_id",
                    "dim_region": "JOIN dim_region r ON v.region_id = r.region_id"
                },
                grouping_options=["d.date", "d.month", "d.year", "r.region_name"]
            ),
            
            QueryIntent.DEMOGRAPHIC_ANALYSIS: SQLTemplate(
                base_query="""
                    SELECT {select_clause}
                    FROM fact_visitor v
                    {join_clause}
                    WHERE 1=1
                    {where_clause}
                    {group_by_clause}
                    {having_clause}
                    {order_by_clause}
                """,
                required_tables=["fact_visitor"],
                optional_joins={
                    "dim_visitor": "JOIN dim_visitor vis ON v.visitor_id = vis.visitor_id",
                    "dim_date": "JOIN dim_date d ON v.date_id = d.date_id",
                    "dim_region": "JOIN dim_region r ON v.region_id = r.region_id"
                },
                grouping_options=["vis.age_group", "vis.gender", "vis.nationality",
                                "d.year", "d.season", "r.region_name"]
            )
        }
        
        # Common SQL components
        self.aggregations = {
            "count": "COUNT(*)",
            "sum": "SUM({field})",
            "avg": "AVG({field})",
            "max": "MAX({field})",
            "min": "MIN({field})"
        }
        
        self.time_filters = {
            "year": "d.year = {year}",
            "month": "d.month = {month}",
            "season": "d.season = '{season}'"
        }

    def generate_sql(self, intent: QueryIntent, context: Dict[str, str], 
                    required_joins: Optional[List[str]] = None) -> str:
        """
        Generate SQL query based on intent and context
        
        Args:
            intent: The query intent
            context: Dictionary containing query context (temporal, geographic, etc.)
            required_joins: List of table names that must be joined
            
        Returns:
            Generated SQL query string
        """
        if intent not in self.templates:
            raise ValueError(f"No template found for intent: {intent}")
            
        template = self.templates[intent]
        
        # Build components
        select_clause = self._build_select_clause(intent, context)
        join_clause = self._build_join_clause(template, required_joins)
        where_clause = self._build_where_clause(context)
        group_by_clause = self._build_group_by_clause(intent, context)
        having_clause = self._build_having_clause(context)
        order_by_clause = self._build_order_by_clause(intent, context)
        
        # Format query
        query = template.base_query.format(
            select_clause=select_clause,
            join_clause=join_clause,
            where_clause=where_clause,
            group_by_clause=group_by_clause,
            having_clause=having_clause,
            order_by_clause=order_by_clause
        )
        
        # Clean up query
        query = self._clean_query(query)
        logger.debug(f"Generated SQL query: {query}")
        
        return query

    def _build_select_clause(self, intent: QueryIntent, context: Dict[str, str]) -> str:
        """Build the SELECT clause based on intent and context"""
        clauses = []
        
        if intent == QueryIntent.VISITOR_COUNT:
            clauses.append("COUNT(*) as visitor_count")
            if "group_by" in context:
                clauses.append(context["group_by"])
                
        elif intent == QueryIntent.SPENDING_ANALYSIS:
            clauses.append("SUM(s.amount) as total_spending")
            if "group_by" in context:
                clauses.append(context["group_by"])
                
        elif intent == QueryIntent.PEAK_PERIOD:
            clauses.extend([
                "d.date",
                "COUNT(*) as visitor_count"
            ])
            
        elif intent == QueryIntent.DEMOGRAPHIC_ANALYSIS:
            clauses.extend([
                "vis.age_group",
                "vis.gender",
                "COUNT(*) as visitor_count"
            ])
            
        return ", ".join(clauses)

    def _build_join_clause(self, template: SQLTemplate, 
                          required_joins: Optional[List[str]] = None) -> str:
        """Build the JOIN clause"""
        joins = []
        
        # Add required joins
        if required_joins:
            for table in required_joins:
                if table in template.optional_joins:
                    joins.append(template.optional_joins[table])
                    
        return "\n".join(joins)

    def _build_where_clause(self, context: Dict[str, str]) -> str:
        """Build the WHERE clause based on context"""
        conditions = []
        
        # Add temporal filters
        if "year" in context:
            conditions.append(self.time_filters["year"].format(year=context["year"]))
        if "month" in context:
            conditions.append(self.time_filters["month"].format(month=context["month"]))
        if "season" in context:
            conditions.append(self.time_filters["season"].format(season=context["season"]))
            
        # Add geographic filters
        if "region" in context:
            conditions.append(f"r.region_name = '{context['region']}'")
            
        # Add demographic filters
        if "age_group" in context:
            conditions.append(f"vis.age_group = '{context['age_group']}'")
        if "gender" in context:
            conditions.append(f"vis.gender = '{context['gender']}'")
            
        return "\nAND ".join(conditions) if conditions else ""

    def _build_group_by_clause(self, intent: QueryIntent, context: Dict[str, str]) -> str:
        """Build the GROUP BY clause"""
        if "group_by" not in context:
            return ""
            
        groups = []
        for group in context["group_by"].split(","):
            group = group.strip()
            if group in self.templates[intent].grouping_options:
                groups.append(group)
                
        return "GROUP BY " + ", ".join(groups) if groups else ""

    def _build_having_clause(self, context: Dict[str, str]) -> str:
        """Build the HAVING clause"""
        if "having" not in context:
            return ""
        return "HAVING " + context["having"]

    def _build_order_by_clause(self, intent: QueryIntent, context: Dict[str, str]) -> str:
        """Build the ORDER BY clause"""
        if intent == QueryIntent.PEAK_PERIOD:
            return "ORDER BY visitor_count DESC LIMIT 10"
        elif "order_by" in context:
            return "ORDER BY " + context["order_by"]
        return ""

    def _clean_query(self, query: str) -> str:
        """Clean up the generated query"""
        # Remove empty clauses
        lines = [line for line in query.split("\n") if line.strip()]
        # Remove extra whitespace
        query = "\n".join(lines)
        # Remove multiple spaces
        query = " ".join(query.split())
        return query 