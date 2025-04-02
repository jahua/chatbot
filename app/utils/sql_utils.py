from typing import List, Dict, Any, Optional
import json
from datetime import datetime, timedelta
import re

def build_date_range_filter(start_date: Optional[str] = None, end_date: Optional[str] = None) -> str:
    """Build a date range filter for SQL queries"""
    conditions = []
    if start_date:
        conditions.append(f"aoi_date >= '{start_date}'")
    if end_date:
        conditions.append(f"aoi_date <= '{end_date}'")
    return " AND ".join(conditions) if conditions else ""

def extract_json_field(field_name: str, json_path: str) -> str:
    """Extract a field from a JSON column using PostgreSQL JSON functions"""
    return f"jsonb_extract_path_text({field_name}, '{json_path}')"

def build_visitor_query(
    aoi_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    group_by: str = "month"
) -> str:
    """Build SQL query for visitor statistics"""
    query = """
    SELECT 
        DATE_TRUNC(:group_by, aoi_date) AS period,
        SUM((visitors->>'foreignTourist')::INT) AS foreign_tourists,
        SUM((visitors->>'swissTourist')::INT) AS swiss_tourists,
        SUM((visitors->>'foreignTourist')::INT + (visitors->>'swissTourist')::INT) AS total_visitors
    FROM 
        data_lake.aoi_days_raw
    WHERE 
        1=1
    """
    
    if aoi_id:
        query += " AND aoi_id = :aoi_id"
    if start_date:
        query += " AND aoi_date >= :start_date"
    if end_date:
        query += " AND aoi_date < :end_date"
        
    query += " GROUP BY period ORDER BY total_visitors DESC"
    
    return query

def build_spending_query(
    geo_name: Optional[str] = None,
    industry: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    group_by: List[str] = None
) -> str:
    """Build a query for spending statistics"""
    base_query = """
    SELECT 
        txn_date,
        geo_name,
        industry,
        SUM(txn_amt) as total_spending,
        SUM(txn_cnt) as transaction_count,
        AVG(avg_ticket) as average_ticket
    FROM data_lake.master_card
    """
    
    conditions = []
    if geo_name:
        conditions.append(f"geo_name = '{geo_name}'")
    if industry:
        conditions.append(f"industry = '{industry}'")
    
    date_filter = build_date_range_filter(start_date, end_date)
    if date_filter:
        conditions.append(date_filter.replace("aoi_date", "txn_date"))
    
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    if group_by:
        base_query += " GROUP BY " + ", ".join(group_by)
    
    base_query += " ORDER BY txn_date"
    return base_query

def build_demographics_query(
    aoi_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> str:
    """Build a query for demographic statistics"""
    base_query = """
    SELECT 
        aoi_date,
        jsonb_extract_path_text(demographics, 'male_proportion')::float as male_proportion,
        jsonb_extract_path_text(demographics, 'age_0_18')::float as age_0_18,
        jsonb_extract_path_text(demographics, 'age_19_35')::float as age_19_35,
        jsonb_extract_path_text(demographics, 'age_36_60')::float as age_36_60,
        jsonb_extract_path_text(demographics, 'age_61_plus')::float as age_61_plus
    FROM data_lake.aoi_days_raw
    """
    
    conditions = []
    if aoi_id:
        conditions.append(f"aoi_id = '{aoi_id}'")
    
    date_filter = build_date_range_filter(start_date, end_date)
    if date_filter:
        conditions.append(date_filter)
    
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    base_query += " ORDER BY aoi_date"
    return base_query

def build_origin_query(
    aoi_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    origin_type: str = "foreign"  # 'foreign', 'canton', or 'municipality'
) -> str:
    """Build a query for visitor origin statistics"""
    origin_field = {
        'foreign': 'top_foreign_countries',
        'canton': 'top_swiss_cantons',
        'municipality': 'top_swiss_municipalities'
    }.get(origin_type, 'top_foreign_countries')
    
    base_query = f"""
    SELECT 
        aoi_date,
        jsonb_array_elements({origin_field})->>'name' as origin_name,
        (jsonb_array_elements({origin_field})->>'visitors')::integer as visitor_count
    FROM data_lake.aoi_days_raw
    """
    
    conditions = []
    if aoi_id:
        conditions.append(f"aoi_id = '{aoi_id}'")
    
    date_filter = build_date_range_filter(start_date, end_date)
    if date_filter:
        conditions.append(date_filter)
    
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    base_query += " ORDER BY aoi_date, visitor_count DESC"
    return base_query

def extract_sql_query(response_text: str) -> Optional[str]:
    """
    Extract SQL query from LLM response text.
    Handles various formats including markdown code blocks and plain SQL.
    """
    # Try to find SQL in markdown code blocks
    sql_pattern = r'```(?:sql)?\s*(SELECT\s+.*?;)\s*```'
    match = re.search(sql_pattern, response_text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Try to find SQL without code blocks
    select_pattern = r'(SELECT\s+.*?;)'
    match = re.search(select_pattern, response_text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Try to find SQL with CTEs
    with_pattern = r'(WITH\s+.*?;)'
    match = re.search(with_pattern, response_text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    return None

def clean_sql_query(sql_query: str) -> str:
    """
    Clean SQL query by removing comments and extra whitespace.
    """
    # Remove SQL comments
    sql_query = re.sub(r'--.*$', '', sql_query, flags=re.MULTILINE)
    sql_query = re.sub(r'/\*.*?\*/', '', sql_query, flags=re.DOTALL)
    
    # Remove extra whitespace
    sql_query = re.sub(r'\s+', ' ', sql_query).strip()
    
    return sql_query 