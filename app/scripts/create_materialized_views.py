#!/usr/bin/env python
"""
Script to create the geo_insights schema and materialized views
using data from the master_card table.

This version includes:
- Connection timeout handling
- Simplified spatial patterns query
- The ability to resume from specific steps
- Better error handling for long-running queries
"""

import os
import sys
import logging
import psycopg2
import traceback
from pathlib import Path
import time
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add the parent directory to the path so we can import the app modules if needed
try:
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    parent_dir = script_dir.parent.parent
    sys.path.append(str(parent_dir))
    logger.info(f"Added {parent_dir} to sys.path")
except Exception as e:
    logger.warning(f"Could not modify sys.path or import app modules: {e}")

# Database connection info
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "336699")
DB_HOST = os.getenv("DB_HOST", "3.76.40.121")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "trip_dw")

# Connection timeout settings (in seconds)
CONNECT_TIMEOUT = 60        # Connection establishment timeout
STATEMENT_TIMEOUT = 1800    # 30 minutes per SQL statement
TCP_KEEPALIVES = True       # Enable TCP keepalives

# --- SQL Definitions ---

CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS geo_insights;"
ENABLE_POSTGIS_SQL = "CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;"

# Fixed check for bounding box validity
CHECK_BOUNDING_BOX_SQL = """
SELECT 
    COUNT(*) as total_rows,
    COUNT(bounding_box) as rows_with_bounding_box,
    COUNT(CASE WHEN bounding_box IS NOT NULL AND bounding_box <> '' THEN 1 END) as non_empty_bounding_box,
    COUNT(CASE WHEN 
        bounding_box IS NOT NULL AND 
        bounding_box <> '' AND
        bounding_box LIKE 'POLYGON%)'
    THEN 1 END) as valid_format_bounding_box
FROM data_lake.master_card;
"""

# Fixed: Simpler region summary without geometry conversion
CREATE_REGION_SUMMARY_SQL = """
CREATE MATERIALIZED VIEW geo_insights.region_summary AS
SELECT
    geo_type,
    geo_name,
    CONCAT(geo_type, '_', LOWER(REPLACE(geo_name, ' ', '_'))) AS region_id,
    SUM(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE 0 END)::FLOAT AS swiss_tourists,
    SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END)::FLOAT AS foreign_tourists,
    SUM(txn_cnt)::FLOAT AS total_visitors,
    AVG(central_latitude)::FLOAT AS central_latitude,
    AVG(central_longitude)::FLOAT AS central_longitude,
    COUNT(DISTINCT txn_date)::INTEGER AS days_with_data,
    MAX(txn_date) AS latest_date
FROM
    data_lake.master_card
WHERE
    geo_type IS NOT NULL AND geo_name IS NOT NULL
GROUP BY
    geo_type, geo_name
WITH DATA;
"""

INDEX_REGION_SUMMARY_SQL = """
CREATE INDEX IF NOT EXISTS idx_region_summary_name ON geo_insights.region_summary(geo_name);
CREATE INDEX IF NOT EXISTS idx_region_summary_type ON geo_insights.region_summary(geo_type);
CREATE INDEX IF NOT EXISTS idx_region_summary_id ON geo_insights.region_summary(region_id);
"""

# Fixed: Simple hotspots without advanced spatial functions
CREATE_REGION_HOTSPOTS_SQL = """
CREATE MATERIALIZED VIEW geo_insights.region_hotspots AS
SELECT
    CONCAT(geo_type, '_', LOWER(REPLACE(geo_name, ' ', '_'))) AS region_id,
    geo_name,
    geo_type,
    central_latitude AS latitude,
    central_longitude AS longitude,
    industry,
    SUM(txn_cnt)::FLOAT AS density,
    SUM(txn_amt)::FLOAT AS total_spend,
    COUNT(*)::INTEGER AS point_count,
    -- Simple clustering approach
    ROW_NUMBER() OVER (
        PARTITION BY geo_type, geo_name 
        ORDER BY SUM(txn_cnt) DESC
    ) AS cluster_id
FROM
    data_lake.master_card
WHERE 
    central_latitude IS NOT NULL AND 
    central_longitude IS NOT NULL AND
    central_latitude BETWEEN -90 AND 90 AND 
    central_longitude BETWEEN -180 AND 180 AND
    txn_cnt > 0
GROUP BY
    geo_type, geo_name, central_latitude, central_longitude, industry
ORDER BY
    region_id, density DESC
WITH DATA;
"""

INDEX_REGION_HOTSPOTS_SQL = """
CREATE INDEX IF NOT EXISTS idx_region_hotspots_id ON geo_insights.region_hotspots(region_id);
CREATE INDEX IF NOT EXISTS idx_region_hotspots_industry ON geo_insights.region_hotspots(industry);
"""

# Fixed: Industry insights (no geometry processing)
CREATE_INDUSTRY_INSIGHTS_SQL = """
CREATE MATERIALIZED VIEW geo_insights.industry_insights AS
SELECT
    geo_type,
    geo_name,
    CONCAT(geo_type, '_', LOWER(REPLACE(geo_name, ' ', '_'))) AS region_id,
    industry,
    SUM(txn_cnt)::FLOAT AS transaction_count,
    SUM(txn_amt)::FLOAT AS total_spend,
    -- Better NULL handling
    CASE 
        WHEN SUM(txn_cnt) > 0 THEN SUM(txn_amt)/SUM(txn_cnt)
        ELSE 0 
    END::FLOAT AS avg_transaction_value,
    COUNT(DISTINCT txn_date)::INTEGER AS active_days,
    ROW_NUMBER() OVER (PARTITION BY geo_type, geo_name ORDER BY SUM(txn_cnt) DESC) AS industry_rank
FROM
    data_lake.master_card
WHERE
    industry <> 'Total Retail' AND
    industry IS NOT NULL
GROUP BY
    geo_type, geo_name, industry
WITH DATA;
"""

INDEX_INDUSTRY_INSIGHTS_SQL = """
CREATE INDEX IF NOT EXISTS idx_industry_insights_id ON geo_insights.industry_insights(region_id);
CREATE INDEX IF NOT EXISTS idx_industry_insights_industry ON geo_insights.industry_insights(industry);
CREATE INDEX IF NOT EXISTS idx_industry_insights_rank ON geo_insights.industry_insights(industry_rank);
"""

# HEAVILY SIMPLIFIED spatial patterns to avoid timeout
CREATE_SPATIAL_PATTERNS_SQL = """
-- First create region centers as a separate view to break up the workload
CREATE MATERIALIZED VIEW IF NOT EXISTS geo_insights.region_centers AS
SELECT
    geo_type,
    geo_name,
    CONCAT(geo_type, '_', LOWER(REPLACE(geo_name, ' ', '_'))) AS region_id,
    AVG(central_latitude)::FLOAT AS avg_lat,
    AVG(central_longitude)::FLOAT AS avg_lon,
    COUNT(*)::INTEGER AS point_count
FROM
    data_lake.master_card
WHERE
    central_latitude IS NOT NULL AND 
    central_longitude IS NOT NULL AND
    central_latitude BETWEEN -90 AND 90 AND 
    central_longitude BETWEEN -180 AND 180
GROUP BY
    geo_type, geo_name
WITH DATA;

-- Now create a very simplified spatial patterns view that doesn't attempt complex calculations
CREATE MATERIALIZED VIEW geo_insights.spatial_patterns AS
SELECT
    rc.region_id,
    rc.geo_name,
    rc.geo_type,
    AVG(m.txn_cnt)::FLOAT AS avg_activity,
    array_agg(DISTINCT m.industry) FILTER (WHERE m.industry IS NOT NULL) AS industries,
    rc.point_count AS total_points
FROM
    geo_insights.region_centers rc
JOIN
    data_lake.master_card m ON rc.geo_type = m.geo_type AND rc.geo_name = m.geo_name
WHERE
    m.txn_cnt > 0
GROUP BY
    rc.region_id, rc.geo_name, rc.geo_type, rc.point_count
WITH DATA;
"""

INDEX_SPATIAL_PATTERNS_SQL = """
CREATE INDEX IF NOT EXISTS idx_region_centers_id ON geo_insights.region_centers(region_id);
CREATE INDEX IF NOT EXISTS idx_spatial_patterns_id ON geo_insights.spatial_patterns(region_id);
"""

# Fixed: Temporal insights (no geometry processing)
CREATE_TEMPORAL_INSIGHTS_SQL = """
CREATE MATERIALIZED VIEW geo_insights.temporal_insights AS
SELECT
    geo_type,
    geo_name,
    CONCAT(geo_type, '_', LOWER(REPLACE(geo_name, ' ', '_'))) AS region_id,
    EXTRACT(MONTH FROM txn_date)::INTEGER AS month,
    EXTRACT(YEAR FROM txn_date)::INTEGER AS year,
    SUM(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE 0 END)::FLOAT AS swiss_tourists,
    SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END)::FLOAT AS foreign_tourists,
    SUM(txn_cnt)::FLOAT AS total_visitors,
    SUM(txn_amt)::FLOAT AS total_spend
FROM
    data_lake.master_card
WHERE
    txn_date IS NOT NULL AND
    geo_type IS NOT NULL AND
    geo_name IS NOT NULL
GROUP BY
    geo_type, geo_name, EXTRACT(MONTH FROM txn_date), EXTRACT(YEAR FROM txn_date)
WITH DATA;
"""

INDEX_TEMPORAL_INSIGHTS_SQL = """
CREATE INDEX IF NOT EXISTS idx_temporal_insights_id ON geo_insights.temporal_insights(region_id);
CREATE INDEX IF NOT EXISTS idx_temporal_insights_month ON geo_insights.temporal_insights(month);
CREATE INDEX IF NOT EXISTS idx_temporal_insights_year ON geo_insights.temporal_insights(year);
"""

CREATE_REFRESH_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION geo_insights.refresh_all_views()
RETURNS void AS $$
BEGIN
    RAISE NOTICE 'Refreshing geo_insights views...';
    REFRESH MATERIALIZED VIEW geo_insights.region_summary;
    REFRESH MATERIALIZED VIEW geo_insights.region_hotspots;
    REFRESH MATERIALIZED VIEW geo_insights.industry_insights;
    REFRESH MATERIALIZED VIEW geo_insights.region_centers;
    REFRESH MATERIALIZED VIEW geo_insights.spatial_patterns;
    REFRESH MATERIALIZED VIEW geo_insights.temporal_insights;
    RAISE NOTICE 'Geo_insights views refreshed.';
END;
$$ LANGUAGE plpgsql;
"""

# Combined spatial and economic insights view
CREATE_COMBINED_INSIGHTS_SQL = """
CREATE MATERIALIZED VIEW geo_insights.combined_spatial_analysis AS
WITH base_data AS (
    SELECT
        geo_type,
        geo_name,
        bounding_box,
        industry,
        segment,
        txn_date,
        txn_cnt,
        txn_amt,
        central_latitude,
        central_longitude
    FROM data_lake.master_card
    WHERE 
        bounding_box IS NOT NULL
        AND central_latitude IS NOT NULL
        AND central_longitude IS NOT NULL
),
spatial_metrics AS (
    SELECT
        geo_type,
        geo_name,
        bounding_box,
        COUNT(*) as total_points,
        SUM(txn_cnt) as total_transactions,
        SUM(txn_amt) as total_spend,
        COUNT(DISTINCT industry) as industry_count,
        ST_Area(bounding_box::geometry) as area_sq_km,
        SUM(txn_cnt) / NULLIF(ST_Area(bounding_box::geometry), 0) as transaction_density
    FROM base_data
    GROUP BY geo_type, geo_name, bounding_box
),
tourist_metrics AS (
    SELECT
        geo_type,
        geo_name,
        EXTRACT(MONTH FROM txn_date) as month,
        SUM(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE 0 END) as swiss_tourists,
        SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END) as foreign_tourists,
        SUM(txn_cnt) as total_visitors
    FROM base_data
    GROUP BY geo_type, geo_name, EXTRACT(MONTH FROM txn_date)
),
industry_metrics AS (
    SELECT
        geo_type,
        geo_name,
        industry,
        SUM(txn_cnt) as industry_transactions,
        SUM(txn_amt) as industry_spend,
        COUNT(*) as industry_points,
        ROW_NUMBER() OVER (PARTITION BY geo_type, geo_name ORDER BY SUM(txn_cnt) DESC) as industry_rank
    FROM base_data
    GROUP BY geo_type, geo_name, industry
),
industry_clusters AS (
    SELECT
        geo_type,
        geo_name,
        array_agg(industry ORDER BY industry_transactions DESC) as top_industries,
        array_agg(industry_transactions ORDER BY industry_transactions DESC) as industry_volumes
    FROM industry_metrics
    WHERE industry_rank <= 5
    GROUP BY geo_type, geo_name
)
SELECT
    sm.geo_type,
    sm.geo_name,
    sm.bounding_box,
    sm.total_points,
    sm.total_transactions,
    sm.total_spend,
    sm.industry_count,
    sm.area_sq_km,
    sm.transaction_density,
    tm.month,
    tm.swiss_tourists,
    tm.foreign_tourists,
    tm.total_visitors,
    ic.top_industries,
    ic.industry_volumes,
    ST_Centroid(sm.bounding_box::geometry) as centroid,
    ST_Envelope(sm.bounding_box::geometry) as envelope
FROM spatial_metrics sm
LEFT JOIN tourist_metrics tm ON sm.geo_type = tm.geo_type AND sm.geo_name = tm.geo_name
LEFT JOIN industry_clusters ic ON sm.geo_type = ic.geo_type AND sm.geo_name = ic.geo_name
WITH DATA;
"""

INDEX_COMBINED_INSIGHTS_SQL = """
CREATE INDEX IF NOT EXISTS idx_combined_insights_geo ON geo_insights.combined_spatial_analysis(geo_type, geo_name);
CREATE INDEX IF NOT EXISTS idx_combined_insights_bbox ON geo_insights.combined_spatial_analysis USING GIST(bounding_box::geometry);
CREATE INDEX IF NOT EXISTS idx_combined_insights_centroid ON geo_insights.combined_spatial_analysis USING GIST(centroid);
"""

# Create a materialized view specifically for choropleth mapping
CREATE_CHOROPLETH_ANALYTICS_SQL = """
CREATE MATERIALIZED VIEW geo_insights.choropleth_analytics AS
WITH region_boundaries AS (
    -- Get base region information from shapefile data
    SELECT DISTINCT
        BZNAME as region_name,
        AREA_HA as area_ha,
        ST_X(ST_Centroid(geometry::geometry)) as longitude,
        ST_Y(ST_Centroid(geometry::geometry)) as latitude
    FROM 
        data_lake.master_card
    WHERE 
        BZNAME IS NOT NULL
),
tourism_metrics AS (
    -- Calculate tourism metrics
    SELECT 
        geo_name as region_name,
        SUM(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE 0 END)::FLOAT as swiss_tourists,
        SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END)::FLOAT as foreign_tourists,
        SUM(txn_cnt)::FLOAT as total_visitors,
        CASE 
            WHEN SUM(txn_cnt) > 0 
            THEN (SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END)::FLOAT / SUM(txn_cnt)::FLOAT * 100)
            ELSE 0 
        END as foreign_tourist_percentage
    FROM 
        data_lake.master_card
    GROUP BY 
        geo_name
),
spending_metrics AS (
    -- Calculate spending metrics
    SELECT 
        geo_name as region_name,
        SUM(txn_amt)::FLOAT as total_spend,
        AVG(txn_amt)::FLOAT as avg_transaction_value,
        SUM(txn_amt)::FLOAT / NULLIF(COUNT(DISTINCT txn_date), 0) as daily_spend
    FROM 
        data_lake.master_card
    GROUP BY 
        geo_name
),
industry_metrics AS (
    -- Calculate industry metrics
    SELECT 
        geo_name as region_name,
        COUNT(DISTINCT industry) as industry_count,
        array_agg(DISTINCT industry) as industries,
        array_agg(industry ORDER BY COUNT(*) DESC) FILTER (WHERE industry IS NOT NULL) as top_industries
    FROM 
        data_lake.master_card
    WHERE 
        industry IS NOT NULL
    GROUP BY 
        geo_name
)
SELECT 
    rb.region_name,
    rb.area_ha,
    rb.longitude,
    rb.latitude,
    tm.swiss_tourists,
    tm.foreign_tourists,
    tm.total_visitors,
    tm.foreign_tourist_percentage,
    sm.total_spend,
    sm.avg_transaction_value,
    sm.daily_spend,
    CASE 
        WHEN rb.area_ha > 0 THEN sm.total_spend / rb.area_ha 
        ELSE NULL 
    END as spend_per_hectare,
    CASE 
        WHEN tm.total_visitors > 0 THEN sm.total_spend / tm.total_visitors 
        ELSE NULL 
    END as spend_per_visitor,
    im.industry_count,
    im.industries,
    im.top_industries[1] as top_industry
FROM 
    region_boundaries rb
LEFT JOIN 
    tourism_metrics tm ON rb.region_name = tm.region_name
LEFT JOIN 
    spending_metrics sm ON rb.region_name = sm.region_name
LEFT JOIN 
    industry_metrics im ON rb.region_name = im.region_name;
"""

# Add index creation for the new view
CREATE_CHOROPLETH_ANALYTICS_INDICES = """
CREATE INDEX IF NOT EXISTS idx_choropleth_region_name ON geo_insights.choropleth_analytics(region_name);
CREATE INDEX IF NOT EXISTS idx_choropleth_top_industry ON geo_insights.choropleth_analytics(top_industry);
"""

# Modified SQL_EXECUTION_ORDER to remove the drop views step
SQL_EXECUTION_ORDER = [
    ("Enable PostGIS", ENABLE_POSTGIS_SQL),
    ("Create Schema", CREATE_SCHEMA_SQL),
    ("Create Combined Insights", CREATE_COMBINED_INSIGHTS_SQL),
    ("Index Combined Insights", INDEX_COMBINED_INSIGHTS_SQL),
    ("Create Temporal Insights", CREATE_TEMPORAL_INSIGHTS_SQL),
    ("Index Temporal Insights", INDEX_TEMPORAL_INSIGHTS_SQL),
    ("Create Industry Insights", CREATE_INDUSTRY_INSIGHTS_SQL),
    ("Index Industry Insights", INDEX_INDUSTRY_INSIGHTS_SQL),
    ("Create Region Summary", CREATE_REGION_SUMMARY_SQL),
    ("Index Region Summary", INDEX_REGION_SUMMARY_SQL),
    ("Create Region Hotspots", CREATE_REGION_HOTSPOTS_SQL),
    ("Index Region Hotspots", INDEX_REGION_HOTSPOTS_SQL),
    ("Create Spatial Patterns", CREATE_SPATIAL_PATTERNS_SQL),
    ("Index Spatial Patterns", INDEX_SPATIAL_PATTERNS_SQL),
    ("Create Refresh Function", CREATE_REFRESH_FUNCTION_SQL),
    ("Create Choropleth Analytics", CREATE_CHOROPLETH_ANALYTICS_SQL),
    ("Create Choropleth Analytics Indices", CREATE_CHOROPLETH_ANALYTICS_INDICES)
]

def get_db_connection(retry_count=3, retry_delay=5):
    """
    Get a connection to the database with retry mechanism and timeout settings.
    
    Args:
        retry_count: Number of times to retry connecting
        retry_delay: Seconds to wait between retries
        
    Returns:
        Database connection object or None if connection failed
    """
    connection = None
    attempt = 0
    
    while attempt < retry_count:
        try:
            attempt += 1
            logger.info(f"Connection attempt {attempt}/{retry_count} to database {DB_NAME} at {DB_HOST}:{DB_PORT} as user {DB_USER}...")
            
            # Additional connection parameters for better timeout handling
            connection = psycopg2.connect(
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                connect_timeout=CONNECT_TIMEOUT,
                options=f"-c statement_timeout={STATEMENT_TIMEOUT * 1000}"  # Convert to ms
            )
            
            # Set connection to be resilient to network issues
            if TCP_KEEPALIVES:
                connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                with connection.cursor() as cursor:
                    # Enable TCP keepalives
                    cursor.execute("SET tcp_keepalives_idle = 60;")
                    cursor.execute("SET tcp_keepalives_interval = 30;")
                    cursor.execute("SET tcp_keepalives_count = 5;")
                connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            
            connection.autocommit = False  # Ensure we control transactions
            logger.info("Successfully connected to database.")
            return connection
            
        except psycopg2.OperationalError as e:
            logger.error(f"Database connection failed (attempt {attempt}/{retry_count}): {e}")
            if attempt >= retry_count:
                logger.error("Maximum connection attempts reached. Giving up.")
                logger.error("Please check database host, port, credentials, and ensure the database server is running and accessible.")
                return None
            else:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
        except Exception as e:
            logger.error(f"An unexpected error occurred during database connection: {e}")
            logger.error(traceback.format_exc())
            return None
    
    return None

def execute_sql_command(connection, description, sql_command, continue_on_error=False):
    """
    Executes a single SQL command block with better multi-statement handling,
    timeout management, and automatic reconnection.
    
    Args:
        connection: The database connection
        description: Description of the SQL command being executed
        sql_command: The SQL command to execute
        continue_on_error: Whether to continue execution if an error occurs
        
    Returns:
        (success, result) tuple
    """
    # Check if connection is still valid
    if connection is None or connection.closed:
        logger.error(f"Connection is closed. Cannot execute '{description}'.")
        if continue_on_error:
            return False, None
        raise psycopg2.InterfaceError("Connection is closed")
    
    try:
        with connection.cursor() as cursor:
            logger.info(f"Executing: {description}...")
            start_time = time.time()
            
            # Check if the view already exists for CREATE MATERIALIZED VIEW commands
            if "CREATE MATERIALIZED VIEW" in sql_command:
                view_name = sql_command.split("CREATE MATERIALIZED VIEW")[1].split("AS")[0].strip()
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_matviews 
                        WHERE schemaname = 'geo_insights' 
                        AND matviewname = '{view_name}'
                    );
                """)
                view_exists = cursor.fetchone()[0]
                if view_exists:
                    logger.info(f"Materialized view {view_name} already exists. Skipping creation.")
                    return True, None
            
            # Use execute_batch for multi-statement SQL commands that might contain errors
            if ";" in sql_command and not sql_command.strip().upper().startswith("SELECT"):
                # Split by semicolon but ignore semicolons inside quotes
                statements = []
                current = ""
                in_quotes = False
                quote_char = None
                
                for char in sql_command:
                    if char in ["'", '"'] and (not in_quotes or quote_char == char):
                        in_quotes = not in_quotes
                        if in_quotes:
                            quote_char = char
                        else:
                            quote_char = None
                    
                    if char == ";" and not in_quotes:
                        if current.strip():
                            statements.append(current.strip())
                            current = ""
                    else:
                        current += char
                
                if current.strip():
                    statements.append(current.strip())
                
                # Execute each statement separately
                for i, stmt in enumerate(statements):
                    if stmt.strip():
                        try:
                            cursor.execute(stmt)
                            logger.info(f"  - Sub-statement {i+1}/{len(statements)} executed successfully: {stmt[:50]}...")
                            # Commit after each statement to avoid long transactions
                            connection.commit()
                        except Exception as stmt_error:
                            logger.error(f"  - Error in sub-statement {i+1}/{len(statements)}: {stmt_error}")
                            logger.error(f"  - Failed statement: {stmt[:200]}...")
                            connection.rollback()
                            if not continue_on_error:
                                raise
            else:
                cursor.execute(sql_command)
                connection.commit()  # Commit after execution
            
            duration = time.time() - start_time
            logger.info(f"Successfully executed '{description}' in {duration:.2f} seconds.")
            
            # If this is a SELECT query, return the results
            if sql_command.strip().upper().startswith("SELECT"):
                result = cursor.fetchall()
                if result:
                    column_names = [desc[0] for desc in cursor.description]
                    logger.info(f"Query result: {dict(zip(column_names, result[0])) if result else 'No results'}")
                return True, result
            return True, None
            
    except psycopg2.OperationalError as e:
        if "server closed the connection" in str(e) or "timeout" in str(e):
            logger.error(f"Database timeout or connection lost during '{description}': {e}")
        else:
            logger.error(f"Database operational error executing '{description}': {e}")
            
        if hasattr(e, 'pgcode') and hasattr(e, 'pgerror'):
            logger.error(f"SQLSTATE: {e.pgcode}, Message: {e.pgerror}")
            
        logger.error(f"Failed SQL:\n------\n{sql_command[:500]}...\n------")
        
        try:
            connection.rollback()
        except:
            pass  # Connection might already be closed
            
        if continue_on_error:
            logger.warning(f"Continuing despite error in '{description}'")
            return False, None
        return False, None
        
    except psycopg2.Error as e:
        logger.error(f"Database error executing '{description}': {e}")
        if hasattr(e, 'pgcode') and hasattr(e, 'pgerror'):
            logger.error(f"SQLSTATE: {e.pgcode}, Message: {e.pgerror}")
        logger.error(f"Failed SQL:\n------\n{sql_command[:500]}...\n------")
        
        try:
            connection.rollback()
        except:
            pass
            
        if continue_on_error:
            logger.warning(f"Continuing despite error in '{description}'")
            return False, None
        return False, None
        
    except Exception as e:
        logger.error(f"Unexpected error executing '{description}': {e}")
        logger.error(traceback.format_exc())
        
        try:
            connection.rollback()
        except:
            pass
            
        if continue_on_error:
            logger.warning(f"Continuing despite error in '{description}'")
            return False, None
        return False, None

def get_existing_views(connection):
    """
    Get a list of existing materialized views in the geo_insights schema.
    
    Args:
        connection: Database connection
        
    Returns:
        List of view names
    """
    try:
        success, result = execute_sql_command(
            connection, 
            "Get Existing Views", 
            "SELECT matviewname FROM pg_matviews WHERE schemaname = 'geo_insights';", 
            continue_on_error=True
        )
        if success and result:
            return [row[0] for row in result]
        return []
    except:
        return []

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Create materialized views for geo_insights schema.')
    parser.add_argument('--start-step', type=int, default=1,
                        help='Step number to start from (1-indexed)')
    parser.add_argument('--only-step', type=int,
                        help='Only execute this specific step (1-indexed)')
    parser.add_argument('--skip-exists', action='store_true',
                        help='Skip views that already exist')
    parser.add_argument('--timeout', type=int, default=1800,
                        help='SQL statement timeout in seconds (default: 1800)')
    return parser.parse_args()

def main():
    """Main function to create schema and views"""
    args = parse_arguments()
    global STATEMENT_TIMEOUT
    STATEMENT_TIMEOUT = args.timeout
    
    connection = None
    success_count = 0
    total_steps = len(SQL_EXECUTION_ORDER)
    
    try:
        # Get a database connection with retry
        connection = get_db_connection()
        if connection is None:
            logger.error("Aborting script due to connection failure.")
            sys.exit(1)
            
        # Test PostGIS functionality
        logger.info("Testing PostGIS functionality...")
        success, result = execute_sql_command(connection, "PostGIS Version Test", "SELECT PostGIS_version();", continue_on_error=True)
        if not success:
            logger.warning("PostGIS may not be properly installed or enabled. Will attempt to create extension.")
        
        # Get existing views if we're skipping
        existing_views = []
        if args.skip_exists:
            existing_views = get_existing_views(connection)
            logger.info(f"Found existing views: {existing_views}")
        
        # Check bounding box data
        logger.info("Checking bounding box data validity...")
        success, result = execute_sql_command(connection, "Bounding Box Data Check", CHECK_BOUNDING_BOX_SQL, continue_on_error=True)
        if success and result:
            # Process results - simpler check for bounding box format
            row = result[0]
            column_names = ['total_rows', 'rows_with_bounding_box', 'non_empty_bounding_box', 'valid_format_bounding_box']
            data_stats = dict(zip(column_names, row))
            
            logger.info(f"Bounding box data summary: {data_stats}")
            if data_stats.get('valid_format_bounding_box', 0) == 0:
                logger.warning("No valid bounding box geometries found. Views using geometry will be simplified.")

        # Determine which steps to execute
        steps_to_execute = []
        if args.only_step:
            if 1 <= args.only_step <= total_steps:
                steps_to_execute = [args.only_step - 1]  # Convert to 0-indexed
            else:
                logger.error(f"Invalid step number: {args.only_step}. Must be between 1 and {total_steps}.")
                sys.exit(1)
        else:
            start_idx = max(0, args.start_step - 1)  # Convert to 0-indexed
            steps_to_execute = range(start_idx, total_steps)

        # Execute the selected SQL commands
        for step_idx in steps_to_execute:
            description, sql_command = SQL_EXECUTION_ORDER[step_idx]
            step_num = step_idx + 1  # Convert back to 1-indexed for display
            
            # Check if we should skip this step
            if args.skip_exists and any(view_name in sql_command for view_name in existing_views) and "DROP" not in description.upper():
                logger.info(f"Skipping step {step_num}/{total_steps}: {description} (view already exists)")
                success_count += 1
                continue
                
            # Execute the step
            logger.info(f"Step {step_num}/{total_steps}: {description}")
            
            # Get a fresh connection for each major step to avoid timeout issues
            if "CREATE MATERIALIZED VIEW" in sql_command and connection.closed:
                logger.info("Connection closed. Establishing a new connection...")
                connection = get_db_connection()
                if connection is None:
                    logger.error(f"Could not reconnect to database. Skipping step {step_num}.")
                    continue
            
            success, _ = execute_sql_command(connection, description, sql_command, continue_on_error=True)
            if success:
                success_count += 1
                logger.info(f"Step {step_num}/{total_steps} completed successfully.")
            else:
                logger.warning(f"Step {step_num}/{total_steps} failed, continuing with next step.")
                
        # Summary of execution
        num_steps = len(steps_to_execute)
        logger.info(f"Script completed. {success_count}/{num_steps} selected steps executed successfully.")
        if success_count < num_steps:
            logger.warning("Some steps failed. Check the log for details.")
        else:
            logger.info("All selected steps completed successfully.")

        # List created views for validation
        if not connection.closed:
            success, result = execute_sql_command(
                connection, 
                "List Created Views", 
                "SELECT schemaname, matviewname FROM pg_matviews WHERE schemaname = 'geo_insights';", 
                continue_on_error=True
            )
            if success and result:
                logger.info(f"Created materialized views: {[row[1] for row in result]}")

    except Exception as e:
        logger.error(f"Script failed with an unexpected error: {e}")
        logger.error(traceback.format_exc())
    finally:
        if connection and not connection.closed:
            connection.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()