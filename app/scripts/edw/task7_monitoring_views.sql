-- First verify if the report_status function exists and create if needed
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'report_status' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'edw')) THEN
        EXECUTE 'CREATE OR REPLACE FUNCTION edw.report_status(p_message TEXT) RETURNS VOID AS $$
        BEGIN
            RAISE NOTICE ''%'', p_message;
            
            -- Also log to a table for persistent tracking
            INSERT INTO edw.etl_metadata (table_name, load_start_time, load_end_time, load_status, additional_info)
            VALUES (''status_message'', clock_timestamp(), clock_timestamp(), ''COMPLETE'', 
                   jsonb_build_object(''message'', p_message));
        END;
        $$ LANGUAGE plpgsql;';
        
        RAISE NOTICE 'Created status reporting function';
    END IF;
END;
$$;

-- Begin monitoring views creation
SELECT edw.report_status('Starting creation of EDW monitoring views');

-- Data Cardinality View
DROP VIEW IF EXISTS edw.data_cardinality;
CREATE OR REPLACE VIEW edw.data_cardinality AS
SELECT
    'edw.dim_time' AS table_name,
    (SELECT COUNT(*) FROM edw.dim_time) AS row_count,
    'Dimension' AS table_type,
    'date_id' AS primary_key,
    NULL AS foreign_key_relationships
UNION ALL
SELECT
    'edw.dim_region' AS table_name,
    (SELECT COUNT(*) FROM edw.dim_region) AS row_count,
    'Dimension' AS table_type,
    'region_id' AS primary_key,
    'parent_region_id references edw.dim_region(region_id)' AS foreign_key_relationships
UNION ALL
SELECT
    'edw.dim_object_type' AS table_name,
    (SELECT COUNT(*) FROM edw.dim_object_type) AS row_count,
    'Dimension' AS table_type,
    'object_type_id' AS primary_key,
    NULL AS foreign_key_relationships
UNION ALL
SELECT
    'edw.dim_visit_type' AS table_name,
    (SELECT COUNT(*) FROM edw.dim_visit_type) AS row_count,
    'Dimension' AS table_type,
    'visit_type_id' AS primary_key,
    NULL AS foreign_key_relationships
UNION ALL
SELECT
    'edw.dim_data_type' AS table_name,
    (SELECT COUNT(*) FROM edw.dim_data_type) AS row_count,
    'Dimension' AS table_type,
    'data_type_id' AS primary_key,
    NULL AS foreign_key_relationships
UNION ALL
SELECT
    'edw.dim_industry' AS table_name,
    (SELECT COUNT(*) FROM edw.dim_industry) AS row_count,
    'Dimension' AS table_type,
    'industry_id' AS primary_key,
    NULL AS foreign_key_relationships
UNION ALL
SELECT
    'edw.fact_tourism_visitors' AS table_name,
    (SELECT COUNT(*) FROM edw.fact_tourism_visitors) AS row_count,
    'Fact' AS table_type,
    'fact_id' AS primary_key,
    'date_id, region_id, object_type_id, visit_type_id, data_type_id' AS foreign_key_relationships
UNION ALL
SELECT
    'edw.fact_tourism_spending' AS table_name,
    (SELECT COUNT(*) FROM edw.fact_tourism_spending) AS row_count,
    'Fact' AS table_type,
    'fact_id' AS primary_key,
    'date_id, region_id, object_type_id, visit_type_id, data_type_id' AS foreign_key_relationships
UNION ALL
SELECT
    'edw.fact_tourism_unified' AS table_name,
    (SELECT COUNT(*) FROM edw.fact_tourism_unified) AS row_count,
    'Fact' AS table_type,
    'fact_id' AS primary_key,
    'date_id, region_id, object_type_id, visit_type_id, data_type_id' AS foreign_key_relationships;

SELECT edw.report_status('data_cardinality view created');

-- ETL History View
DROP VIEW IF EXISTS edw.etl_history;
CREATE OR REPLACE VIEW edw.etl_history AS
SELECT 
    etl_run_id,
    table_name,
    load_start_time,
    load_end_time,
    EXTRACT(EPOCH FROM (load_end_time - load_start_time))::numeric(10,2) AS duration_seconds,
    rows_processed,
    rows_inserted,
    rows_updated,
    load_status,
    error_message,
    -- Extract the message from status messages
    CASE WHEN table_name = 'status_message' THEN additional_info->>'message' ELSE NULL END AS status_message
FROM edw.etl_metadata
ORDER BY load_start_time DESC;

SELECT edw.report_status('etl_history view created');

-- Data Completeness View
DROP VIEW IF EXISTS edw.data_completeness;
CREATE OR REPLACE VIEW edw.data_completeness AS
SELECT
    dt.year,
    dt.month_number,
    dt.month,
    r.region_type,
    COUNT(*) AS total_records,
    SUM(CASE WHEN COALESCE(f.visitor_data_completion_pct, 0) > 0 THEN 1 ELSE 0 END) AS records_with_visitor_data,
    SUM(CASE WHEN COALESCE(f.spending_data_completion_pct, 0) > 0 THEN 1 ELSE 0 END) AS records_with_spending_data,
    SUM(CASE WHEN f.data_completeness = 'complete' THEN 1 ELSE 0 END) AS complete_records,
    SUM(CASE WHEN f.data_completeness = 'partial' THEN 1 ELSE 0 END) AS partial_records,
    SUM(CASE WHEN f.data_completeness = 'minimal' THEN 1 ELSE 0 END) AS minimal_records,
    ROUND(AVG(COALESCE(f.data_quality_score, 0)), 1) AS avg_quality_score,
    ROUND(AVG(COALESCE(f.visitor_data_completion_pct, 0)), 1) AS avg_visitor_completion_pct,
    ROUND(AVG(COALESCE(f.spending_data_completion_pct, 0)), 1) AS avg_spending_completion_pct,
    ROUND(
        100.0 * SUM(CASE WHEN f.data_completeness = 'complete' THEN 1 ELSE 0 END)::numeric / 
        NULLIF(COUNT(*), 0),
        1
    ) AS pct_complete_records,
    ROUND(
        100.0 * SUM(CASE WHEN f.data_completeness IN ('complete', 'partial') THEN 1 ELSE 0 END)::numeric / 
        NULLIF(COUNT(*), 0),
        1
    ) AS pct_usable_records
FROM edw.fact_tourism_unified f
JOIN edw.dim_time dt ON f.date_id = dt.date_id
JOIN edw.dim_region r ON f.region_id = r.region_id
GROUP BY 
    dt.year, 
    dt.month_number,
    dt.month,
    r.region_type
ORDER BY 
    dt.year, 
    dt.month_number,
    r.region_type;

SELECT edw.report_status('data_completeness view created');

-- Data Quality Summary View
DROP VIEW IF EXISTS edw.data_quality_summary;
CREATE OR REPLACE VIEW edw.data_quality_summary AS
SELECT
    'Overall' AS scope,
    (SELECT COUNT(*) FROM edw.fact_tourism_unified) AS total_records,
    (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'complete') AS complete_records,
    (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'partial') AS partial_records,
    (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'minimal') AS minimal_records,
    ROUND(
        (SELECT 100.0 * COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'complete') / 
        NULLIF((SELECT COUNT(*) FROM edw.fact_tourism_unified), 0),
        1
    ) AS pct_complete,
    ROUND(
        (SELECT AVG(COALESCE(data_quality_score, 0)) FROM edw.fact_tourism_unified),
        1
    ) AS avg_quality_score,
    ROUND(
        (SELECT AVG(COALESCE(visitor_data_completion_pct, 0)) FROM edw.fact_tourism_unified),
        1
    ) AS avg_visitor_completion,
    ROUND(
        (SELECT AVG(COALESCE(spending_data_completion_pct, 0)) FROM edw.fact_tourism_unified),
        1
    ) AS avg_spending_completion,
    CURRENT_TIMESTAMP AS generated_at
UNION ALL
SELECT
    'Last 3 Months' AS scope,
    COUNT(*) AS total_records,
    SUM(CASE WHEN data_completeness = 'complete' THEN 1 ELSE 0 END) AS complete_records,
    SUM(CASE WHEN data_completeness = 'partial' THEN 1 ELSE 0 END) AS partial_records,
    SUM(CASE WHEN data_completeness = 'minimal' THEN 1 ELSE 0 END) AS minimal_records,
    ROUND(
        100.0 * SUM(CASE WHEN data_completeness = 'complete' THEN 1 ELSE 0 END) / 
        NULLIF(COUNT(*), 0),
        1
    ) AS pct_complete,
    ROUND(AVG(COALESCE(data_quality_score, 0)), 1) AS avg_quality_score,
    ROUND(AVG(COALESCE(visitor_data_completion_pct, 0)), 1) AS avg_visitor_completion,
    ROUND(AVG(COALESCE(spending_data_completion_pct, 0)), 1) AS avg_spending_completion,
    CURRENT_TIMESTAMP AS generated_at
FROM edw.fact_tourism_unified f
JOIN edw.dim_time dt ON f.date_id = dt.date_id
WHERE (dt.year * 100 + dt.month_number) >= 
      ((SELECT MAX(year) * 100 + MAX(month_number) FROM edw.dim_time) - 3);

SELECT edw.report_status('data_quality_summary view created');

-- Source Integration Overview - Fixed
DROP VIEW IF EXISTS edw.source_integration;
CREATE OR REPLACE VIEW edw.source_integration AS
WITH source_stats AS (
    SELECT
        'inervista' AS source_system,
        COUNT(*) AS total_records,
        SUM(CASE WHEN metadata->'inervista' IS NOT NULL THEN 1 ELSE 0 END) AS integrated_records,
        100.0 * SUM(CASE WHEN metadata->'inervista' IS NOT NULL THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(*), 0) AS integration_rate
    FROM edw.fact_tourism_unified
    UNION ALL
    SELECT
        'data_lake.aoi_days_raw' AS source_system,
        COUNT(*) AS total_records,
        SUM(CASE WHEN swiss_tourists_raw IS NOT NULL THEN 1 ELSE 0 END) AS integrated_records,
        100.0 * SUM(CASE WHEN swiss_tourists_raw IS NOT NULL THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(*), 0) AS integration_rate
    FROM edw.fact_tourism_unified
    UNION ALL
    SELECT
        'data_lake.master_card' AS source_system,
        COUNT(*) AS total_records,
        SUM(CASE WHEN spending_amount IS NOT NULL THEN 1 ELSE 0 END) AS integrated_records,
        100.0 * SUM(CASE WHEN spending_amount IS NOT NULL THEN 1 ELSE 0 END) / 
            NULLIF(COUNT(*), 0) AS integration_rate
    FROM edw.fact_tourism_unified
)
SELECT
    source_system,
    total_records,
    integrated_records,
    ROUND(integration_rate, 1) AS integration_rate,
    CASE 
        WHEN integration_rate > 80 THEN 'Excellent'
        WHEN integration_rate > 60 THEN 'Good'
        WHEN integration_rate > 40 THEN 'Fair'
        WHEN integration_rate > 20 THEN 'Poor'
        ELSE 'Critical'
    END AS integration_quality,
    CURRENT_TIMESTAMP AS generated_at
FROM 
    source_stats
ORDER BY 
    integration_rate DESC;

SELECT edw.report_status('source_integration view created');

-- Add overall ETL summary to metadata
INSERT INTO edw.etl_metadata (
    table_name, 
    load_start_time, 
    load_end_time, 
    rows_processed,
    load_status, 
    additional_info
)
VALUES (
    'monitoring_views_summary',
    clock_timestamp() - interval '5 minutes',
    clock_timestamp(),
    (
        SELECT SUM(row_count) FROM edw.data_cardinality
    ),
    'COMPLETE',
    jsonb_build_object(
        'summary', 'EDW monitoring views created successfully',
        'dimension_tables', (
            SELECT SUM(row_count) FROM edw.data_cardinality WHERE table_type = 'Dimension'
        ),
        'fact_tables', (
            SELECT SUM(row_count) FROM edw.data_cardinality WHERE table_type = 'Fact'
        ),
        'monitoring_views', 5,
        'views_created', ARRAY[
            'data_cardinality', 
            'etl_history', 
            'data_completeness', 
            'data_quality_summary',
            'source_integration'
        ]
    )
);

-- Print final summary
SELECT edw.report_status('All monitoring views created successfully');

-- Print console summary
\echo '--------------------------------------------------------------'
\echo 'EDW Monitoring Views Setup Complete'
\echo '--------------------------------------------------------------'
\echo 'Created views: data_cardinality, etl_history, data_completeness, data_quality_summary, source_integration'
\echo 'To see all warehouse objects: SELECT * FROM edw.data_cardinality;'
\echo 'To see data quality metrics: SELECT * FROM edw.data_quality_summary;'
\echo 'To see ETL history: SELECT * FROM edw.etl_history LIMIT 10;'
\echo '--------------------------------------------------------------'