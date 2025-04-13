-- Create a view to monitor ETL execution history
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

-- Summary of all ETL operations
INSERT INTO edw.etl_metadata (
    table_name, 
    load_start_time, 
    load_end_time, 
    rows_processed,
    load_status, 
    additional_info
)
VALUES (
    'edw_optimization_complete',
    clock_timestamp() - interval '1 hour',
    clock_timestamp(),
    (
        SELECT COALESCE(SUM(row_count), 0) FROM edw.data_cardinality
    ),
    'COMPLETE',
    jsonb_build_object(
        'summary', 'Optimized EDW schema created successfully',
        'dimension_tables', (
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'edw' AND table_name LIKE 'dim_%'
        ),
        'fact_tables', (
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'edw' AND table_name LIKE 'fact_%'
        ),
        'views', (
            SELECT COUNT(*) FROM information_schema.views 
            WHERE table_schema = 'edw'
        )
    )
);

SELECT edw.report_status('EDW optimization completed successfully');