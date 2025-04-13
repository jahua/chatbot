-- Task 1: Initial Setup and Status Reporting Function
-- Create the EDW schema
CREATE SCHEMA IF NOT EXISTS edw;

-- Create ETL metadata table if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.etl_metadata (
    etl_run_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    load_start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_time TIMESTAMP,
    rows_processed INTEGER,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    load_status VARCHAR(20) CHECK (load_status IN ('IN PROGRESS', 'COMPLETE', 'FAILED')),
    error_message TEXT,
    additional_info JSONB
);

-- Create status reporting function
CREATE OR REPLACE FUNCTION edw.report_status(message TEXT)
RETURNS void AS $$
BEGIN
    INSERT INTO edw.etl_metadata (
        table_name,
        load_start_time,
        load_end_time,
        load_status,
        additional_info
    ) VALUES (
        'status_message',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'COMPLETE',
        jsonb_build_object('message', message)
    );
    
    -- Also output to console for immediate feedback
    RAISE NOTICE '%', message;
END;
$$ LANGUAGE plpgsql;

-- Report initial setup complete
SELECT edw.report_status('Task 1: Initial setup completed successfully'); 