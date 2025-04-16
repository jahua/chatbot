-- Script to extract and save unique geo_names from geo_insights.mastercard
-- This script creates a table to store unique geo_names and their counts

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS dw;

-- Create table to store unique geo_names if it doesn't exist
CREATE TABLE IF NOT EXISTS dw.unique_geo_names (
    id SERIAL PRIMARY KEY,
    geo_name TEXT NOT NULL,
    source_table TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(geo_name, source_table)
);

-- Create table to track processing status
CREATE TABLE IF NOT EXISTS dw.processing_status (
    id SERIAL PRIMARY KEY,
    process_name TEXT NOT NULL,
    total_records INTEGER NOT NULL,
    processed_records INTEGER DEFAULT 0,
    current_batch INTEGER DEFAULT 0,
    total_batches INTEGER NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT DEFAULT 'running',
    error_message TEXT
);

-- Function to update unique geo_names with batch processing
CREATE OR REPLACE FUNCTION dw.update_unique_geo_names_batch()
RETURNS TABLE (
    batch_number INTEGER,
    records_processed INTEGER,
    total_records INTEGER,
    percentage_complete DECIMAL(5,2)
) AS $$
DECLARE
    v_total_records INTEGER;
    v_batch_size INTEGER := 10000;
    v_current_batch INTEGER := 0;
    v_processed_records INTEGER := 0;
    v_batch_records INTEGER;
    v_start_time TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    -- Get total number of records
    SELECT COUNT(*) INTO v_total_records FROM geo_insights.mastercard WHERE geo_name IS NOT NULL;
    
    -- Calculate total number of batches
    DECLARE
        v_total_batches INTEGER := CEIL(v_total_records::float / v_batch_size);
    BEGIN
        -- Insert initial status
        INSERT INTO dw.processing_status (process_name, total_records, total_batches)
        VALUES ('geo_name_extraction', v_total_records, v_total_batches);
    END;

    -- Process in batches
    WHILE v_processed_records < v_total_records LOOP
        v_current_batch := v_current_batch + 1;
        
        -- Process current batch
        WITH batch_data AS (
            SELECT 
                geo_name,
                COUNT(*) as record_count
            FROM geo_insights.mastercard
            WHERE geo_name IS NOT NULL
            GROUP BY geo_name
            LIMIT v_batch_size
            OFFSET v_processed_records
        )
        INSERT INTO dw.unique_geo_names (geo_name, source_table, record_count, first_seen_date, last_seen_date)
        SELECT 
            geo_name,
            'geo_insights.mastercard',
            record_count,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM batch_data
        ON CONFLICT (geo_name, source_table) 
        DO UPDATE SET 
            record_count = EXCLUDED.record_count,
            last_seen_date = CURRENT_TIMESTAMP;

        -- Get records processed in this batch
        GET DIAGNOSTICS v_batch_records = ROW_COUNT;
        v_processed_records := v_processed_records + v_batch_records;

        -- Update status
        UPDATE dw.processing_status
        SET 
            processed_records = v_processed_records,
            current_batch = v_current_batch,
            status = CASE 
                WHEN v_processed_records >= v_total_records THEN 'completed'
                ELSE 'running'
            END,
            end_time = CASE 
                WHEN v_processed_records >= v_total_records THEN CURRENT_TIMESTAMP
                ELSE NULL
            END
        WHERE process_name = 'geo_name_extraction';

        -- Return progress information
        RETURN QUERY
        SELECT 
            v_current_batch,
            v_processed_records,
            v_total_records,
            ROUND((v_processed_records::float / v_total_records) * 100, 2);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to view current status
CREATE OR REPLACE FUNCTION dw.get_processing_status()
RETURNS TABLE (
    process_name TEXT,
    total_records INTEGER,
    processed_records INTEGER,
    current_batch INTEGER,
    total_batches INTEGER,
    percentage_complete DECIMAL(5,2),
    status TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    processing_time INTERVAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ps.process_name,
        ps.total_records,
        ps.processed_records,
        ps.current_batch,
        ps.total_batches,
        ROUND((ps.processed_records::float / ps.total_records) * 100, 2) as percentage_complete,
        ps.status,
        ps.start_time,
        ps.end_time,
        COALESCE(ps.end_time, CURRENT_TIMESTAMP) - ps.start_time as processing_time
    FROM dw.processing_status ps
    WHERE ps.process_name = 'geo_name_extraction'
    ORDER BY ps.id DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Execute the batch processing
SELECT * FROM dw.update_unique_geo_names_batch();

-- View current status
SELECT * FROM dw.get_processing_status();

-- View the results
SELECT 
    geo_name,
    record_count,
    first_seen_date,
    last_seen_date
FROM dw.unique_geo_names
WHERE source_table = 'geo_insights.mastercard'
ORDER BY geo_name;

-- Copy unique geo_names from mastercard table to a CSV file
COPY (
    SELECT DISTINCT geo_name
    FROM geo_insights.mastercard
    WHERE geo_name IS NOT NULL
    ORDER BY geo_name
) TO '/tmp/unique_geo_names.csv' WITH CSV HEADER; 