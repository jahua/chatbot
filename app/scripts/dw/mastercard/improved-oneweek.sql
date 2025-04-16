-- Improved MasterCard One Week Injection Script
-- Based on successful simple script approach but for one week of data

-- First verify we have the necessary tables
DO $$
BEGIN
    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE 'Starting MasterCard One Week Data Injection';
    RAISE NOTICE 'Timestamp: %', CURRENT_TIMESTAMP;
    RAISE NOTICE '--------------------------------------------------';
END $$;

-- Main data insertion block
DO $$
DECLARE
    v_batch_id INTEGER;
    v_now TIMESTAMP := CURRENT_TIMESTAMP;
    v_count INTEGER := 0;
    v_start_date DATE := '2022-02-01';
    v_end_date DATE := '2022-02-07';
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_source_count INTEGER := 0;
BEGIN
    -- Set start time for tracking
    v_start_time := clock_timestamp();
    
    -- Get next batch ID
    v_batch_id := nextval('dw.etl_metadata_etl_id_seq');
    RAISE NOTICE 'Using batch ID: %', v_batch_id;
    
    -- Check source data count first
    RAISE NOTICE 'Checking source data...';
    SELECT COUNT(*) INTO v_source_count
    FROM data_lake.master_card
    WHERE txn_date BETWEEN v_start_date AND v_end_date;
    
    RAISE NOTICE 'Found % records in source data for date range % to %', 
                v_source_count, v_start_date, v_end_date;
    
    -- Insert MasterCard data for the one week period
    RAISE NOTICE 'Starting data insertion...';
    
    INSERT INTO dw.fact_spending (
        date_id,
        industry_id,
        region_id,
        transaction_count,
        total_amount,
        avg_transaction,
        source_system,
        batch_id,
        created_at
    )
    SELECT 
        TO_CHAR(mc.txn_date, 'YYYYMMDD')::INTEGER AS date_id,
        di.industry_id,
        dr.region_id,
        SUM(mc.txn_cnt) AS transaction_count,
        SUM(mc.txn_amt) AS total_amount,
        CASE WHEN SUM(mc.txn_cnt) > 0 THEN SUM(mc.txn_amt) / SUM(mc.txn_cnt) ELSE 0 END AS avg_transaction,
        'mastercard' AS source_system,
        v_batch_id AS batch_id,
        v_now AS created_at
    FROM data_lake.master_card mc
    -- Join to industry dimension using simple matching to avoid the encoding issues
    JOIN dw.dim_industry di ON di.industry_name = mc.industry AND di.industry_code IS NOT NULL
    -- Join directly to region dimension using region_type matching
    JOIN dw.dim_region dr ON (
        -- Match by direct region name when possible, or handle special cases
        (dr.region_name = mc.geo_name OR
         (mc.geo_name = 'Zurich' AND dr.region_name IN ('Zürich', 'Zurich')) OR
         (mc.geo_name = 'Geneva' AND dr.region_name IN ('Genève', 'Geneva')))
        AND
        -- Match region type based on geo_type
        ((mc.geo_type = 'State' AND dr.region_type = 'canton') OR
         (mc.geo_type = 'Msa' AND dr.region_type = 'tourism_region') OR
         (mc.geo_type = 'Country' AND dr.region_type = 'district'))
    )
    WHERE mc.txn_date BETWEEN v_start_date AND v_end_date
    GROUP BY 
        TO_CHAR(mc.txn_date, 'YYYYMMDD')::INTEGER,
        di.industry_id,
        dr.region_id;
    
    -- Get the count of rows affected
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- Record this in the ETL metadata
    INSERT INTO dw.etl_metadata (
        etl_id,
        process_name,
        source_system,
        status_code,
        start_time,
        end_time,
        records_processed,
        records_successful,
        status_message
    ) VALUES (
        v_batch_id,
        'improved_mastercard_oneweek',
        'mastercard',
        'COMPLETED',
        v_start_time,
        CURRENT_TIMESTAMP,
        v_count,
        v_count,
        'Successfully injected MasterCard one week data. Records: ' || v_count
    );
    
    -- End timing
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    
    -- Final summary
    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE 'MasterCard ONE WEEK completed successfully!';
    RAISE NOTICE '  Start time: %', v_start_time;
    RAISE NOTICE '  End time:   %', v_end_time;
    RAISE NOTICE '  Duration:   % seconds', EXTRACT(EPOCH FROM v_duration);
    RAISE NOTICE '  Records:    %', v_count;
    RAISE NOTICE '  Batch ID:   %', v_batch_id;
    RAISE NOTICE '--------------------------------------------------';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR: %', SQLERRM;
    
    -- Record error in ETL metadata
    INSERT INTO dw.etl_metadata (
        etl_id,
        process_name,
        source_system,
        status_code,
        start_time,
        end_time,
        records_processed,
        records_successful,
        status_message
    ) VALUES (
        v_batch_id,
        'improved_mastercard_oneweek',
        'mastercard',
        'FAILED',
        v_start_time,
        CURRENT_TIMESTAMP,
        0,
        0,
        'Error injecting MasterCard one week data: ' || SQLERRM
    );
END $$;

-- Check the results
SELECT 
    'Count by date' AS check_type,
    COUNT(*) AS total_records
FROM dw.fact_spending
WHERE source_system = 'mastercard';

SELECT 
    date_id,
    COUNT(*) as record_count
FROM dw.fact_spending
WHERE source_system = 'mastercard'
GROUP BY date_id
ORDER BY date_id;

-- Print completion message
DO $$
BEGIN
    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE 'MasterCard ONE WEEK test load complete!';
    RAISE NOTICE '--------------------------------------------------';
END $$; 