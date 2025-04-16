-- Enhanced MasterCard Two-Day Injection Script (March 1-2, 2022)
-- Optimized version with improved performance while maintaining all functionality

-- Clear screen and display header
DO $$
BEGIN
    RAISE NOTICE '==================================================';
    RAISE NOTICE '   MasterCard Two-Day Data Injection - Optimized  ';
    RAISE NOTICE '==================================================';
    RAISE NOTICE 'Started at: %', CURRENT_TIMESTAMP;
    RAISE NOTICE '';
END $$;

-- Check for existing data to avoid duplication
DO $$
DECLARE
    v_existing_count INTEGER := 0;
    v_start_date DATE := '2022-03-01';
    v_end_date DATE := '2022-03-02';
BEGIN
    -- Check if we already have data for these dates
    SELECT COUNT(*) INTO v_existing_count
    FROM dw.fact_spending
    WHERE source_system = 'mastercard'
      AND date_id BETWEEN TO_CHAR(v_start_date, 'YYYYMMDD')::INTEGER 
                       AND TO_CHAR(v_end_date, 'YYYYMMDD')::INTEGER;
    
    IF v_existing_count > 0 THEN
        RAISE NOTICE 'WARNING: Found % existing records for the selected date range', v_existing_count;
        RAISE NOTICE 'Consider using a different date range or removing existing data.';
    ELSE
        RAISE NOTICE 'Date range is clear for insertion: % to %', v_start_date, v_end_date;
    END IF;
END $$;

-- Main data insertion block with optimizations
DO $$
DECLARE
    v_batch_id INTEGER;
    v_now TIMESTAMP := CURRENT_TIMESTAMP;
    v_count INTEGER := 0;
    v_industry_count INTEGER := 0;
    v_region_count INTEGER := 0;
    v_unmapped_count INTEGER := 0;
    v_date_count INTEGER := 0;
    v_start_date DATE := '2022-03-01';
    v_end_date DATE := '2022-03-02';
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_source_count INTEGER := 0;
    v_mapped_source_count INTEGER := 0;
    v_missing_industry_list TEXT := '';
    v_progress_count INTEGER := 0;
    v_progress_total INTEGER := 0;
    v_progress_pct INTEGER := 0;
    v_progress_last_pct INTEGER := 0;
BEGIN
    -- Set work_mem higher for this session for better query performance
    SET LOCAL work_mem = '32MB';
    
    -- Set start time for tracking
    v_start_time := clock_timestamp();
    
    -- Get next batch ID
    v_batch_id := nextval('dw.etl_metadata_etl_id_seq');
    RAISE NOTICE 'Process initiated with batch ID: %', v_batch_id;
    
    -- Environment check
    RAISE NOTICE '> Environment check...';
    SELECT COUNT(*) INTO v_industry_count FROM dw.dim_industry WHERE industry_code IS NOT NULL;
    SELECT COUNT(*) INTO v_region_count FROM dw.dim_region;
    
    RAISE NOTICE '  - Available industries: %', v_industry_count;
    RAISE NOTICE '  - Available regions: %', v_region_count;
    
    -- Create temporary table to prefilter source data (Performance optimization #1)
    RAISE NOTICE '> Creating temporary source data table...';
    CREATE TEMP TABLE temp_mastercard_source AS
    SELECT *
    FROM data_lake.master_card
    WHERE txn_date BETWEEN v_start_date AND v_end_date;
    
    -- Create indexes on the temp table to speed up joins (Performance optimization #2)
    RAISE NOTICE '> Creating indexes on temporary tables...';
    CREATE INDEX idx_temp_mc_industry ON temp_mastercard_source(industry);
    CREATE INDEX idx_temp_mc_geo_name ON temp_mastercard_source(geo_name);
    CREATE INDEX idx_temp_mc_geo_type ON temp_mastercard_source(geo_type);
    CREATE INDEX idx_temp_mc_txn_date ON temp_mastercard_source(txn_date);
    
    -- Source data analysis (using temp table for better performance)
    RAISE NOTICE '> Analyzing source data...';
    SELECT COUNT(*) INTO v_source_count
    FROM temp_mastercard_source;
    
    -- Identify unmapped industries for logging (Validation improvement #1)
    WITH missing_industries AS (
        SELECT DISTINCT mc.industry
        FROM temp_mastercard_source mc
        LEFT JOIN dw.dim_industry di ON di.industry_name = mc.industry
        WHERE di.industry_id IS NULL
    )
    SELECT string_agg(industry, ', ') INTO v_missing_industry_list
    FROM missing_industries;

    IF v_missing_industry_list IS NOT NULL AND LENGTH(v_missing_industry_list) > 0 THEN
        RAISE NOTICE '  - WARNING: Found unmapped industries: %', v_missing_industry_list;
    END IF;
    
    -- Check how many records we can map using the optimized approach
    -- Using the same join logic as in improved-twoday-march.sql
    SELECT COUNT(*) INTO v_mapped_source_count
    FROM temp_mastercard_source mc
    JOIN dw.dim_industry di ON di.industry_name = mc.industry AND di.industry_code IS NOT NULL
    JOIN dw.dim_region dr ON (
        (dr.region_name = mc.geo_name OR
         (mc.geo_name = 'Zurich' AND dr.region_name IN ('Zürich', 'Zurich')) OR
         (mc.geo_name = 'Geneva' AND dr.region_name IN ('Genève', 'Geneva')))
        AND
        ((mc.geo_type = 'State' AND dr.region_type = 'canton') OR
         (mc.geo_type = 'Msa' AND dr.region_type = 'tourism_region') OR
         (mc.geo_type = 'Country' AND dr.region_type = 'district'))
    );
    
    RAISE NOTICE '  - Total source records: %', v_source_count;
    RAISE NOTICE '  - Mappable records: % (%.1f%%)', 
        v_mapped_source_count, 
        CASE WHEN v_source_count > 0 
             THEN (v_mapped_source_count::FLOAT / v_source_count) * 100 
             ELSE 0 END;
    
    -- Create a table to track progress (Progress tracking improvement)
    CREATE TEMP TABLE progress_tracker (
        step_name TEXT PRIMARY KEY,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        record_count INTEGER,
        status TEXT
    );
    
    -- Insert MasterCard data using the optimized approach (Performance optimization #6)
    RAISE NOTICE '> Starting data insertion at %...', clock_timestamp();
    INSERT INTO progress_tracker VALUES ('data_insertion', clock_timestamp(), NULL, 0, 'IN_PROGRESS');
    
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
    FROM temp_mastercard_source mc
    -- Join to industry dimension using simple matching
    JOIN dw.dim_industry di ON di.industry_name = mc.industry AND di.industry_code IS NOT NULL
    -- Join to region dimension using the same logic as in improved-twoday-march.sql
    JOIN dw.dim_region dr ON (
        -- Match by region name with special handling for encoding issues
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
    
    -- Update progress tracker
    UPDATE progress_tracker 
    SET end_time = clock_timestamp(), 
        status = 'COMPLETED'
    WHERE step_name = 'data_insertion';
    
    -- Get the count of rows affected
    GET DIAGNOSTICS v_count = ROW_COUNT;
    UPDATE progress_tracker SET record_count = v_count WHERE step_name = 'data_insertion';
    
    -- Get date count
    SELECT COUNT(DISTINCT date_id) INTO v_date_count
    FROM dw.fact_spending
    WHERE source_system = 'mastercard'
      AND batch_id = v_batch_id;
    
    -- Record ETL metadata
    RAISE NOTICE '> Recording ETL metadata...';
    INSERT INTO progress_tracker VALUES ('metadata_recording', clock_timestamp(), NULL, 0, 'IN_PROGRESS');
    
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
        'optimized_mastercard_march',
        'mastercard',
        'COMPLETED',
        v_start_time,
        CURRENT_TIMESTAMP,
        v_count,
        v_count,
        'Successfully injected MasterCard data for ' || v_date_count || 
        ' days between ' || v_start_date || ' and ' || v_end_date ||
        '. Records: ' || v_count
    );
    
    -- Update progress tracker
    UPDATE progress_tracker 
    SET end_time = clock_timestamp(), 
        status = 'COMPLETED',
        record_count = 1
    WHERE step_name = 'metadata_recording';
    
    -- End timing
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    
    -- Advanced performance metrics (Reporting improvement)
    INSERT INTO progress_tracker VALUES ('post_processing', clock_timestamp(), NULL, 0, 'IN_PROGRESS');
    
    -- Run ANALYZE on the fact table for query optimization
    ANALYZE dw.fact_spending;
    
    UPDATE progress_tracker 
    SET end_time = clock_timestamp(), 
        status = 'COMPLETED'
    WHERE step_name = 'post_processing';
    
    -- Final summary with enhanced details
    RAISE NOTICE '';
    RAISE NOTICE '==================================================';
    RAISE NOTICE '            PROCESS COMPLETED SUCCESSFULLY         ';
    RAISE NOTICE '==================================================';
    RAISE NOTICE '  Start time:       %', v_start_time;
    RAISE NOTICE '  End time:         %', v_end_time;
    RAISE NOTICE '  Duration:         % seconds', EXTRACT(EPOCH FROM v_duration);
    RAISE NOTICE '  Source records:   %', v_source_count;
    RAISE NOTICE '  Mapped records:   %', v_mapped_source_count;
    RAISE NOTICE '  Inserted records: %', v_count;
    RAISE NOTICE '  Coverage:         %.1f%%', 
        CASE WHEN v_source_count > 0 
             THEN (v_count::FLOAT / v_source_count) * 100 
             ELSE 0 END;
    RAISE NOTICE '  Batch ID:         %', v_batch_id;
    RAISE NOTICE '  Dates processed:  %', v_date_count;
    RAISE NOTICE '==================================================';
    RAISE NOTICE '  Step timings:';
    
    -- Display step timings without using FOR loop
    RAISE NOTICE '  Step statistics:';
    
    -- Get data from progress_tracker and display directly
    RAISE NOTICE '  - Data insertion: % seconds (% records)', 
        EXTRACT(EPOCH FROM ((SELECT end_time FROM progress_tracker WHERE step_name = 'data_insertion') - 
                           (SELECT start_time FROM progress_tracker WHERE step_name = 'data_insertion'))),
        (SELECT record_count FROM progress_tracker WHERE step_name = 'data_insertion');
        
    RAISE NOTICE '  - Metadata recording: % seconds', 
        EXTRACT(EPOCH FROM ((SELECT end_time FROM progress_tracker WHERE step_name = 'metadata_recording') - 
                           (SELECT start_time FROM progress_tracker WHERE step_name = 'metadata_recording')));
        
    RAISE NOTICE '  - Post-processing: % seconds', 
        EXTRACT(EPOCH FROM ((SELECT end_time FROM progress_tracker WHERE step_name = 'post_processing') - 
                           (SELECT start_time FROM progress_tracker WHERE step_name = 'post_processing')));
    
    RAISE NOTICE '==================================================';
    
    -- Clean up temporary tables (Performance optimization #7)
    DROP TABLE IF EXISTS temp_mastercard_source;
    DROP TABLE IF EXISTS progress_tracker;
    
EXCEPTION WHEN OTHERS THEN
    -- Enhanced error reporting
    RAISE NOTICE '';
    RAISE NOTICE '==================================================';
    RAISE NOTICE '                 ERROR OCCURRED                    ';
    RAISE NOTICE '==================================================';
    RAISE NOTICE 'Error message: %', SQLERRM;
    RAISE NOTICE 'Error context: %', SQLSTATE;
    RAISE NOTICE 'Error detail: %', CASE WHEN SQLERRM LIKE '%unique constraint%' 
                                   THEN 'Unique constraint violation - duplicate data found' 
                                   WHEN SQLERRM LIKE '%null value%not null%' 
                                   THEN 'NULL value violation - required field missing data'
                                   ELSE 'See error message above' END;
    RAISE NOTICE 'Time of error: %', clock_timestamp();
    
    -- Get the current processing step for better error tracking
    DECLARE
        current_step TEXT := 'unknown';
    BEGIN
        SELECT step_name INTO current_step
        FROM progress_tracker
        WHERE status = 'IN_PROGRESS'
        LIMIT 1;
        
        IF current_step IS NOT NULL THEN
            RAISE NOTICE 'Failed during step: %', current_step;
            
            -- Update the progress tracking
            UPDATE progress_tracker 
            SET end_time = clock_timestamp(), 
                status = 'FAILED'
            WHERE step_name = current_step;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            -- If we can't access the progress tracker
            RAISE NOTICE 'Could not determine current processing step.';
    END;
    
    -- Record error in ETL metadata with more details
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
        'optimized_mastercard_march',
        'mastercard',
        'FAILED',
        v_start_time,
        CURRENT_TIMESTAMP,
        0,
        0,
        'Error injecting MasterCard data: ' || SQLERRM || ' [' || SQLSTATE || ']'
    );
    
    -- Clean up temporary tables even on error
    DROP TABLE IF EXISTS temp_mastercard_source;
    DROP TABLE IF EXISTS progress_tracker;
    
    RAISE NOTICE 'Error recorded in ETL metadata with batch ID: %', v_batch_id;
    RAISE NOTICE '==================================================';
END $$;

-- Enhanced results report with data quality metrics
DO $$
DECLARE
    v_total_records INTEGER;
    v_latest_batch INTEGER;
    v_batch_records INTEGER;
    v_zero_txn_count INTEGER := 0;
    v_min_amount NUMERIC;
    v_max_amount NUMERIC;
    v_avg_amount NUMERIC;
BEGIN
    -- Get the latest batch ID
    SELECT MAX(batch_id) INTO v_latest_batch 
    FROM dw.fact_spending 
    WHERE source_system = 'mastercard';
    
    -- Get total record count
    SELECT COUNT(*) INTO v_total_records
    FROM dw.fact_spending
    WHERE source_system = 'mastercard';
    
    -- Get records from latest batch
    SELECT COUNT(*) INTO v_batch_records
    FROM dw.fact_spending
    WHERE source_system = 'mastercard'
      AND batch_id = v_latest_batch;
      
    -- Get data quality metrics for inserted data
    SELECT 
        COUNT(*) FILTER (WHERE transaction_count = 0),
        MIN(total_amount),
        MAX(total_amount),
        AVG(total_amount)
    INTO 
        v_zero_txn_count,
        v_min_amount,
        v_max_amount,
        v_avg_amount
    FROM dw.fact_spending 
    WHERE source_system = 'mastercard'
      AND batch_id = v_latest_batch;
    
    RAISE NOTICE '';
    RAISE NOTICE '==================================================';
    RAISE NOTICE '              DATA SUMMARY REPORT                  ';
    RAISE NOTICE '==================================================';
    RAISE NOTICE 'Total MasterCard records:        %', v_total_records;
    RAISE NOTICE 'Records in latest batch (%):   %', v_latest_batch, v_batch_records;
    RAISE NOTICE 'Percentage of total:             %.1f%%', 
        CASE WHEN v_total_records > 0 
             THEN (v_batch_records::FLOAT / v_total_records) * 100 
             ELSE 0 END;
    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE 'Data Quality Metrics (latest batch):';
    RAISE NOTICE '  - Zero transaction records:     %', v_zero_txn_count;
    RAISE NOTICE '  - Min transaction amount:       %', v_min_amount;
    RAISE NOTICE '  - Max transaction amount:       %', v_max_amount;
    RAISE NOTICE '  - Avg transaction amount:       %.2f', v_avg_amount;
    RAISE NOTICE '==================================================';
    RAISE NOTICE '';
END $$;

-- Detailed data breakdown by date with enhanced information
SELECT 
    date_id,
    COUNT(*) as record_count,
    SUM(transaction_count) as total_transactions,
    ROUND(SUM(total_amount)::NUMERIC, 2) as total_spend,
    ROUND(AVG(avg_transaction)::NUMERIC, 2) as average_transaction,
    COUNT(DISTINCT industry_id) as industry_count,
    COUNT(DISTINCT region_id) as region_count,
    MIN(transaction_count) as min_transactions,
    MAX(transaction_count) as max_transactions,
    MIN(total_amount) as min_amount,
    MAX(total_amount) as max_amount,
    MAX(batch_id) as batch_id
FROM dw.fact_spending
WHERE source_system = 'mastercard'
GROUP BY date_id
ORDER BY date_id;