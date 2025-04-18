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
    v_date_count INTEGER := 0;
    v_start_date DATE := '2022-03-01';
    v_end_date DATE := '2022-03-02';
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_source_count INTEGER := 0;
    v_mapped_source_count INTEGER := 0;
BEGIN
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
    CREATE TEMP TABLE temp_mastercard_source AS
    SELECT *
    FROM data_lake.master_card
    WHERE txn_date BETWEEN v_start_date AND v_end_date;
    
    -- Create indexes on the temp table to speed up joins (Performance optimization #2)
    CREATE INDEX idx_temp_mc_industry ON temp_mastercard_source(industry);
    CREATE INDEX idx_temp_mc_geo_name ON temp_mastercard_source(geo_name);
    CREATE INDEX idx_temp_mc_geo_type ON temp_mastercard_source(geo_type);
    CREATE INDEX idx_temp_mc_txn_date ON temp_mastercard_source(txn_date);
    
    -- Source data analysis (using temp table for better performance)
    RAISE NOTICE '> Analyzing source data...';
    SELECT COUNT(*) INTO v_source_count
    FROM temp_mastercard_source;
    
    -- Create a temporary region mapping table to handle encoding issues (Performance optimization #3)
    CREATE TEMP TABLE temp_region_mapping AS
    SELECT 
        mc_geo_name,
        mc_geo_type,
        region_id,
        region_type
    FROM (
        SELECT DISTINCT
            geo_name AS mc_geo_name,
            geo_type AS mc_geo_type,
            dr.region_id,
            dr.region_type
        FROM (
            -- Use VALUES clause to explicitly define mappings (Performance optimization #4)
            VALUES 
                ('Zurich', 'Zürich'),
                ('Zurich', 'Zurich'),
                ('Geneva', 'Genève'),
                ('Geneva', 'Geneva')
            UNION
            -- For standard cases, use the geo_name directly
            SELECT DISTINCT geo_name, geo_name 
            FROM temp_mastercard_source
        ) AS name_mappings(mc_geo_name, region_name)
        JOIN dw.dim_region dr ON dr.region_name = name_mappings.region_name
        CROSS JOIN (
            -- Define geo_type to region_type mappings
            VALUES 
                ('State', 'canton'),
                ('Msa', 'tourism_region'),
                ('Country', 'district')
        ) AS type_mappings(mc_geo_type, region_type)
    ) mapped
    WHERE (mc_geo_type, region_type) IN (
        ('State', 'canton'),
        ('Msa', 'tourism_region'),
        ('Country', 'district')
    );
    
    -- Create index on the mapping table (Performance optimization #5)
    CREATE INDEX idx_temp_region_mapping ON temp_region_mapping(mc_geo_name, mc_geo_type);
    
    -- Check how many records we can map using the optimized approach
    SELECT COUNT(*) INTO v_mapped_source_count
    FROM temp_mastercard_source mc
    JOIN dw.dim_industry di ON di.industry_name = mc.industry AND di.industry_code IS NOT NULL
    JOIN temp_region_mapping rm ON rm.mc_geo_name = mc.geo_name AND rm.mc_geo_type = mc.geo_type;
    
    RAISE NOTICE '  - Total source records: %', v_source_count;
    RAISE NOTICE '  - Mappable records: % (%.1f%%)', 
        v_mapped_source_count, 
        CASE WHEN v_source_count > 0 
             THEN (v_mapped_source_count::FLOAT / v_source_count) * 100 
             ELSE 0 END;
    
    -- Insert MasterCard data using the optimized approach (Performance optimization #6)
    RAISE NOTICE '> Starting data insertion at %...', clock_timestamp();
    
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
        rm.region_id,
        SUM(mc.txn_cnt) AS transaction_count,
        SUM(mc.txn_amt) AS total_amount,
        CASE WHEN SUM(mc.txn_cnt) > 0 THEN SUM(mc.txn_amt) / SUM(mc.txn_cnt) ELSE 0 END AS avg_transaction,
        'mastercard' AS source_system,
        v_batch_id AS batch_id,
        v_now AS created_at
    FROM temp_mastercard_source mc
    -- Join to industry dimension using simple matching
    JOIN dw.dim_industry di ON di.industry_name = mc.industry AND di.industry_code IS NOT NULL
    -- Join to our optimized region mapping table
    JOIN temp_region_mapping rm ON rm.mc_geo_name = mc.geo_name AND rm.mc_geo_type = mc.geo_type
    GROUP BY 
        TO_CHAR(mc.txn_date, 'YYYYMMDD')::INTEGER,
        di.industry_id,
        rm.region_id;
    
    -- Get the count of rows affected
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- Get date count
    SELECT COUNT(DISTINCT date_id) INTO v_date_count
    FROM dw.fact_spending
    WHERE source_system = 'mastercard'
      AND batch_id = v_batch_id;
    
    -- Record ETL metadata
    RAISE NOTICE '> Recording ETL metadata...';
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
    
    -- End timing
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    
    -- Final summary
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
    
    -- Clean up temporary tables (Performance optimization #7)
    DROP TABLE IF EXISTS temp_mastercard_source;
    DROP TABLE IF EXISTS temp_region_mapping;
    
EXCEPTION WHEN OTHERS THEN
    -- Enhanced error reporting
    RAISE NOTICE '';
    RAISE NOTICE '==================================================';
    RAISE NOTICE '                 ERROR OCCURRED                    ';
    RAISE NOTICE '==================================================';
    RAISE NOTICE 'Error message: %', SQLERRM;
    RAISE NOTICE 'Error context: %', SQLSTATE;
    RAISE NOTICE 'Time of error: %', clock_timestamp();
    
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
    DROP TABLE IF EXISTS temp_region_mapping;
    
    RAISE NOTICE 'Error recorded in ETL metadata with batch ID: %', v_batch_id;
    RAISE NOTICE '==================================================';
END $$;

-- Enhanced results report
DO $$
DECLARE
    v_total_records INTEGER;
    v_latest_batch INTEGER;
    v_batch_records INTEGER;
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
    RAISE NOTICE '==================================================';
    RAISE NOTICE '';
END $$;

-- Detailed data breakdown by date
SELECT 
    date_id,
    COUNT(*) as record_count,
    SUM(transaction_count) as total_transactions,
    ROUND(SUM(total_amount)::NUMERIC, 2) as total_spend,
    ROUND(AVG(avg_transaction)::NUMERIC, 2) as average_transaction,
    COUNT(DISTINCT industry_id) as industry_count,
    COUNT(DISTINCT region_id) as region_count,
    MAX(batch_id) as batch_id
FROM dw.fact_spending
WHERE source_system = 'mastercard'
GROUP BY date_id
ORDER BY date_id;