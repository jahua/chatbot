-- MasterCard Simple Direct Injection Test Script
-- This script performs a very simple insertion of test data

-- First verify we have the necessary tables
DO $$
BEGIN
    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE 'Starting simple MasterCard data test';
    RAISE NOTICE 'Timestamp: %', CURRENT_TIMESTAMP;
    RAISE NOTICE '--------------------------------------------------';
END $$;

-- Create a small batch of test data (10 records)
DO $$
DECLARE
    v_batch_id INTEGER;
    v_now TIMESTAMP := CURRENT_TIMESTAMP;
    v_count INTEGER := 0;
    v_region_id INTEGER;
    v_industry_id INTEGER;
BEGIN
    -- Get next batch ID
    v_batch_id := nextval('dw.etl_metadata_etl_id_seq');
    RAISE NOTICE 'Using batch ID: %', v_batch_id;

    -- Get a valid region_id for testing
    SELECT region_id INTO v_region_id
    FROM dw.dim_region
    WHERE region_type = 'canton'
    LIMIT 1;
    
    RAISE NOTICE 'Using region_id: %', v_region_id;
    
    -- Get a valid industry_id for testing
    SELECT industry_id INTO v_industry_id
    FROM dw.dim_industry
    WHERE industry_code IS NOT NULL
    LIMIT 1;
    
    RAISE NOTICE 'Using industry_id: %', v_industry_id;
    
    -- Insert 10 test records for different dates
    FOR i IN 1..10 LOOP
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
        ) VALUES (
            20220100 + i, -- Different date_id for each record
            v_industry_id,
            v_region_id,
            100 * i, -- Different transaction count
            1000.00 * i, -- Different total amount
            10.00, -- Same avg transaction
            'mastercard',
            v_batch_id,
            v_now
        );
        
        v_count := v_count + 1;
    END LOOP;
    
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
        'inject_mastercard_simple_test',
        'mastercard',
        'COMPLETED',
        v_now,
        CURRENT_TIMESTAMP,
        v_count,
        v_count,
        'Successfully injected simple MasterCard test data. Records: ' || v_count
    );
    
    RAISE NOTICE 'Successfully inserted % test records with batch ID %', v_count, v_batch_id;
    RAISE NOTICE '--------------------------------------------------';
END $$;

-- Check the results
SELECT 
    'Count by date' AS check_type,
    COUNT(*) AS total_records
FROM dw.fact_spending;

SELECT 
    date_id,
    transaction_count,
    total_amount,
    avg_transaction,
    batch_id,
    created_at
FROM dw.fact_spending
ORDER BY date_id;

-- Print completion message
DO $$
BEGIN
    RAISE NOTICE '--------------------------------------------------';
    RAISE NOTICE 'Simple MasterCard test load complete!';
    RAISE NOTICE '--------------------------------------------------';
END $$; 