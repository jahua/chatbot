-- Synthetic test for MasterCard schema
-- This script tests the necessary schemas with minimal synthetic data

DO $$
DECLARE
    v_region_id INTEGER;
    v_industry_id INTEGER;
    v_date_id INTEGER;
    v_geography_id INTEGER;
BEGIN
    RAISE NOTICE 'Starting synthetic test for MasterCard schemas...';

    -- 1. Test that dim_region accepts 'canton', 'tourism_region', and 'district'
    BEGIN
        RAISE NOTICE 'Testing dim_region schema...';
        
        -- Test canton type
        INSERT INTO dw.dim_region (region_name, region_type, is_active, created_at)
        VALUES ('Test Canton', 'canton', TRUE, CURRENT_TIMESTAMP)
        RETURNING region_id INTO v_region_id;
        
        RAISE NOTICE 'Successfully inserted test canton with ID %', v_region_id;
        
        -- Test tourism_region type
        INSERT INTO dw.dim_region (region_name, region_type, is_active, created_at)
        VALUES ('Test Tourism Region', 'tourism_region', TRUE, CURRENT_TIMESTAMP)
        RETURNING region_id INTO v_region_id;
        
        RAISE NOTICE 'Successfully inserted test tourism_region with ID %', v_region_id;
        
        -- Test district type
        INSERT INTO dw.dim_region (region_name, region_type, is_active, created_at)
        VALUES ('Test District', 'district', TRUE, CURRENT_TIMESTAMP)
        RETURNING region_id INTO v_region_id;
        
        RAISE NOTICE 'Successfully inserted test district with ID %', v_region_id;
        
        -- Clean up test data
        DELETE FROM dw.dim_region 
        WHERE region_name IN ('Test Canton', 'Test Tourism Region', 'Test District');
        
        RAISE NOTICE 'Cleaned up test region data';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error testing dim_region: %', SQLERRM;
    END;
    
    -- 2. Test dim_geography schema
    BEGIN
        RAISE NOTICE 'Testing dim_geography schema...';
        
        -- Insert test geography
        INSERT INTO dw.dim_geography (geo_name, geo_type, country, state, city, created_at)
        VALUES ('Test Msa', 'Msa', 'Switzerland', NULL, 'Test Msa', CURRENT_TIMESTAMP)
        RETURNING geography_id INTO v_geography_id;
        
        RAISE NOTICE 'Successfully inserted test geography with ID %', v_geography_id;
        
        -- Clean up test data
        DELETE FROM dw.dim_geography WHERE geography_id = v_geography_id;
        
        RAISE NOTICE 'Cleaned up test geography data';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error testing dim_geography: %', SQLERRM;
    END;
    
    -- 3. Test dim_industry schema
    BEGIN
        RAISE NOTICE 'Testing dim_industry schema...';
        
        -- Insert test industry
        INSERT INTO dw.dim_industry (industry_name, industry_category, industry_code, is_tourism_related, is_active, created_at)
        VALUES ('Test Industry', 'Test Category', 'TEST', TRUE, TRUE, CURRENT_TIMESTAMP)
        RETURNING industry_id INTO v_industry_id;
        
        RAISE NOTICE 'Successfully inserted test industry with ID %', v_industry_id;
        
        -- Clean up test data
        DELETE FROM dw.dim_industry WHERE industry_id = v_industry_id;
        
        RAISE NOTICE 'Cleaned up test industry data';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error testing dim_industry: %', SQLERRM;
    END;
    
    -- 4. Test fact_spending schema
    BEGIN
        RAISE NOTICE 'Testing fact_spending schema...';
        
        -- First need real dimension values
        INSERT INTO dw.dim_region (region_name, region_type, is_active, created_at)
        VALUES ('Test Region', 'tourism_region', TRUE, CURRENT_TIMESTAMP)
        RETURNING region_id INTO v_region_id;
        
        INSERT INTO dw.dim_industry (industry_name, industry_category, industry_code, is_tourism_related, is_active, created_at)
        VALUES ('Test Industry', 'Test Category', 'TEST', TRUE, TRUE, CURRENT_TIMESTAMP)
        RETURNING industry_id INTO v_industry_id;
        
        -- Use existing date
        SELECT date_id INTO v_date_id FROM dw.dim_date WHERE year = 2023 AND month = 1 AND day = 1;
        
        -- Insert test fact
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
            v_date_id,
            v_industry_id,
            v_region_id,
            100,
            1000.50,
            10.01,
            'mastercard',
            999,
            CURRENT_TIMESTAMP
        );
        
        RAISE NOTICE 'Successfully inserted test fact record';
        
        -- Clean up test data
        DELETE FROM dw.fact_spending 
        WHERE date_id = v_date_id 
        AND industry_id = v_industry_id 
        AND region_id = v_region_id;
        
        DELETE FROM dw.dim_region WHERE region_id = v_region_id;
        DELETE FROM dw.dim_industry WHERE industry_id = v_industry_id;
        
        RAISE NOTICE 'Cleaned up test fact data';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error testing fact_spending: %', SQLERRM;
        
        -- Attempt cleanup even after error
        DELETE FROM dw.dim_region WHERE region_name = 'Test Region';
        DELETE FROM dw.dim_industry WHERE industry_name = 'Test Industry';
    END;
    
    RAISE NOTICE 'Completed synthetic test for MasterCard schemas';
END $$; 