-- Optimized MasterCard Data Loading Script with Geospatial Support
-- Loads transaction data from data_lake.master_card to dw.fact_spending

-- Clear screen and display header
DO $$
BEGIN
    RAISE NOTICE '==================================================';
    RAISE NOTICE '   Optimized MasterCard Data Load with Geospatial   ';
    RAISE NOTICE '==================================================';
    RAISE NOTICE 'Started at: %', CURRENT_TIMESTAMP;
    RAISE NOTICE '';
END $$;

-- Main data loading block with optimizations
DO $$
DECLARE
    v_batch_id INTEGER;
    v_now TIMESTAMP := CURRENT_TIMESTAMP;
    v_count INTEGER := 0;
    v_industry_count INTEGER := 0;
    v_region_count INTEGER := 0;
    v_date_count INTEGER := 0;
    v_start_date DATE := '2023-01-01';
    v_end_date DATE := '2023-12-31';
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_source_count INTEGER := 0;
    v_mapped_source_count INTEGER := 0;
    v_added_industries INTEGER := 0;
    v_added_regions INTEGER := 0;
    v_has_postgis BOOLEAN;
    v_unmapped_region_count INTEGER := 0;
    v_unmapped_industry_count INTEGER := 0;
    v_unmapped_records_count INTEGER := 0;
BEGIN
    -- Set start time for tracking
    v_start_time := clock_timestamp();
    
    -- Check if PostGIS is available
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') INTO v_has_postgis;
    
    -- Get next batch ID
    v_batch_id := nextval('dw.etl_metadata_etl_id_seq');
    RAISE NOTICE 'Process initiated with batch ID: %', v_batch_id;
    
    -- Environment check - just counting once
    RAISE NOTICE '> Environment check...';
    SELECT COUNT(*) INTO v_industry_count FROM dw.dim_industry WHERE industry_code IS NOT NULL;
    SELECT COUNT(*) INTO v_region_count FROM dw.dim_region;
    
    RAISE NOTICE '  - Available industries: %', v_industry_count;
    RAISE NOTICE '  - Available regions: %', v_region_count;
    RAISE NOTICE '  - PostGIS extension: %', CASE WHEN v_has_postgis THEN 'Available' ELSE 'Not available' END;
    
    -- OPTIMIZATION 1: Create focused temp table with only necessary columns
    -- This reduces memory usage and improves join performance
    CREATE TEMP TABLE temp_mastercard_source AS
    SELECT
        txn_date,
        industry,
        geo_name,
        geo_type,
        central_latitude,
        central_longitude,
        txn_amt,
        txn_cnt
    FROM data_lake.master_card
    WHERE txn_date BETWEEN v_start_date AND v_end_date;
    
    -- Get source count just once
    SELECT COUNT(*) INTO v_source_count FROM temp_mastercard_source;
    RAISE NOTICE '> Source records found: %', v_source_count;
    
    -- OPTIMIZATION 2: Create efficient indexes
    CREATE INDEX idx_temp_mc_composite ON temp_mastercard_source(industry, geo_name, geo_type);
    CREATE INDEX idx_temp_mc_txn_date ON temp_mastercard_source(txn_date);
    
    -- OPTIMIZATION 3: Handle industry mapping efficiently with a single pass
    -- First add any missing industries (with pre-aggregation of distinct values)
    WITH distinct_industries AS (
        SELECT DISTINCT industry FROM temp_mastercard_source
    )
    INSERT INTO dw.dim_industry (
        industry_code,
        industry_name,
        industry_category,
        is_active,
        created_at,
        updated_at
    )
    SELECT 
        'MC_' || SUBSTRING(UPPER(REPLACE(REPLACE(REPLACE(industry, ' ', '_'), '-', '_'), '&', 'AND')), 1, 16) as industry_code,
        industry as industry_name,
        industry as industry_category,
        TRUE as is_active,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM distinct_industries di
    WHERE NOT EXISTS (
        SELECT 1 
        FROM dw.dim_industry existing
        WHERE LOWER(existing.industry_name) = LOWER(di.industry)
    );
    
    GET DIAGNOSTICS v_added_industries = ROW_COUNT;
    RAISE NOTICE '  - Added % new industries to dimension table', v_added_industries;
    
    -- OPTIMIZATION 4: Create efficient industry mapping with only necessary columns
    CREATE TEMP TABLE temp_industry_mapping AS
    SELECT 
        mc.industry AS source_industry,
        di.industry_id
    FROM (
        SELECT DISTINCT industry 
        FROM temp_mastercard_source
    ) mc
    JOIN dw.dim_industry di ON LOWER(di.industry_name) = LOWER(mc.industry);
    
    CREATE INDEX idx_temp_industry_map ON temp_industry_mapping(source_industry);
    
    -- OPTIMIZATION 5: Create region mappings more efficiently
    -- First, create a temp table with distinct regions from source
    CREATE TEMP TABLE temp_distinct_regions AS
        SELECT DISTINCT
        geo_name,
        geo_type,
        AVG(central_latitude) AS central_latitude,
        AVG(central_longitude) AS central_longitude
            FROM temp_mastercard_source
    GROUP BY geo_name, geo_type;
    
    -- IMPROVEMENT: Create a table to track potential canton/city name conflicts
    CREATE TEMP TABLE potential_region_conflicts AS
    SELECT 
        geo_name,
        array_agg(DISTINCT geo_type) AS geo_types,
        COUNT(DISTINCT geo_type) AS type_count
    FROM temp_distinct_regions
    GROUP BY geo_name
    HAVING COUNT(DISTINCT geo_type) > 1;
    
    -- First, add all cantons
    WITH standardized_regions AS (
        SELECT
            geo_name AS original_geo_name,
            geo_type AS original_geo_type,
            geo_name AS standard_name,
            'canton' AS region_type, -- Only for cantons in this batch
            central_latitude,
            central_longitude
        FROM temp_distinct_regions
        WHERE geo_type = 'State' -- MasterCard's type for cantons
    )
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        latitude,
        longitude,
        is_active,
        created_at
    )
    SELECT 
        sr.standard_name,
        sr.region_type,
        sr.central_latitude,
        sr.central_longitude,
        TRUE,
        CURRENT_TIMESTAMP
    FROM standardized_regions sr
    WHERE NOT EXISTS (
        SELECT 1 
        FROM dw.dim_region dr
        WHERE LOWER(dr.region_name) = LOWER(sr.standard_name)
        AND dr.region_type = sr.region_type
    );
    
    -- Then, add all cities with conflict handling
    WITH standardized_regions AS (
        SELECT
            geo_name AS original_geo_name,
            geo_type AS original_geo_type,
            geo_name AS standard_name,
            'city' AS region_type, -- Only for cities in this batch
            central_latitude,
            central_longitude
        FROM temp_distinct_regions
        WHERE geo_type = 'Msa' -- MasterCard's type for cities
    )
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        latitude,
        longitude,
        is_active,
        created_at
    )
    SELECT 
        -- Add 'City' suffix if this city name also exists as a canton
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM dw.dim_region dr 
                WHERE LOWER(dr.region_name) = LOWER(sr.standard_name)
                AND dr.region_type = 'canton'
            ) THEN sr.standard_name || ' City'
            ELSE sr.standard_name
        END AS region_name,
        sr.region_type,
        sr.central_latitude,
        sr.central_longitude,
        TRUE,
        CURRENT_TIMESTAMP
    FROM standardized_regions sr
    WHERE NOT EXISTS (
        SELECT 1 
        FROM dw.dim_region dr
        WHERE (LOWER(dr.region_name) = LOWER(sr.standard_name) OR 
               LOWER(dr.region_name) = LOWER(sr.standard_name || ' City'))
        AND dr.region_type = sr.region_type
    );
    
    -- Finally, add countries and other types
    WITH standardized_regions AS (
        SELECT
            geo_name AS original_geo_name,
            geo_type AS original_geo_type,
            geo_name AS standard_name,
            CASE
                WHEN geo_type = 'Country' THEN 'country'
                WHEN geo_type = 'Province' THEN 'canton'
                WHEN geo_type = 'Region' THEN 'tourism_region'
                ELSE 'district' -- For any other types, classify as district
            END AS region_type,
            central_latitude,
            central_longitude
        FROM temp_distinct_regions
        WHERE geo_type NOT IN ('State', 'Msa') -- Skip already processed types
    )
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        latitude,
        longitude,
        is_active,
        created_at
    )
    SELECT 
        sr.standard_name,
        sr.region_type,
        sr.central_latitude,
        sr.central_longitude,
        TRUE,
        CURRENT_TIMESTAMP
    FROM standardized_regions sr
    WHERE NOT EXISTS (
        SELECT 1 
        FROM dw.dim_region dr
        WHERE LOWER(dr.region_name) = LOWER(sr.standard_name)
        AND dr.region_type = sr.region_type
    );
    
    GET DIAGNOSTICS v_added_regions = ROW_COUNT;
    RAISE NOTICE '  - Added % new regions to dimension table', v_added_regions;
    
    -- OPTIMIZATION 6: Create an efficient region mapping table with improved naming
    CREATE TEMP TABLE temp_region_mapping AS
    SELECT 
        dr.region_id,
        tr.geo_name AS source_geo_name,
        tr.geo_type AS source_geo_type
    FROM temp_distinct_regions tr
    JOIN dw.dim_region dr ON 
        (
            -- For cities with possible canton conflicts, check both regular name and name with City suffix
            (tr.geo_type = 'Msa' AND dr.region_type = 'city' AND 
             (
                 (LOWER(dr.region_name) = LOWER(tr.geo_name)) OR
                 (LOWER(dr.region_name) = LOWER(tr.geo_name || ' City'))
             )
            ) OR
            -- For states (cantons)
            (tr.geo_type = 'State' AND dr.region_type = 'canton' AND 
             LOWER(dr.region_name) = LOWER(tr.geo_name)
            ) OR
            -- For countries and other types
            (tr.geo_type = 'Country' AND dr.region_type = 'country' AND 
             LOWER(dr.region_name) = LOWER(tr.geo_name)
            ) OR
            -- For other geo types
            (tr.geo_type = 'Province' AND dr.region_type = 'canton' AND 
             LOWER(dr.region_name) = LOWER(tr.geo_name)
            ) OR
            -- For tourism regions
            (tr.geo_type = 'Region' AND dr.region_type = 'tourism_region' AND 
             LOWER(dr.region_name) = LOWER(tr.geo_name)
            ) OR
            -- For other districts
            (tr.geo_type NOT IN ('Msa', 'State', 'Country', 'Province', 'Region') AND dr.region_type = 'district' AND 
             LOWER(dr.region_name) = LOWER(tr.geo_name)
            )
        );
    
    CREATE INDEX idx_temp_region_map ON temp_region_mapping(source_geo_name, source_geo_type);
    
    -- Create a table to log unmapped records
    CREATE TEMP TABLE unmapped_records_log (
        record_id SERIAL PRIMARY KEY,
        source_geo_name VARCHAR(255),
        source_geo_type VARCHAR(50),
        source_industry VARCHAR(255),
        is_region_unmapped BOOLEAN,
        is_industry_unmapped BOOLEAN,
        unmapped_reason TEXT,
        record_count INTEGER,
        transaction_count NUMERIC,
        transaction_amount NUMERIC,
        txn_date DATE,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Log unmapped records for later review - Include the date
    INSERT INTO unmapped_records_log (
        source_geo_name,
        source_geo_type,
        source_industry,
        is_region_unmapped,
        is_industry_unmapped,
        unmapped_reason,
        record_count,
        transaction_count,
        transaction_amount,
        txn_date
    )
    SELECT 
        mc.geo_name,
        mc.geo_type,
        mc.industry,
        NOT EXISTS (
            SELECT 1 FROM temp_region_mapping rm 
            WHERE rm.source_geo_name = mc.geo_name AND rm.source_geo_type = mc.geo_type
        ) AS is_region_unmapped,
        NOT EXISTS (
            SELECT 1 FROM temp_industry_mapping im 
            WHERE im.source_industry = mc.industry
        ) AS is_industry_unmapped,
        CASE 
            WHEN NOT EXISTS (
                SELECT 1 FROM temp_region_mapping rm 
                WHERE rm.source_geo_name = mc.geo_name AND rm.source_geo_type = mc.geo_type
            ) AND NOT EXISTS (
                SELECT 1 FROM temp_industry_mapping im 
                WHERE im.source_industry = mc.industry
            ) THEN 'Both region and industry unmapped'
            WHEN NOT EXISTS (
                SELECT 1 FROM temp_region_mapping rm 
                WHERE rm.source_geo_name = mc.geo_name AND rm.source_geo_type = mc.geo_type
            ) THEN 'Region unmapped'
            WHEN NOT EXISTS (
                SELECT 1 FROM temp_industry_mapping im 
                WHERE im.source_industry = mc.industry
            ) THEN 'Industry unmapped'
            ELSE 'Other issue'
        END AS unmapped_reason,
        COUNT(*) AS record_count,
        SUM(mc.txn_cnt) AS transaction_count,
        SUM(mc.txn_amt) AS transaction_amount,
        mc.txn_date
    FROM temp_mastercard_source mc
    WHERE 
        NOT EXISTS (
            SELECT 1 FROM temp_region_mapping rm 
            WHERE rm.source_geo_name = mc.geo_name AND rm.source_geo_type = mc.geo_type
        )
        OR 
        NOT EXISTS (
            SELECT 1 FROM temp_industry_mapping im 
            WHERE im.source_industry = mc.industry
        )
    GROUP BY 
        mc.geo_name, 
        mc.geo_type, 
        mc.industry,
        mc.txn_date;
    
    -- Get counts for reporting
    SELECT COUNT(*) INTO v_unmapped_records_count FROM unmapped_records_log;
    
    -- Fix: Using subquery for COUNT DISTINCT with multiple columns
    SELECT COUNT(*) INTO v_unmapped_region_count 
    FROM (
        SELECT DISTINCT source_geo_name, source_geo_type 
        FROM unmapped_records_log 
        WHERE is_region_unmapped = TRUE
    ) AS distinct_regions;
    
    SELECT COUNT(DISTINCT source_industry) INTO v_unmapped_industry_count 
    FROM unmapped_records_log WHERE is_industry_unmapped = TRUE;
    
    -- Get accurate count of mappable records
    SELECT COUNT(*) INTO v_mapped_source_count
    FROM temp_mastercard_source mc
    JOIN temp_industry_mapping im ON im.source_industry = mc.industry
    JOIN temp_region_mapping rm ON rm.source_geo_name = mc.geo_name AND rm.source_geo_type = mc.geo_type;
    
    RAISE NOTICE '  - Total source records: %', v_source_count;
    RAISE NOTICE '  - Mappable records: % (%.1f%%)', 
        v_mapped_source_count, 
        CASE WHEN v_source_count > 0 
             THEN (v_mapped_source_count::FLOAT / v_source_count) * 100 
             ELSE 0 END;
    
    RAISE NOTICE '> Unmapped records identified: % records (% regions, % industries)',
                  v_unmapped_records_count, v_unmapped_region_count, v_unmapped_industry_count;
    
    -- OPTIMIZATION 7: Aggregation before insertion - only for records with valid mappings
    -- Use INNER JOINs to skip unmapped records
    CREATE TEMP TABLE temp_aggregated_data AS
    SELECT 
        TO_CHAR(mc.txn_date, 'YYYYMMDD')::INTEGER AS date_id,
        im.industry_id,
        rm.region_id,
        ROUND(SUM(mc.txn_cnt))::INTEGER AS transaction_count,
        ROUND(SUM(mc.txn_amt)::NUMERIC, 2) AS total_amount,
        CASE WHEN SUM(mc.txn_cnt) > 0 
             THEN ROUND((SUM(mc.txn_amt) / SUM(mc.txn_cnt))::NUMERIC, 2) 
             ELSE 0 
        END AS avg_transaction,
        AVG(mc.central_latitude) AS geo_latitude,
        AVG(mc.central_longitude) AS geo_longitude
    FROM temp_mastercard_source mc
    INNER JOIN temp_industry_mapping im ON im.source_industry = mc.industry
    INNER JOIN temp_region_mapping rm ON rm.source_geo_name = mc.geo_name AND rm.source_geo_type = mc.geo_type
    GROUP BY 
        TO_CHAR(mc.txn_date, 'YYYYMMDD')::INTEGER,
        im.industry_id,
        rm.region_id;
    
    -- OPTIMIZATION 8: Efficient insertion with proper conflict handling
    RAISE NOTICE '> Starting data insertion with geospatial data at %...', clock_timestamp();
    
    INSERT INTO dw.fact_spending (
        date_id,
        industry_id,
        region_id,
        transaction_count,
        total_amount,
        avg_transaction,
        geo_latitude,
        geo_longitude,
        source_system,
        batch_id,
        created_at
    )
    SELECT 
        date_id,
        industry_id,
        region_id,
        transaction_count,
        total_amount,
        avg_transaction,
        geo_latitude,
        geo_longitude,
        'mastercard' AS source_system,
        v_batch_id AS batch_id,
        v_now AS created_at
    FROM temp_aggregated_data
    ON CONFLICT (date_id, region_id, industry_id, source_system)
    DO UPDATE SET
        transaction_count = EXCLUDED.transaction_count,
        total_amount = EXCLUDED.total_amount,
        avg_transaction = EXCLUDED.avg_transaction,
        geo_latitude = EXCLUDED.geo_latitude,
        geo_longitude = EXCLUDED.geo_longitude,
        batch_id = EXCLUDED.batch_id,
        created_at = EXCLUDED.created_at;
    
    -- Get the count of rows affected
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- Get date count
    SELECT COUNT(DISTINCT date_id) INTO v_date_count
    FROM dw.fact_spending
    WHERE source_system = 'mastercard'
      AND batch_id = v_batch_id;
    
    -- Create permanent tracking table for unmapped records and export them
    CREATE TABLE IF NOT EXISTS dw.unmapped_records_history (
        record_id SERIAL PRIMARY KEY,
        batch_id INTEGER,
        source_geo_name VARCHAR(255),
        source_geo_type VARCHAR(50),
        source_industry VARCHAR(255),
        is_region_unmapped BOOLEAN,
        is_industry_unmapped BOOLEAN,
        unmapped_reason TEXT,
        record_count INTEGER,
        transaction_count NUMERIC,
        transaction_amount NUMERIC,
        txn_date DATE,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Export unmapped records for later analysis
    INSERT INTO dw.unmapped_records_history (
        batch_id,
        source_geo_name,
        source_geo_type,
        source_industry,
        is_region_unmapped,
        is_industry_unmapped,
        unmapped_reason,
        record_count,
        transaction_count,
        transaction_amount,
        txn_date,
        logged_at
    )
    SELECT
        v_batch_id,
        source_geo_name,
        source_geo_type,
        source_industry,
        is_region_unmapped,
        is_industry_unmapped,
        unmapped_reason,
        record_count,
        transaction_count,
        transaction_amount,
        txn_date,
        logged_at
    FROM unmapped_records_log;
    
    RAISE NOTICE '> Unmapped records exported to dw.unmapped_records_history for review';
    
    -- Record ETL metadata
    RAISE NOTICE '> Recording ETL metadata...';
    
    INSERT INTO dw.etl_metadata (
        etl_id,
        task_name,
        status,
        message,
        source_system,
        start_time,
        end_time
    ) VALUES (
        v_batch_id,
        'optimized_mastercard_geo_load',
        'completed',
        'Successfully loaded MasterCard data with geospatial coordinates for ' || v_date_count || 
        ' days between ' || v_start_date || ' and ' || v_end_date ||
        '. Records: ' || v_count || '. Unmapped records: ' || v_unmapped_records_count,
        'mastercard',
        v_start_time,
        CURRENT_TIMESTAMP
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
    RAISE NOTICE '  Unmapped records: %', v_unmapped_records_count;
    RAISE NOTICE '  Unmapped regions: %', v_unmapped_region_count;
    RAISE NOTICE '  Unmapped industries: %', v_unmapped_industry_count;
    RAISE NOTICE '  Inserted records: %', v_count;
    RAISE NOTICE '  Added industries: %', v_added_industries;
    RAISE NOTICE '  Added regions:    %', v_added_regions;
    RAISE NOTICE '  Coverage:         %.1f%%', 
        CASE WHEN v_source_count > 0 
             THEN (v_mapped_source_count::FLOAT / v_source_count) * 100 
             ELSE 0 END;
    RAISE NOTICE '  Batch ID:         %', v_batch_id;
    RAISE NOTICE '  Dates processed:  %', v_date_count;
    RAISE NOTICE '==================================================';
    
    -- OPTIMIZATION 9: Efficient cleanup in order
    DROP TABLE IF EXISTS temp_aggregated_data;
    DROP TABLE IF EXISTS temp_region_mapping;
    DROP TABLE IF EXISTS temp_distinct_regions;
    DROP TABLE IF EXISTS potential_region_conflicts;
    DROP TABLE IF EXISTS temp_industry_mapping;
    DROP TABLE IF EXISTS temp_mastercard_source;
    DROP TABLE IF EXISTS unmapped_records_log;
    
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
    BEGIN
    INSERT INTO dw.etl_metadata (
        etl_id,
        task_name,
        status,
        message,
        source_system,
        start_time,
        end_time
    ) VALUES (
        v_batch_id,
        'optimized_mastercard_geo_load',
        'failed',
        'Error loading MasterCard data with geospatial information: ' || SQLERRM || ' [' || SQLSTATE || ']',
        'mastercard',
        v_start_time,
        CURRENT_TIMESTAMP
    );
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not log error to etl_metadata: %', SQLERRM;
    END;
    
    -- Clean up temporary tables even on error
    DROP TABLE IF EXISTS temp_aggregated_data;
    DROP TABLE IF EXISTS temp_region_mapping;
    DROP TABLE IF EXISTS temp_distinct_regions;
    DROP TABLE IF EXISTS potential_region_conflicts;
    DROP TABLE IF EXISTS temp_industry_mapping;
    DROP TABLE IF EXISTS temp_mastercard_source;
    DROP TABLE IF EXISTS unmapped_records_log;
    
    RAISE NOTICE 'Error recorded in ETL metadata with batch ID: %', v_batch_id;
    RAISE NOTICE '==================================================';
END $$;

-- Compact data summary report
SELECT 
    d.full_date,
    COUNT(*) as fact_count,
    SUM(transaction_count) as total_transactions,
    ROUND(SUM(total_amount)::NUMERIC, 2) as total_spend,
    COUNT(DISTINCT industry_id) as industry_count,
    COUNT(DISTINCT region_id) as region_count,
    MAX(batch_id) as batch_id
FROM 
    dw.fact_spending fs
JOIN 
    dw.dim_date d ON fs.date_id = d.date_id
WHERE 
    source_system = 'mastercard'
GROUP BY 
    d.full_date
ORDER BY 
    d.full_date;

-- Create view for unmapped records report
CREATE OR REPLACE VIEW dw.vw_unmapped_records_summary AS
SELECT 
    'Unmapped record summary' as report_name,
    batch_id,
    txn_date,
    COUNT(*) as total_unmapped_records,
    SUM(transaction_count) as total_unmapped_transactions,
    ROUND(SUM(transaction_amount)::NUMERIC, 2) as total_unmapped_amount
FROM dw.unmapped_records_history
GROUP BY batch_id, txn_date
ORDER BY batch_id DESC, txn_date;

-- Create view for top unmapped regions
CREATE OR REPLACE VIEW dw.vw_unmapped_regions AS
SELECT 
    'Top unmapped regions' as report_name,
    batch_id,
    source_geo_name,
    source_geo_type,
    COUNT(*) as occurrence_count,
    SUM(transaction_count) as total_transactions,
    ROUND(SUM(transaction_amount)::NUMERIC, 2) as total_amount
FROM dw.unmapped_records_history
WHERE is_region_unmapped = TRUE
GROUP BY batch_id, source_geo_name, source_geo_type
ORDER BY batch_id DESC, COUNT(*) DESC;

-- Create view for top unmapped industries
CREATE OR REPLACE VIEW dw.vw_unmapped_industries AS
SELECT 
    'Top unmapped industries' as report_name,
    batch_id,
    source_industry,
    COUNT(*) as occurrence_count,
    SUM(transaction_count) as total_transactions,
    ROUND(SUM(transaction_amount)::NUMERIC, 2) as total_amount
FROM dw.unmapped_records_history
WHERE is_industry_unmapped = TRUE
GROUP BY batch_id, source_industry
ORDER BY batch_id DESC, COUNT(*) DESC;

-- Show summary of latest load including unmapped records
SELECT * FROM dw.vw_unmapped_records_summary 
WHERE batch_id = (SELECT MAX(batch_id) FROM dw.unmapped_records_history)
LIMIT 10;