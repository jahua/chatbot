-- MasterCard Spending ETL Script
-- This script loads MasterCard spending data from data_lake into the data warehouse
-- for analysis of spending patterns by industry, geography, and time

-- Step 1: Create required tables if they don't exist
DO $$
BEGIN
    -- Create dim_industry table if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'dim_industry') THEN
        CREATE TABLE dw.dim_industry (
            industry_id SERIAL PRIMARY KEY,
            industry_code VARCHAR(20),
            industry_name VARCHAR(100) NOT NULL,
            industry_category VARCHAR(50),
            is_tourism_related BOOLEAN DEFAULT FALSE,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT dim_industry_name_uq UNIQUE (industry_name)
        );
        
        -- Add indexes for better query performance
        CREATE INDEX idx_dim_industry_name ON dw.dim_industry(industry_name);
        
        RAISE NOTICE 'Created dim_industry table';
    ELSE
        -- Check if we need to add the is_tourism_related column
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_schema = 'dw' AND table_name = 'dim_industry' 
                      AND column_name = 'is_tourism_related') THEN
            ALTER TABLE dw.dim_industry ADD COLUMN is_tourism_related BOOLEAN DEFAULT FALSE;
            RAISE NOTICE 'Added is_tourism_related column to dim_industry table';
        END IF;
        
        -- Check if we need to add the industry_category column
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_schema = 'dw' AND table_name = 'dim_industry' 
                      AND column_name = 'industry_category') THEN
            ALTER TABLE dw.dim_industry ADD COLUMN industry_category VARCHAR(50);
            RAISE NOTICE 'Added industry_category column to dim_industry table';
        END IF;
    END IF;

    -- Create dim_geography table if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'dim_geography') THEN
        CREATE TABLE dw.dim_geography (
            geography_id SERIAL PRIMARY KEY,
            geo_name VARCHAR(100) NOT NULL,
            geo_type VARCHAR(50) NOT NULL,
            country VARCHAR(100),
            state VARCHAR(100),
            city VARCHAR(100),
            latitude NUMERIC(10,6),
            longitude NUMERIC(10,6),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT dim_geography_name_type_uq UNIQUE (geo_name, geo_type)
        );
        
        -- Add indexes for better query performance
        CREATE INDEX idx_dim_geography_name ON dw.dim_geography(geo_name);
        CREATE INDEX idx_dim_geography_type ON dw.dim_geography(geo_type);
        
        RAISE NOTICE 'Created dim_geography table';
    END IF;

    -- Create mapping table for geography to region if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'temp_geography_region_map') THEN
        CREATE TABLE dw.temp_geography_region_map (
            geography_id INTEGER NOT NULL,
            region_id INTEGER NOT NULL,
            geo_name VARCHAR(100) NOT NULL,
            geo_type VARCHAR(50) NOT NULL,
            PRIMARY KEY (geography_id, region_id)
        );
        
        RAISE NOTICE 'Created temporary mapping table for geography to region';
    END IF;

    -- Fix fact_spending table to include avg_transaction
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'fact_spending') THEN
        -- Check if avg_transaction column exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_schema = 'dw' AND table_name = 'fact_spending' 
                      AND column_name = 'avg_transaction') THEN
            -- Add the missing column
            ALTER TABLE dw.fact_spending ADD COLUMN avg_transaction NUMERIC(12,2);
            RAISE NOTICE 'Added missing avg_transaction column to fact_spending table';
        END IF;
    ELSE
        -- Create fact_spending table with all required columns
        CREATE TABLE dw.fact_spending (
            fact_id SERIAL PRIMARY KEY,
            date_id INTEGER NOT NULL,
            industry_id INTEGER NOT NULL,
            region_id INTEGER NOT NULL,
            transaction_count INTEGER NOT NULL,
            total_amount NUMERIC(20,2) NOT NULL,
            avg_transaction NUMERIC(12,2),
            source_system VARCHAR(50) DEFAULT 'mastercard',
            batch_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fact_spending_date_fk FOREIGN KEY (date_id) REFERENCES dw.dim_date(date_id),
            CONSTRAINT fact_spending_industry_fk FOREIGN KEY (industry_id) REFERENCES dw.dim_industry(industry_id),
            CONSTRAINT fact_spending_region_fk FOREIGN KEY (region_id) REFERENCES dw.dim_region(region_id),
            CONSTRAINT fact_spending_unique UNIQUE (date_id, industry_id, region_id)
        );
        
        -- Create appropriate indexes for fact table
        CREATE INDEX idx_fact_spending_date ON dw.fact_spending(date_id);
        CREATE INDEX idx_fact_spending_industry ON dw.fact_spending(industry_id);
        CREATE INDEX idx_fact_spending_region ON dw.fact_spending(region_id);
        CREATE INDEX idx_fact_spending_source ON dw.fact_spending(source_system);
        CREATE INDEX idx_fact_spending_batch ON dw.fact_spending(batch_id);
        
        RAISE NOTICE 'Created fact_spending table with indexes';
    END IF;
END $$;

-- Fix encoding issues in geo_name values before loading data
-- This script corrects problematic characters in Swiss region names
DO $$
DECLARE
    v_count INTEGER := 0;
BEGIN
    -- 1. First, let's create a temporary lookup table for the problematic geo names
    CREATE TEMPORARY TABLE temp_geo_name_fixes (
        incorrect_name VARCHAR(100),
        correct_name VARCHAR(100)
    );
    
    -- 2. Insert known problematic geo names
    INSERT INTO temp_geo_name_fixes VALUES
        ('ZÃÂ¼rich', 'Zürich'),
        ('BÃÂ¼lach', 'Bülach'),
        ('DelÃÂ©mont', 'Delémont'),
        ('GenÃÂ¨ve', 'Genève'),
        ('GraubÃÂ¼nden', 'Graubünden'),
        ('GÃÂ¤u', 'Gäu'),
        ('GÃÂ¶sgen', 'Gösgen'),
        ('HÃÂ©rens', 'Hérens'),
        ('HÃÂ¶fe', 'Höfe'),
        ('KÃÂ¼ssnacht (SZ)', 'Küssnacht (SZ)'),
        ('La GlÃÂ¢ne', 'La Glâne'),
        ('La GruyÃÂ¨re', 'La Gruyère'),
        ('MÃÂ¼nchwilen', 'Münchwilen'),
        ('NeuchÃÂ¢tel', 'Neuchâtel'),
        ('PfÃÂ¤ffikon', 'Pfäffikon'),
        ('PrÃÂ¤ttigau-Davos', 'Prättigau-Davos'),
        ('ZÃÂ¼rich', 'Zürich');
    
    RAISE NOTICE 'Created temporary geo name fixes for % names', (SELECT COUNT(*) FROM temp_geo_name_fixes);
    
    -- 3. Fix the geo_names in dim_geography
    UPDATE dw.dim_geography dg
    SET 
        geo_name = f.correct_name,
        country = CASE WHEN dg.geo_type = 'Country' THEN f.correct_name ELSE dg.country END,
        state = CASE WHEN dg.geo_type = 'State' THEN f.correct_name ELSE dg.state END,
        city = CASE WHEN dg.geo_type = 'Msa' THEN f.correct_name ELSE dg.city END
    FROM temp_geo_name_fixes f
    WHERE dg.geo_name = f.incorrect_name;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Updated % rows in dim_geography', v_count;
    
    -- 4. Update the region mappings
    UPDATE dw.temp_geography_region_map m
    SET geo_name = f.correct_name
    FROM temp_geo_name_fixes f
    WHERE m.geo_name = f.incorrect_name;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Updated % rows in temp_geography_region_map', v_count;
    
    -- 5. Update the dim_region table
    UPDATE dw.dim_region r
    SET region_name = f.correct_name
    FROM temp_geo_name_fixes f
    WHERE r.region_name = f.incorrect_name;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Updated % rows in dim_region', v_count;
    
    -- 6. Check if there's a unique constraint that needs to be added to fact_spending
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_constraint 
        WHERE conname = 'fact_spending_unique' 
        AND conrelid = 'dw.fact_spending'::regclass
    ) THEN
        -- Add the missing unique constraint
        ALTER TABLE dw.fact_spending 
        ADD CONSTRAINT fact_spending_unique 
        UNIQUE (date_id, industry_id, region_id);
        
        RAISE NOTICE 'Added missing unique constraint to fact_spending table';
    ELSE
        RAISE NOTICE 'Unique constraint already exists on fact_spending table';
    END IF;
    
    -- 7. Test if the current transaction is read-only
    BEGIN
        -- Try a small insert that will be rolled back
        CREATE TEMPORARY TABLE test_write_access (id int);
        DROP TABLE test_write_access;
        
        RAISE NOTICE 'Transaction is NOT read-only - write operations are allowed';
    EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%read-only transaction%' THEN
            RAISE EXCEPTION 'Database is in READ-ONLY mode. Cannot perform write operations.';
        ELSE
            RAISE EXCEPTION 'Error testing write access: %', SQLERRM;
        END IF;
    END;
    
    RAISE NOTICE 'Geo name encoding fixes completed successfully';
END $$;

-- Step 2: Load MasterCard data into dimensions and facts
DO $$
DECLARE
    v_start_date DATE := '2022-01-01';  -- Updated to include 2022
    v_end_date DATE := '2023-12-31';
    v_start_date_id INTEGER;
    v_end_date_id INTEGER;
    v_industry_count INTEGER := 0;
    v_geography_count INTEGER := 0;
    v_fact_count INTEGER := 0;
    v_batch_id INTEGER;
    v_source_count INTEGER := 0;
    v_existing_fact_count INTEGER := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_min_date DATE;
    v_max_date DATE;
    v_chunk_size INTEGER := 10000;  -- Process in chunks for better performance
    v_current_date DATE;
    v_chunk_end_date DATE;
    v_chunk_count INTEGER;
    v_total_loaded INTEGER := 0;
    v_error_message TEXT;
    v_missing_dates INTEGER;
BEGIN
    -- Set start time for performance tracking
    v_start_time := clock_timestamp();

    -- Verify that all required dates exist in the dim_date table
    SELECT COUNT(*) INTO v_missing_dates
    FROM (
        SELECT TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_id
        FROM generate_series(v_start_date, v_end_date, '1 day'::interval) d
    ) all_dates
    LEFT JOIN dw.dim_date dd ON all_dates.date_id = dd.date_id
    WHERE dd.date_id IS NULL;
    
    IF v_missing_dates > 0 THEN
        RAISE EXCEPTION 'Missing % dates in dim_date table for period % to %. Please run the populate dim_date script first.', 
            v_missing_dates, v_start_date, v_end_date;
    END IF;

    -- First verify source data exists and get date range
    SELECT MIN(txn_date), MAX(txn_date) 
    INTO v_min_date, v_max_date
    FROM data_lake.master_card;
    
    IF v_min_date IS NULL THEN
        RAISE EXCEPTION 'No data found in source table data_lake.master_card';
    END IF;
    
    -- Adjust date range based on actual data availability
    IF v_start_date < v_min_date THEN
        v_start_date := v_min_date;
        RAISE NOTICE 'Adjusted start date to match earliest available data: %', v_start_date;
    END IF;
    
    IF v_end_date > v_max_date THEN
        v_end_date := v_max_date;
        RAISE NOTICE 'Adjusted end date to match latest available data: %', v_end_date;
    END IF;

    -- Calculate date IDs
    v_start_date_id := TO_CHAR(v_start_date, 'YYYYMMDD')::INTEGER;
    v_end_date_id := TO_CHAR(v_end_date, 'YYYYMMDD')::INTEGER;
    
    -- Create ETL batch record
    INSERT INTO dw.etl_metadata (
        process_name,
        source_system,
        status_code,
        start_time,
        records_processed,
        records_successful,
        status_message
    ) VALUES (
        'load_mastercard_spending',
        'mastercard',
        'RUNNING',
        v_start_time,
        0,
        0,
        'Started loading MasterCard spending data for period ' || v_start_date || ' to ' || v_end_date
    ) RETURNING etl_id INTO v_batch_id;
    
    RAISE NOTICE 'Starting MasterCard data load for period % to % (date_id: % to %). Batch ID: %', 
                v_start_date, v_end_date, v_start_date_id, v_end_date_id, v_batch_id;
    
    -- Check for existing data to avoid duplicates
    SELECT COUNT(*) INTO v_existing_fact_count
    FROM dw.fact_spending
    WHERE date_id BETWEEN v_start_date_id AND v_end_date_id
    AND source_system = 'mastercard';
    
    -- Check if there's source data for the period
    SELECT COUNT(*) INTO v_source_count
    FROM data_lake.master_card
    WHERE txn_date BETWEEN v_start_date AND v_end_date;
    
    RAISE NOTICE 'Found % records in source and % existing records in fact table for the specified period', 
        v_source_count, v_existing_fact_count;
    
    IF v_source_count = 0 THEN
        -- Update ETL metadata
        UPDATE dw.etl_metadata SET 
            status_code = 'COMPLETED',
            end_time = clock_timestamp(),
            status_message = 'No data found in source for this period. ETL process completed.'
        WHERE etl_id = v_batch_id;
        
        RAISE NOTICE 'No data found in source for this period. ETL process completed.';
        RETURN;
    END IF;
    
    -- Option to clean existing data for a refresh
    IF v_existing_fact_count > 0 THEN
        RAISE NOTICE 'Cleaning % existing records for the period', v_existing_fact_count;
        DELETE FROM dw.fact_spending 
        WHERE date_id BETWEEN v_start_date_id AND v_end_date_id
        AND source_system = 'mastercard';
    END IF;
    
    -- 1. Load industry dimension
    RAISE NOTICE 'Loading industry dimension...';
    
    BEGIN
        INSERT INTO dw.dim_industry (
            industry_code,
            industry_name,
            industry_category,
            is_tourism_related,
            is_active
        )
        SELECT DISTINCT
            'MC_' || SUBSTRING(REGEXP_REPLACE(mc.industry, '[^a-zA-Z0-9]', '', 'g'), 1, 6),
            mc.industry,
            CASE 
                WHEN mc.industry IN ('Accommodations', 'Eating Places', 'Bars/Taverns/Nightclubs', 
                                    'Art, Entertainment and Recreation', 'Museums, Historical Sites and similar', 
                                    'Travel') THEN 'Tourism'
                WHEN mc.industry IN ('Grocery and Food Stores', 'Home Furnishings/Furniture', 
                                    'Department and General Merchandise') THEN 'Retail'
                ELSE 'Other'
            END,
            CASE 
                WHEN mc.industry IN ('Accommodations', 'Eating Places', 'Bars/Taverns/Nightclubs', 
                                    'Art, Entertainment and Recreation', 'Museums, Historical Sites and similar', 
                                    'Travel') THEN TRUE
                ELSE FALSE
            END,
            TRUE
        FROM data_lake.master_card mc
        WHERE mc.txn_date BETWEEN v_start_date AND v_end_date
        AND NOT EXISTS (
            SELECT 1 FROM dw.dim_industry di
            WHERE di.industry_name = mc.industry
        );
        
        GET DIAGNOSTICS v_industry_count = ROW_COUNT;
        RAISE NOTICE 'Added % new industries', v_industry_count;
    EXCEPTION WHEN OTHERS THEN
        v_error_message := 'Error loading industry dimension: ' || SQLERRM;
        RAISE NOTICE '%', v_error_message;
        RAISE;
    END;
    
    -- 2. Load geography dimension
    RAISE NOTICE 'Loading geography dimension...';
    
    BEGIN
        -- Use ON CONFLICT DO NOTHING to handle existing records gracefully
        INSERT INTO dw.dim_geography (
            geo_name,
            geo_type,
            country,
            state,
            city,
            latitude,
            longitude
        )
        SELECT DISTINCT
            TRIM(mc.geo_name),  -- Add TRIM to handle leading/trailing whitespace
            mc.geo_type,
            CASE WHEN mc.geo_type = 'Country' THEN TRIM(mc.geo_name) ELSE NULL END,
            CASE WHEN mc.geo_type = 'State' THEN TRIM(mc.geo_name) ELSE NULL END,
            CASE WHEN mc.geo_type = 'Msa' THEN TRIM(mc.geo_name) ELSE NULL END,
            mc.central_latitude,
            mc.central_longitude
        FROM data_lake.master_card mc
        WHERE mc.txn_date BETWEEN v_start_date AND v_end_date
        ON CONFLICT (geo_name, geo_type) DO NOTHING;
        
        GET DIAGNOSTICS v_geography_count = ROW_COUNT;
        RAISE NOTICE 'Added % new geographies', v_geography_count;
    EXCEPTION WHEN OTHERS THEN
        v_error_message := 'Error loading geography dimension: ' || SQLERRM;
        RAISE NOTICE '%', v_error_message;
        RAISE;
    END;
    
    -- Insert geography data into region tables using accepted region types
    RAISE NOTICE 'Creating region mappings from geography data...';
    
    BEGIN
        -- Map geography types to allowed region types
        -- Canton for State, Tourism_region for Msa, District for Country
        INSERT INTO dw.dim_region (
            region_name,
            region_type
        )
        SELECT DISTINCT
            TRIM(dg.geo_name),  -- Add TRIM to handle whitespace issues
            CASE 
                WHEN dg.geo_type = 'State' THEN 'canton'
                WHEN dg.geo_type = 'Msa' THEN 'tourism_region'
                WHEN dg.geo_type = 'Country' THEN 'district'
                ELSE 'tourism_region' -- default fallback
            END
        FROM dw.dim_geography dg
        LEFT JOIN dw.dim_region dr ON 
            dr.region_name = TRIM(dg.geo_name)  -- Ensure consistent trimming 
        WHERE dr.region_id IS NULL
        ON CONFLICT (region_name, region_type) DO NOTHING;
        
        -- Create mappings between geography and region with improved matching
        TRUNCATE dw.temp_geography_region_map;  -- Clear previous mappings
        
        INSERT INTO dw.temp_geography_region_map (
            geography_id,
            region_id,
            geo_name,
            geo_type
        )
        SELECT 
            dg.geography_id,
            dr.region_id,
            dg.geo_name,
            dg.geo_type
        FROM dw.dim_geography dg
        JOIN dw.dim_region dr ON 
            TRIM(dr.region_name) = TRIM(dg.geo_name)  -- Ensure consistent trimming
        ON CONFLICT (geography_id, region_id) DO NOTHING;
        
        RAISE NOTICE 'Created region mappings for % geography records', 
            (SELECT COUNT(*) FROM dw.temp_geography_region_map);
    EXCEPTION WHEN OTHERS THEN
        v_error_message := 'Error creating region mappings: ' || SQLERRM;
        RAISE NOTICE '%', v_error_message;
        RAISE;
    END;
    
    -- 3. Load fact data in chunks (by month to improve performance)
    RAISE NOTICE 'Loading fact spending data in chunks...';
    
    v_current_date := v_start_date;
    
    WHILE v_current_date <= v_end_date LOOP
        -- Process by month for better performance
        v_chunk_end_date := (DATE_TRUNC('MONTH', v_current_date) + INTERVAL '1 MONTH')::DATE - INTERVAL '1 DAY';
        
        -- Don't go beyond the overall end date
        IF v_chunk_end_date > v_end_date THEN
            v_chunk_end_date := v_end_date;
        END IF;
        
        RAISE NOTICE 'Processing chunk from % to %', v_current_date, v_chunk_end_date;
        
        BEGIN
            WITH inserted_rows AS (
                INSERT INTO dw.fact_spending (
                    date_id,
                    industry_id,
                    region_id,
                    transaction_count,
                    total_amount,
                    avg_transaction,
                    source_system,
                    batch_id
                )
                SELECT 
                    TO_CHAR(mc.txn_date, 'YYYYMMDD')::INTEGER,
                    di.industry_id,
                    m.region_id,
                    SUM(mc.txn_cnt),
                    SUM(mc.txn_amt),
                    CASE WHEN SUM(mc.txn_cnt) > 0 THEN SUM(mc.txn_amt) / SUM(mc.txn_cnt) ELSE 0 END,
                    'mastercard',
                    v_batch_id
                FROM data_lake.master_card mc
                JOIN dw.dim_industry di ON di.industry_name = mc.industry
                JOIN dw.dim_geography dg ON TRIM(dg.geo_name) = TRIM(mc.geo_name) AND dg.geo_type = mc.geo_type
                JOIN dw.temp_geography_region_map m ON m.geography_id = dg.geography_id
                WHERE mc.txn_date BETWEEN v_current_date AND v_chunk_end_date
                GROUP BY 
                    TO_CHAR(mc.txn_date, 'YYYYMMDD')::INTEGER,
                    di.industry_id,
                    m.region_id
                ON CONFLICT (date_id, industry_id, region_id) DO UPDATE
                SET 
                    transaction_count = EXCLUDED.transaction_count,
                    total_amount = EXCLUDED.total_amount,
                    avg_transaction = EXCLUDED.avg_transaction,
                    batch_id = EXCLUDED.batch_id
                RETURNING 1
            )
            SELECT COUNT(*) INTO v_chunk_count FROM inserted_rows;
            
            v_total_loaded := v_total_loaded + v_chunk_count;
            RAISE NOTICE 'Loaded % fact records for period % to %', v_chunk_count, v_current_date, v_chunk_end_date;
            
            -- Move to next month
            v_current_date := (DATE_TRUNC('MONTH', v_current_date) + INTERVAL '1 MONTH')::DATE;
        EXCEPTION WHEN OTHERS THEN
            v_error_message := 'Error loading fact data for period ' || v_current_date || ' to ' || v_chunk_end_date || ': ' || SQLERRM;
            RAISE NOTICE '%', v_error_message;
            
            -- Continue with next chunk instead of failing completely
            v_current_date := (DATE_TRUNC('MONTH', v_current_date) + INTERVAL '1 MONTH')::DATE;
        END;
    END LOOP;
    
    v_fact_count := v_total_loaded;
    RAISE NOTICE 'Loaded % total fact spending records', v_fact_count;
    
    -- Calculate execution time
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    
    -- Update ETL metadata with success
    UPDATE dw.etl_metadata SET 
        status_code = 'COMPLETED',
        end_time = v_end_time,
        records_processed = v_source_count,
        records_successful = v_fact_count,
        status_message = 'Successfully loaded MasterCard spending data. Duration: ' || 
                         EXTRACT(EPOCH FROM v_duration) || ' seconds. Years loaded: 2022-2023.'
    WHERE etl_id = v_batch_id;
    
    -- Create useful aggregation views if they don't exist
    -- Fix the view definition to use calculated avg rather than column
    IF EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema = 'dw' AND table_name = 'vw_spending_by_industry_month') THEN
        DROP VIEW dw.vw_spending_by_industry_month;
    END IF;
    
    CREATE OR REPLACE VIEW dw.vw_spending_by_industry_month AS
    SELECT 
        dd.year, 
        dd.month, 
        di.industry_name,
        di.industry_category,
        di.is_tourism_related,
        SUM(fs.transaction_count) as total_transactions,
        SUM(fs.total_amount) as total_spending,
        CASE 
            WHEN SUM(fs.transaction_count) > 0 
            THEN ROUND(SUM(fs.total_amount) / SUM(fs.transaction_count), 2) 
            ELSE 0 
        END as avg_transaction
    FROM dw.fact_spending fs
    JOIN dw.dim_date dd ON fs.date_id = dd.date_id
    JOIN dw.dim_industry di ON fs.industry_id = di.industry_id
    WHERE fs.source_system = 'mastercard'
    GROUP BY dd.year, dd.month, di.industry_name, di.industry_category, di.is_tourism_related
    ORDER BY dd.year, dd.month, total_spending DESC;
    
    RAISE NOTICE 'Created view: vw_spending_by_industry_month';
    
    IF EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema = 'dw' AND table_name = 'vw_spending_by_region') THEN
        DROP VIEW dw.vw_spending_by_region;
    END IF;
    
    CREATE OR REPLACE VIEW dw.vw_spending_by_region AS
    SELECT 
        dd.year, 
        dd.month, 
        dr.region_type,
        dr.region_name,
        SUM(fs.transaction_count) as total_transactions,
        SUM(fs.total_amount) as total_spending
    FROM dw.fact_spending fs
    JOIN dw.dim_date dd ON fs.date_id = dd.date_id
    JOIN dw.dim_region dr ON fs.region_id = dr.region_id
    WHERE fs.source_system = 'mastercard'
    GROUP BY dd.year, dd.month, dr.region_type, dr.region_name
    ORDER BY dd.year, dd.month, total_spending DESC;
    
    RAISE NOTICE 'Created view: vw_spending_by_region';
    
    -- Summary
    RAISE NOTICE 'Data load complete. Summary:';
    RAISE NOTICE '  - Period: % to %', v_start_date, v_end_date;
    RAISE NOTICE '  - New industries: %', v_industry_count;
    RAISE NOTICE '  - New geographies: %', v_geography_count;
    RAISE NOTICE '  - Fact records: %', v_fact_count;
    RAISE NOTICE '  - Execution time: % seconds', EXTRACT(EPOCH FROM v_duration);
    
EXCEPTION WHEN OTHERS THEN
    -- Update ETL metadata with error
    UPDATE dw.etl_metadata SET 
        status_code = 'FAILED',
        end_time = clock_timestamp(),
        status_message = 'Error in ETL process: ' || SQLERRM
    WHERE etl_id = v_batch_id;
    
    RAISE NOTICE 'Error in ETL process: %', SQLERRM;
    RAISE;
END $$;

-- Verify data was loaded correctly - summary by year and month
SELECT 
    dd.year, 
    dd.month, 
    COUNT(*) as record_count
FROM dw.fact_spending fs
JOIN dw.dim_date dd ON fs.date_id = dd.date_id
WHERE fs.source_system = 'mastercard'
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;

-- Sample of spending by industry (limited for demonstration)
SELECT 
    dd.year, 
    dd.month, 
    di.industry_name,
    di.industry_category,
    SUM(fs.transaction_count) as total_transactions,
    SUM(fs.total_amount) as total_spending,
    CASE 
        WHEN SUM(fs.transaction_count) > 0 
        THEN ROUND(SUM(fs.total_amount) / SUM(fs.transaction_count), 2) 
        ELSE 0 
    END as avg_transaction
FROM dw.fact_spending fs
JOIN dw.dim_date dd ON fs.date_id = dd.date_id
JOIN dw.dim_industry di ON fs.industry_id = di.industry_id
WHERE fs.source_system = 'mastercard'
AND dd.year = 2023
GROUP BY dd.year, dd.month, di.industry_name, di.industry_category
ORDER BY dd.year, dd.month, total_spending DESC
LIMIT 20;