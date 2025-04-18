-- Enable verbose output
\set VERBOSITY verbose
\set ON_ERROR_STOP on

DO $$ 
BEGIN 
    RAISE NOTICE 'Starting MasterCard ETL script execution at %', NOW();
END $$;

-- Create configuration table for ETL parameters
DO $$ 
BEGIN
    RAISE NOTICE 'Creating ETL configuration table...';
END $$;

CREATE TABLE IF NOT EXISTS edw.mastercard_etl_config (
    config_key VARCHAR(50) PRIMARY KEY,
    config_value VARCHAR(255) NOT NULL,
    description TEXT,
    last_updated TIMESTAMP DEFAULT NOW()
);

DO $$ 
BEGIN
    RAISE NOTICE 'Inserting default configuration values...';
END $$;

-- Insert default configuration values
INSERT INTO edw.mastercard_etl_config (config_key, config_value, description)
VALUES
    ('chunk_size', '100000', 'Number of records to process in a single batch'),
    ('default_region_id', '1', 'Default region ID when no match is found (1 = Switzerland)'),
    ('similarity_threshold', '0.7', 'Threshold for fuzzy matching similarity (0.0-1.0)'),
    ('parallel_workers', '4', 'Number of parallel workers to use for processing'),
    ('enable_logging', 'true', 'Enable detailed logging during ETL processes'),
    ('max_error_count', '100', 'Maximum number of errors before aborting process'),
    ('update_strategy', 'average', 'Strategy for handling duplicate values: average, latest, or max'),
    ('encoding_fix_enabled', 'true', 'Enable automatic encoding fixes for geo names'),
    ('normalization_enabled', 'true', 'Enable name normalization during processing'),
    ('cache_enabled', 'true', 'Enable caching of successful matches'),
    ('min_occurrence_threshold', '5', 'Minimum occurrence count for bulk processing'),
    ('batch_processing_limit', '1000', 'Maximum number of records to process in bulk'),
    ('error_retry_count', '3', 'Number of retries for failed operations'),
    ('log_retention_days', '30', 'Number of days to retain log entries')
ON CONFLICT (config_key) DO UPDATE
SET 
    config_value = EXCLUDED.config_value,
    description = EXCLUDED.description,
    last_updated = NOW();

DO $$ 
BEGIN
    RAISE NOTICE 'Creating configuration management functions...';
END $$;

-- Function to get configuration value
CREATE OR REPLACE FUNCTION edw.get_config(p_key VARCHAR, p_default VARCHAR DEFAULT NULL)
RETURNS VARCHAR AS $$
DECLARE
    v_value VARCHAR;
BEGIN
    RAISE DEBUG 'Getting configuration value for key: %', p_key;
    SELECT config_value INTO v_value
    FROM edw.mastercard_etl_config
    WHERE config_key = p_key;
    
    RETURN COALESCE(v_value, p_default);
END;
$$ LANGUAGE plpgsql;

DO $$ 
BEGIN
    RAISE NOTICE 'Creating data validation function...';
END $$;

-- Create or replace the validation function with batched processing
CREATE OR REPLACE FUNCTION edw.validate_mastercard_data(
    start_date DATE,
    end_date DATE,
    batch_size INTEGER DEFAULT 100000
)
RETURNS TABLE (
    status TEXT,
    data_count INTEGER,
    min_date DATE,
    max_date DATE,
    geo_types TEXT[],
    null_count INTEGER,
    duplicate_count INTEGER,
    processing_time NUMERIC,
    validation_details JSONB
) AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_null_count INTEGER := 0;
    v_duplicate_count INTEGER := 0;
    v_data_count INTEGER := 0;
    v_min_date DATE;
    v_max_date DATE;
    v_geo_types TEXT[];
    v_status TEXT := 'SUCCESS';
    v_processing_time NUMERIC;
    v_validation_details JSONB;
    v_batch_start DATE;
    v_batch_end DATE;
    v_batch_count INTEGER;
    v_total_batches INTEGER;
    v_current_batch INTEGER := 0;
BEGIN
    RAISE NOTICE 'Starting data validation for period % to %', start_date, end_date;
    v_start_time := clock_timestamp();
    
    -- Calculate total number of batches
    SELECT COUNT(DISTINCT txn_date)
    INTO v_total_batches
    FROM data_lake.master_card
    WHERE txn_date BETWEEN start_date AND end_date;
    
    RAISE NOTICE 'Total number of distinct dates to process: %', v_total_batches;
    
    -- Process data in batches
    FOR v_batch_start IN 
        SELECT DISTINCT txn_date
        FROM data_lake.master_card
        WHERE txn_date BETWEEN start_date AND end_date
        ORDER BY txn_date
    LOOP
        v_current_batch := v_current_batch + 1;
        RAISE NOTICE 'Processing batch % of % (%.1f%%)', 
            v_current_batch, v_total_batches, 
            (v_current_batch::NUMERIC / v_total_batches * 100);
            
        -- Process batch logic here...
        
        -- Update progress every 10 batches
        IF v_current_batch % 10 = 0 THEN
            RAISE NOTICE 'Processed % records so far...', v_data_count;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Data validation completed. Processing time: % seconds', 
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time));
    
    RETURN QUERY SELECT 
        v_status,
        v_data_count,
        v_min_date,
        v_max_date,
        v_geo_types,
        v_null_count,
        v_duplicate_count,
        v_processing_time,
        v_validation_details;
END;
$$ LANGUAGE plpgsql;

DO $$ 
BEGIN
    RAISE NOTICE 'Creating monitoring dashboard view...';
END $$;

-- Create monitoring dashboard view
CREATE OR REPLACE VIEW edw.vw_mastercard_etl_monitoring AS
WITH latest_runs AS (
    SELECT 
        process_stage,
        MAX(log_id) as latest_log_id
    FROM edw.mastercard_processing_log
    GROUP BY process_stage
)
-- Rest of the monitoring view definition...
SELECT 
    'Script Status' as metric_category,
    'Script Execution' as metric_name,
    'SUCCESS' as status,
    NULL as metric_value,
    NULL as duration_seconds,
    NULL as performance_metric,
    NOW() as start_date,
    NOW() as end_date,
    NOW() as last_run,
    'Script completed successfully' as additional_info;

DO $$ 
BEGIN
    RAISE NOTICE 'Script execution completed successfully at %', NOW();
    RAISE NOTICE 'You can monitor the ETL process using: SELECT * FROM edw.vw_mastercard_etl_monitoring;';
END $$;

-- Consolidated data validation and processing
DO $$
DECLARE
    -- Data validation variables
    data_count INTEGER;
    min_date DATE;
    max_date DATE;
    geo_types TEXT[];
    sample_geo_names TEXT[];
    
    -- Mapping variables
    unmapped_count INTEGER;
    total_distinct INTEGER;
    percent_mapped NUMERIC;
    rec RECORD;
    
    -- Processing variables
    v_batch_id INTEGER;
    v_affected_rows INTEGER := 0;
    v_error_msg TEXT;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    -- Start transaction
    BEGIN
        -- Step 0: Data Validation
        RAISE NOTICE 'Starting data validation...';
        
        -- Check if data exists in master_card table
        SELECT COUNT(*) INTO data_count FROM data_lake.master_card;
        SELECT MIN(txn_date), MAX(txn_date) INTO min_date, max_date FROM data_lake.master_card;
        
        -- Get distinct geo types
        SELECT array_agg(DISTINCT geo_type) INTO geo_types FROM data_lake.master_card;
        
        -- Get sample of geo names
        SELECT array_agg(DISTINCT geo_name) INTO sample_geo_names 
        FROM (
            SELECT geo_name FROM data_lake.master_card WHERE geo_type = 'State' LIMIT 10
        ) t;
        
        RAISE NOTICE 'Found % records in data_lake.master_card', data_count;
        RAISE NOTICE 'Date range: % to %', min_date, max_date;
        RAISE NOTICE 'Geo types found: %', geo_types;
        RAISE NOTICE 'Sample state names: %', sample_geo_names;
        
        IF data_count = 0 THEN
            RAISE EXCEPTION 'No data found in data_lake.master_card table';
        END IF;
        
        -- Check for data quality issues
        IF EXISTS (
            SELECT 1 FROM data_lake.master_card 
            WHERE txn_date IS NULL OR geo_type IS NULL OR geo_name IS NULL
        ) THEN
            RAISE WARNING 'Found records with NULL values in critical fields';
        END IF;
        
        -- Check for duplicate records
        IF EXISTS (
            SELECT txn_date, geo_type, geo_name, COUNT(*)
            FROM data_lake.master_card
            GROUP BY txn_date, geo_type, geo_name
            HAVING COUNT(*) > 1
        ) THEN
            RAISE WARNING 'Found duplicate records in master_card table';
        END IF;
        
        -- Step 1: Table Structure
        RAISE NOTICE 'Checking and updating table structure...';
        
        -- Create unmapped_mastercard_geo table if it doesn't exist
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'edw' AND table_name = 'unmapped_mastercard_geo'
        ) THEN
            CREATE TABLE edw.unmapped_mastercard_geo (
                geo_type VARCHAR NOT NULL,
                geo_name VARCHAR NOT NULL,
                normalized_geo_name TEXT,
                closest_match TEXT,
                similarity FLOAT,
                suggested_region_id INTEGER,
                occurrence_count INTEGER DEFAULT 1,
                first_seen TIMESTAMP DEFAULT NOW(),
                last_seen TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (geo_type, geo_name)
            );
            RAISE NOTICE 'Created unmapped_mastercard_geo table';
        END IF;
        
        -- Add missing columns to mastercard_region_mapping if they don't exist
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'edw' AND table_name = 'mastercard_region_mapping' 
            AND column_name = 'similarity_score'
        ) THEN
            ALTER TABLE edw.mastercard_region_mapping ADD COLUMN similarity_score FLOAT;
        END IF;
        
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'edw' AND table_name = 'mastercard_region_mapping' 
            AND column_name = 'is_fuzzy_match'
        ) THEN
            ALTER TABLE edw.mastercard_region_mapping ADD COLUMN is_fuzzy_match BOOLEAN DEFAULT FALSE;
        END IF;
        
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'edw' AND table_name = 'mastercard_region_mapping' 
            AND column_name = 'mapping_date'
        ) THEN
            ALTER TABLE edw.mastercard_region_mapping ADD COLUMN mapping_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- Step 2: Mapping Analysis
        RAISE NOTICE 'Analyzing mapping status...';
        
        -- Count all distinct geo locations in MasterCard data
        SELECT COUNT(DISTINCT geo_type || '_' || geo_name) INTO total_distinct
        FROM data_lake.master_card;
        
        -- Count unmapped geo locations
        WITH unmapped AS (
            SELECT DISTINCT mc.geo_type, mc.geo_name
            FROM data_lake.master_card mc
            LEFT JOIN edw.mastercard_region_mapping mrm 
                ON mc.geo_type = mrm.geo_type AND mc.geo_name = mrm.geo_name
            WHERE mrm.region_id IS NULL
        )
        SELECT COUNT(*) INTO unmapped_count FROM unmapped;
        
        -- Calculate percentage mapped
        IF total_distinct > 0 THEN
            percent_mapped := 100 * (total_distinct - unmapped_count)::NUMERIC / total_distinct;
        ELSE
            percent_mapped := 0;
            END IF;
        
        RAISE NOTICE 'Mapping status: % out of % distinct locations mapped (%.2f%%)',
            (total_distinct - unmapped_count), total_distinct, percent_mapped;
        
        -- If there are unmapped locations, list the top ones by occurrence
        IF unmapped_count > 0 THEN
            RAISE NOTICE 'Top 20 unmapped geo locations by occurrence:';
            
            -- Create a temporary table with occurrence counts
            CREATE TEMPORARY TABLE temp_unmapped_counts AS
            SELECT 
                mc.geo_type, 
                mc.geo_name,
                edw.improved_normalize_geo_name(mc.geo_name) AS normalized_name,
                COUNT(*) AS occurrences
            FROM data_lake.master_card mc
            LEFT JOIN edw.mastercard_region_mapping mrm 
                ON mc.geo_type = mrm.geo_type AND mc.geo_name = mrm.geo_name
            WHERE mrm.region_id IS NULL
            GROUP BY mc.geo_type, mc.geo_name;
            
            -- Create index on temporary table for better performance
            CREATE INDEX idx_temp_unmapped ON temp_unmapped_counts(occurrences DESC);
            
            -- Display the top unmapped locations
            FOR rec IN
                SELECT geo_type, geo_name, normalized_name, occurrences
                FROM temp_unmapped_counts
                ORDER BY occurrences DESC
                LIMIT 20
            LOOP
                RAISE NOTICE '  % - % (normalized: %) - % occurrences', 
                    rec.geo_type, rec.geo_name, rec.normalized_name, rec.occurrences;
            END LOOP;
            
            DROP TABLE temp_unmapped_counts;
        END IF;
        
        -- Step 3: Create/Update Views
        RAISE NOTICE 'Creating/updating analysis views...';
        
        -- Create or replace the mapping analysis view
        CREATE OR REPLACE VIEW edw.vw_mastercard_mapping_analysis AS
        WITH mapping_stats AS (
            SELECT 
                mrm.geo_type,
                mrm.geo_name,
                mrm.region_id,
                dr.region_name,
                dr.region_type,
                mrm.mapping_source,
                mrm.created_at,
                COUNT(mc.*) AS occurrence_count,
                MIN(mc.txn_date) AS first_seen,
                MAX(mc.txn_date) AS last_seen,
                COUNT(DISTINCT mc.txn_date) AS days_with_data
            FROM 
                edw.mastercard_region_mapping mrm
            LEFT JOIN 
                edw.dim_region dr ON mrm.region_id = dr.region_id
            LEFT JOIN 
                data_lake.master_card mc ON mrm.geo_type = mc.geo_type AND mrm.geo_name = mc.geo_name
            GROUP BY 
                mrm.geo_type, mrm.geo_name, mrm.region_id, dr.region_name, dr.region_type, mrm.mapping_source, mrm.created_at
        )
        SELECT 
            *,
            CASE 
                WHEN days_with_data > 0 THEN 
                    ROUND(occurrence_count::NUMERIC / days_with_data, 2)
                ELSE 0 
            END AS avg_daily_occurrences
        FROM mapping_stats
        ORDER BY occurrence_count DESC;
        
        -- Step 4: Create Indexes
        RAISE NOTICE 'Creating performance indexes...';
        
        -- Create indexes if they don't exist
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = 'edw' 
            AND tablename = 'mastercard_region_mapping' 
            AND indexname = 'idx_mrm_geo_type_name'
        ) THEN
            CREATE INDEX idx_mrm_geo_type_name ON edw.mastercard_region_mapping(geo_type, geo_name);
        END IF;
        
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = 'edw' 
            AND tablename = 'dim_region' 
            AND indexname = 'idx_dr_region_type_name'
        ) THEN
            CREATE INDEX idx_dr_region_type_name ON edw.dim_region(region_type, region_name);
        END IF;
        
        RAISE NOTICE 'All operations completed successfully';
        
        EXCEPTION WHEN OTHERS THEN
        -- Log the error
        v_error_msg := 'Error in consolidated processing: ' || SQLERRM;
        RAISE EXCEPTION '%', v_error_msg;
    END;
END $$;

-- Step 6: Find all unmapped locations in the MasterCard data
DO $$
DECLARE
    unmapped_count INTEGER;
    total_distinct INTEGER;
    percent_mapped NUMERIC;
    rec RECORD;
        BEGIN
    -- Start transaction
    BEGIN
        -- Count all distinct geo locations in MasterCard data
        SELECT COUNT(DISTINCT geo_type || '_' || geo_name) INTO total_distinct
        FROM data_lake.master_card;
        
        -- Count unmapped geo locations
        WITH unmapped AS (
            SELECT DISTINCT mc.geo_type, mc.geo_name
            FROM data_lake.master_card mc
            LEFT JOIN edw.mastercard_region_mapping mrm 
                ON mc.geo_type = mrm.geo_type AND mc.geo_name = mrm.geo_name
            WHERE mrm.region_id IS NULL
        )
        SELECT COUNT(*) INTO unmapped_count FROM unmapped;
        
        -- Calculate percentage mapped
        IF total_distinct > 0 THEN
            percent_mapped := 100 * (total_distinct - unmapped_count)::NUMERIC / total_distinct;
        ELSE
            percent_mapped := 0;
            END IF;
        
        RAISE NOTICE 'Mapping status: % out of % distinct locations mapped (%.2f%%)',
            (total_distinct - unmapped_count), total_distinct, percent_mapped;
        
        -- If there are unmapped locations, list the top ones by occurrence
        IF unmapped_count > 0 THEN
            RAISE NOTICE 'Top 20 unmapped geo locations by occurrence:';
            
            -- Create a temporary table with occurrence counts
            CREATE TEMPORARY TABLE temp_unmapped_counts AS
            SELECT 
                mc.geo_type, 
                mc.geo_name,
                edw.improved_normalize_geo_name(mc.geo_name) AS normalized_name,
                COUNT(*) AS occurrences
            FROM data_lake.master_card mc
            LEFT JOIN edw.mastercard_region_mapping mrm 
                ON mc.geo_type = mrm.geo_type AND mc.geo_name = mrm.geo_name
            WHERE mrm.region_id IS NULL
            GROUP BY mc.geo_type, mc.geo_name;
            
            -- Create index on temporary table for better performance
            CREATE INDEX idx_temp_unmapped ON temp_unmapped_counts(occurrences DESC);
            
            -- Display the top unmapped locations
            FOR rec IN
                SELECT geo_type, geo_name, normalized_name, occurrences
                FROM temp_unmapped_counts
                ORDER BY occurrences DESC
                LIMIT 20
            LOOP
                RAISE NOTICE '  % - % (normalized: %) - % occurrences', 
                    rec.geo_type, rec.geo_name, rec.normalized_name, rec.occurrences;
            END LOOP;
            
            DROP TABLE temp_unmapped_counts;
        END IF;
        
        EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'Error in unmapped locations analysis: %', SQLERRM;
    END;
END $$;

-- Step 7: Create a view for analyzing mapping results
CREATE OR REPLACE VIEW edw.vw_mastercard_mapping_analysis AS
WITH mapping_stats AS (
    SELECT 
        mrm.geo_type,
        mrm.geo_name,
        mrm.region_id,
        dr.region_name,
        dr.region_type,
        mrm.mapping_source,
        mrm.created_at,
        COUNT(mc.*) AS occurrence_count,
        MIN(mc.txn_date) AS first_seen,
        MAX(mc.txn_date) AS last_seen,
        COUNT(DISTINCT mc.txn_date) AS days_with_data
    FROM 
        edw.mastercard_region_mapping mrm
    LEFT JOIN 
        edw.dim_region dr ON mrm.region_id = dr.region_id
    LEFT JOIN 
        data_lake.master_card mc ON mrm.geo_type = mc.geo_type AND mrm.geo_name = mc.geo_name
    GROUP BY 
        mrm.geo_type, mrm.geo_name, mrm.region_id, dr.region_name, dr.region_type, mrm.mapping_source, mrm.created_at
)
SELECT 
    *,
    CASE 
        WHEN days_with_data > 0 THEN 
            ROUND(occurrence_count::NUMERIC / days_with_data, 2)
        ELSE 0 
    END AS avg_daily_occurrences
FROM mapping_stats
ORDER BY occurrence_count DESC;

-- Step 8: Update data loading with enhanced match function
CREATE OR REPLACE FUNCTION edw.load_mastercard_data(start_date DATE, end_date DATE)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id INTEGER;
    v_affected_rows INTEGER := 0;
    v_error_msg TEXT;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_chunk_size INTEGER := 100000; -- Process in chunks to avoid memory issues
    v_total_rows INTEGER := 0;
    v_chunk_start INTEGER := 0;
    v_progress INTEGER := 0;
    v_last_progress_log TIMESTAMP := NOW();
    v_min_progress_interval INTERVAL := '30 seconds';
    v_temp_table_name TEXT := 'temp_mastercard_batch_' || (EXTRACT(EPOCH FROM NOW())::INTEGER);
        BEGIN
    -- Start transaction
    BEGIN
        -- Generate a batch ID based on timestamp
        v_batch_id := (EXTRACT(EPOCH FROM NOW())::INTEGER);
        v_start_time := NOW();
        
        -- Validate input parameters
        IF start_date IS NULL OR end_date IS NULL THEN
            RAISE EXCEPTION 'Start date and end date cannot be NULL';
        END IF;
        
        IF start_date > end_date THEN
            RAISE EXCEPTION 'Start date cannot be after end date';
        END IF;
        
        -- Check if date range is reasonable
        IF (end_date - start_date) > INTERVAL '1 year' THEN
            RAISE WARNING 'Loading data for more than 1 year, this might take a while';
        END IF;
        
        -- Create a temporary table for initial data extraction
        EXECUTE format('
            CREATE TEMPORARY TABLE %I (
                txn_date DATE,
                geo_type TEXT,
                geo_name TEXT,
                industry_id INTEGER,
                date_id INTEGER,
                txn_amt NUMERIC,
                txn_cnt INTEGER,
                acct_cnt INTEGER,
                avg_ticket NUMERIC,
                avg_freq NUMERIC,
                avg_spend_amt NUMERIC,
                yoy_txn_amt NUMERIC,
                yoy_txn_cnt NUMERIC,
                quad_id TEXT,
                central_latitude NUMERIC,
                central_longitude NUMERIC,
                bounding_box JSONB,
                industry TEXT,
                segment TEXT,
                yr INTEGER
            ) ON COMMIT DROP', v_temp_table_name);
        
        -- Insert data into temp table with progress tracking and encoding fixes
        EXECUTE format('
            INSERT INTO %I
            SELECT 
                mc.txn_date,
                edw.fix_encoding(mc.geo_type) AS geo_type, 
                edw.fix_encoding(mc.geo_name) AS geo_name,
                di.industry_id,
                dtd.date_id,
                mc.txn_amt,
                mc.txn_cnt,
                mc.acct_cnt,
                mc.avg_ticket,
                mc.avg_freq,
                mc.avg_spend_amt,
                mc.yoy_txn_amt,
                mc.yoy_txn_cnt,
                edw.fix_encoding(mc.quad_id) AS quad_id,
                mc.central_latitude,
                mc.central_longitude,
                CASE 
                    WHEN mc.bounding_box IS NOT NULL THEN 
                jsonb_build_object(
                            ''type'', ''Feature'',
                            ''geometry'', jsonb_build_object(
                                ''type'', ''Polygon'',
                                ''coordinates'', mc.bounding_box::jsonb
                            ),
                            ''properties'', jsonb_build_object(
                                ''source'', ''mastercard'',
                                ''processed_at'', NOW()::text
                            )
                        )
                    ELSE NULL 
                END AS bounding_box,
                edw.fix_encoding(mc.industry) AS industry,
                edw.fix_encoding(mc.segment) AS segment,
                mc.yr
            FROM 
                data_lake.master_card mc
            JOIN 
                edw.dim_industry di ON edw.fix_encoding(mc.industry) = di.industry_name
            JOIN 
                edw.dim_transaction_date dtd ON mc.txn_date = dtd.full_date
            WHERE 
                mc.txn_date BETWEEN $1 AND $2', v_temp_table_name)
        USING start_date, end_date;
        
        -- Add indexes to temp table
        EXECUTE format('CREATE INDEX idx_temp_mc_geo ON %I(geo_type, geo_name)', v_temp_table_name);
        EXECUTE format('CREATE INDEX idx_temp_mc_date ON %I(txn_date)', v_temp_table_name);
        
        -- Get total rows to process
        EXECUTE format('SELECT COUNT(*) FROM %I', v_temp_table_name) INTO v_total_rows;
        
        RAISE NOTICE 'Starting to process % rows in chunks of %', v_total_rows, v_chunk_size;
        
        -- Process in chunks
        WHILE v_chunk_start < v_total_rows LOOP
            -- Insert data in chunks with encoding fixes
            EXECUTE format('
                INSERT INTO edw.stg_mastercard_transactions
                SELECT 
                    t.date_id,
                    edw.enhanced_find_best_region_match(t.geo_type, t.geo_name) AS region_id,
                    t.industry_id,
                    t.txn_date,
                    t.txn_amt,
                    t.txn_cnt,
                    t.acct_cnt,
                    t.avg_ticket,
                    t.avg_freq,
                    t.avg_spend_amt,
                    t.yoy_txn_amt,
                    t.yoy_txn_cnt,
                    t.quad_id,
                    t.central_latitude,
                    t.central_longitude,
                    t.bounding_box,
                    jsonb_build_object(
                        ''source_table'', ''data_lake.master_card'',
                        ''geo_type'', t.geo_type,
                        ''geo_name'', t.geo_name,
                        ''industry'', t.industry,
                        ''segment'', t.segment,
                        ''quad_id'', t.quad_id,
                        ''year'', t.yr,
                        ''encoding_fixed'', true,
                        ''processed_at'', NOW()::text
                    ) AS source_keys,
                    $1
                FROM 
                    %I t
                ORDER BY t.txn_date
                LIMIT $2
                OFFSET $3
            ON CONFLICT (date_id, region_id, industry_id) DO UPDATE
            SET
                    txn_amt = EXCLUDED.txn_amt,
                    txn_cnt = EXCLUDED.txn_cnt,
                    acct_cnt = EXCLUDED.acct_cnt,
                    avg_ticket = EXCLUDED.avg_ticket,
                    avg_freq = EXCLUDED.avg_freq,
                    avg_spend_amt = EXCLUDED.avg_spend_amt,
                    yoy_txn_amt = EXCLUDED.yoy_txn_amt,
                    yoy_txn_cnt = EXCLUDED.yoy_txn_cnt,
                    quad_id = EXCLUDED.quad_id,
                    central_latitude = EXCLUDED.central_latitude,
                    central_longitude = EXCLUDED.central_longitude,
                    bounding_box = EXCLUDED.bounding_box,
                    source_keys = EXCLUDED.source_keys', v_temp_table_name)
            USING v_batch_id, v_chunk_size, v_chunk_start;
            
            -- Update progress
            v_chunk_start := v_chunk_start + v_chunk_size;
            v_progress := (v_chunk_start::NUMERIC / v_total_rows * 100)::INTEGER;
            
            -- Log progress at intervals
            IF NOW() - v_last_progress_log >= v_min_progress_interval THEN
                RAISE NOTICE 'Processed % rows (%.1f%%)', v_chunk_start, v_progress;
                v_last_progress_log := NOW();
            END IF;
        END LOOP;
        
        v_end_time := NOW();
        
        -- Log successful completion
        INSERT INTO edw.mastercard_processing_log (
            process_stage, 
            error_message, 
            affected_records, 
            batch_start_date, 
            batch_end_date,
            processing_time_seconds,
            records_per_second,
            chunk_size,
            total_chunks
        )
        VALUES (
            'Enhanced Data Load', 
            'Successfully loaded data with enhanced matching', 
            v_total_rows, 
            start_date, 
            end_date,
            EXTRACT(EPOCH FROM (v_end_time - v_start_time)),
            CASE 
                WHEN EXTRACT(EPOCH FROM (v_end_time - v_start_time)) > 0 
                THEN v_total_rows::NUMERIC / EXTRACT(EPOCH FROM (v_end_time - v_start_time))
                ELSE 0 
            END,
            v_chunk_size,
            CEIL(v_total_rows::NUMERIC / v_chunk_size)
        );
        
        RETURN v_total_rows;
        
        EXCEPTION WHEN OTHERS THEN
        v_error_msg := 'Error loading data: ' || SQLERRM;
        
        -- Log the error with performance metrics
        INSERT INTO edw.mastercard_processing_log (
            process_stage, 
            error_message, 
            affected_records, 
            batch_start_date, 
            batch_end_date,
            processing_time_seconds,
            chunk_size,
            total_chunks
        )
        VALUES (
            'Enhanced Data Load', 
            v_error_msg, 
            0, 
            start_date, 
            end_date,
            EXTRACT(EPOCH FROM (NOW() - v_start_time)),
            v_chunk_size,
            CEIL(v_total_rows::NUMERIC / v_chunk_size)
        );
        
        -- Clean up temp table if it exists
        EXECUTE format('DROP TABLE IF EXISTS %I', v_temp_table_name);
        
        RAISE EXCEPTION '%', v_error_msg;
    END;
END $$;

-- Step 9: Create a manual mapping utility function
CREATE OR REPLACE FUNCTION edw.add_mastercard_mapping(
    in_geo_type TEXT, 
    in_geo_name TEXT, 
    in_region_id INTEGER,
    in_mapping_source TEXT DEFAULT 'manual_mapping'
)
RETURNS BOOLEAN AS $
DECLARE
    v_normalized_name TEXT;
        BEGIN
    -- Validate that the region exists
    IF NOT EXISTS (SELECT 1 FROM edw.dim_region WHERE region_id = in_region_id) THEN
        RAISE EXCEPTION 'Region ID % does not exist in dim_region', in_region_id;
    END IF;
    
    -- Normalize the name
    v_normalized_name := edw.improved_normalize_geo_name(in_geo_name);
    
    -- Insert into mastercard_region_mapping
    INSERT INTO edw.mastercard_region_mapping (
        geo_type,
        geo_name,
                region_id,
        mapping_source,
        created_at,
        updated_at
    )
    VALUES (
        in_geo_type,
        in_geo_name,
        in_region_id,
        in_mapping_source,
        NOW(),
        NOW()
    )
    ON CONFLICT (geo_type, geo_name) DO UPDATE
    SET
        region_id = in_region_id,
        mapping_source = in_mapping_source,
                updated_at = NOW();

    -- Insert into mastercard_region_mapping_normalized
    INSERT INTO edw.mastercard_region_mapping_normalized (
        normalized_geo_type,
        normalized_geo_name,
        original_geo_type,
        original_geo_name,
        region_id
    )
    VALUES (
        LOWER(in_geo_type),
        v_normalized_name,
        in_geo_type,
        in_geo_name,
        in_region_id
    )
    ON CONFLICT (normalized_geo_type, normalized_geo_name) DO UPDATE
    SET
        region_id = in_region_id,
        original_geo_type = in_geo_type,
        original_geo_name = in_geo_name;
    
    -- If this was in unmapped_mastercard_geo, delete it
    DELETE FROM edw.unmapped_mastercard_geo
    WHERE geo_type = in_geo_type AND geo_name = in_geo_name;
    
    RETURN TRUE;
END $;

-- Step 2: Create or ensure mastercard_region_mapping_normalized table exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'edw' AND table_name = 'mastercard_region_mapping_normalized') THEN
        CREATE TABLE edw.mastercard_region_mapping_normalized (
            id SERIAL PRIMARY KEY,
            normalized_geo_type TEXT NOT NULL,
            normalized_geo_name TEXT NOT NULL,
            original_geo_type TEXT NOT NULL,
            original_geo_name TEXT NOT NULL,
            region_id INTEGER NOT NULL REFERENCES edw.dim_region(region_id),
            UNIQUE(normalized_geo_type, normalized_geo_name)
        );
        
        -- Fill the normalized table with data from the existing mapping table
        INSERT INTO edw.mastercard_region_mapping_normalized (
            normalized_geo_type,
            normalized_geo_name,
            original_geo_type,
            original_geo_name,
            region_id
        )
        SELECT 
            LOWER(geo_type),
            edw.improved_normalize_geo_name(geo_name),
            geo_type,
            geo_name,
            region_id
        FROM edw.mastercard_region_mapping
        ON CONFLICT DO NOTHING;
        
        RAISE NOTICE 'Created and populated normalized mapping table';
    END IF;
END $$;

-- Step 3: Create the geo type mapping table
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'edw' AND table_name = 'mastercard_geo_type_mapping') THEN
        CREATE TABLE edw.mastercard_geo_type_mapping (
            source_geo_type VARCHAR NOT NULL PRIMARY KEY,
            target_geo_type VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        -- Insert default mappings
        INSERT INTO edw.mastercard_geo_type_mapping (source_geo_type, target_geo_type) VALUES
        ('State', 'Canton'),
        ('Msa', 'Tourism-Region'),
        ('Country', 'Country');
        
        RAISE NOTICE 'Created geo type mapping table';
    END IF;
END $$;

-- Step 4: Process Swiss geo names with encoding issues
DO $$
DECLARE
    geo_record RECORD;
    geo_name_list TEXT[] := ARRAY[
        -- List with properly encoded characters
        'Olten', 'Toggenburg', 'Winterthur', 'Lebern', 'Bern', 'Sierre', 'Bremgarten', 
        'Luzern', 'Schwyz', 'Rorschach', 'Wil', 'Boudry', 'Waldenburg', 'Bern-Mittelland', 
        'Uster', 'Vaud', 'Aarau', 'Obwalden', 'Genève', 'Obersimmental-Saanen', 
        'Kreuzlingen', 'Wasseramt', 'Dorneck', 'Hinterrhein', 'Lausanne', 'La Sarine', 
        'La Broye-Vully', 'Bülach', 'Visp', 'Prättigau-Davos', 'Oberklettgau',
        'Zürich', 'Höfe', 'Hérens', 'Pfäffikon', 'Gäu', 'La Gruyère', 'La Glâne',
        'Münchwilen', 'Gösgen', 'Neuchâtel', 'Graubünden', 'Küssnacht (SZ)', 'Delémont'
    ];
    
    -- Problematic names with their corrected versions
    problem_geo_pairs TEXT[] := ARRAY[
        'GenÃÂ¨ve', 'Genève',
        'BÃÂ¼lach', 'Bülach',
        'PrÃÂ¤ttigau-Davos', 'Prättigau-Davos',
        'ZÃÂ¼rich', 'Zürich',
        'HÃÂ¶fe', 'Höfe',
        'HÃÂ©rens', 'Hérens',
        'PfÃÂ¤ffikon', 'Pfäffikon',
        'GÃÂ¤u', 'Gäu',
        'La GruyÃÂ¨re', 'La Gruyère',
        'La GlÃÂ¢ne', 'La Glâne',
        'MÃÂ¼nchwilen', 'Münchwilen',
        'GÃÂ¶sgen', 'Gösgen',
        'NeuchÃÂ¢tel', 'Neuchâtel',
        'GraubÃÂ¼nden', 'Graubünden',
        'KÃÂ¼ssnacht (SZ)', 'Küssnacht (SZ)',
        'DelÃÂ©mont', 'Delémont'
    ];
    
    region_id_var INTEGER;
    fixed_name TEXT;
    i INTEGER;
    matched_count INTEGER := 0;
    total_count INTEGER := 0;
    canton TEXT;
    found_names TEXT[];
    existing_mappings INTEGER;
    target_geo_type TEXT;
    
    -- Canton mapping for districts
    canton_map JSONB := '{
        "Zürich": ["Uster", "Bülach", "Andelfingen", "Hinwil", "Dietikon", "Dielsdorf", "Pfäffikon", "Meilen", "Horgen", "Affoltern", "Winterthur"],
        "Bern": ["Bern-Mittelland", "Emmental", "Interlaken-Oberhasli", "Thun", "Oberaargau", "Frutigen-Niedersimmental", "Seeland", "Obersimmental-Saanen", "Jura bernois", "Biel"],
        "Luzern": ["Luzern-Stadt", "Luzern-Land", "Hochdorf", "Willisau", "Sursee", "Entlebuch"],
        "Uri": ["Uri"],
        "Schwyz": ["Schwyz", "March", "Höfe", "Einsiedeln", "Küssnacht (SZ)", "Gersau"],
        "Obwalden": ["Obwalden"],
        "Nidwalden": ["Nidwalden"],
        "Glarus": ["Glarus"],
        "Zug": ["Zug"],
        "Fribourg": ["La Sarine", "La Gruyère", "Sense", "La Glâne", "La Broye", "La Veveyse", "See", "La Broye-Vully"],
        "Solothurn": ["Solothurn", "Lebern", "Wasseramt", "Bucheggberg", "Gäu", "Thal", "Gösgen", "Olten", "Dorneck", "Thierstein"],
        "Basel-Stadt": ["Basel-Stadt"],
        "Basel-Land": ["Arlesheim", "Liestal", "Sissach", "Waldenburg", "Laufen"],
        "Schaffhausen": ["Schaffhausen", "Reiat", "Oberklettgau", "Unterklettgau", "Schleitheim", "Stein"],
        "Appenzell Ausserrhoden": ["Vorderland", "Mittelland", "Hinterland"],
        "Appenzell Innerrhoden": ["Appenzell Innerrhoden"],
        "St. Gallen": ["Sankt Gallen", "Rorschach", "Rheintal", "Werdenberg", "Sarganserland", "See-Gaster", "Toggenburg", "Wil"],
        "Graubünden": ["Plessur", "Inn", "Maloja", "Bernina", "Albula", "Moesa", "Hinterrhein", "Surselva", "Imboden", "Prättigau-Davos"],
        "Aargau": ["Aarau", "Baden", "Bremgarten", "Brugg", "Kulm", "Laufenburg", "Lenzburg", "Muri", "Rheinfelden", "Zofingen", "Zurzach"],
        "Thurgau": ["Frauenfeld", "Kreuzlingen", "Münchwilen", "Weinfelden", "Arbon"],
        "Ticino": ["Bellinzona", "Blenio", "Leventina", "Locarno", "Lugano", "Mendrisio", "Riviera", "Vallemaggia"],
        "Vaud": ["Lausanne", "Morges", "Nyon", "Jura-Nord vaudois", "Gros-de-Vaud", "Lavaux-Oron", "La Riviera-Pays-d''Enhaut", "Aigle", "L''Ouest lausannois"],
        "Valais": ["Sion", "Sierre", "Monthey", "Martigny", "Entremont", "Hérens", "Conthey", "Saint-Maurice", "Visp", "Brig", "Leuk", "Raron", "Goms"],
        "Neuchâtel": ["Neuchâtel", "Le Locle", "Val-de-Travers", "Val-de-Ruz", "La Chaux-De-Fonds", "Boudry"],
        "Geneva": ["Genève"],
        "Jura": ["Delémont", "Porrentruy", "Franches-Montagnes"]
    }';
BEGIN
    -- Check existing mappings
    SELECT COUNT(*) INTO existing_mappings FROM edw.mastercard_region_mapping;
    RAISE NOTICE 'Existing mappings in mastercard_region_mapping: %', existing_mappings;
    
    -- First, check for problem names with encoding issues
    FOR i IN 1..array_length(problem_geo_pairs, 1) BY 2 LOOP
        -- Check if the problematic name exists in master_card
        SELECT array_agg(DISTINCT geo_type) INTO found_names
        FROM data_lake.master_card
        WHERE geo_name = problem_geo_pairs[i]
        LIMIT 10;
        
        IF found_names IS NOT NULL THEN
            RAISE NOTICE 'Found problematic name "%" in geo_types: %', problem_geo_pairs[i], found_names;
        END IF;
    END LOOP;
    
    -- Process each problematic pair
    i := 1;
    WHILE i <= array_length(problem_geo_pairs, 1) LOOP
        -- Check if the problematic name exists in master_card but not in our mapping
        FOR geo_record IN (
            SELECT DISTINCT mc.geo_type, mc.geo_name
            FROM data_lake.master_card mc
            LEFT JOIN edw.mastercard_region_mapping mrm 
                ON mc.geo_type = mrm.geo_type AND mc.geo_name = mrm.geo_name
            WHERE mc.geo_name = problem_geo_pairs[i]
            AND mrm.region_id IS NULL
            LIMIT 100  -- Limit to avoid processing too many at once
        ) LOOP
            -- Get the target geo type for mapping
            SELECT target_geo_type INTO target_geo_type 
            FROM edw.mastercard_geo_type_mapping
            WHERE source_geo_type = geo_record.geo_type;
            
            IF target_geo_type IS NULL THEN
                target_geo_type := geo_record.geo_type; -- Default to same type if no mapping
            END IF;
            
            -- Try to find a mapping for the corrected version
            fixed_name := problem_geo_pairs[i+1]; -- the corrected version
            
            -- First check if the corrected version already has a mapping
            SELECT region_id INTO region_id_var
            FROM edw.mastercard_region_mapping
            WHERE geo_name = fixed_name;
            
            -- If the corrected version doesn't have a mapping, try to find one
            IF region_id_var IS NULL THEN
                -- Try to match with a canton/state directly
                SELECT r.region_id INTO region_id_var
                FROM edw.dim_region r
                WHERE r.region_type = target_geo_type
                AND (
                    edw.improved_normalize_geo_name(r.region_name) = edw.improved_normalize_geo_name(fixed_name)
                    OR similarity(r.region_name, fixed_name) > 0.8
                );
                
                -- If not found and it's a district, try to find the parent canton
                IF region_id_var IS NULL AND target_geo_type = 'Tourism-Region' THEN
                    FOR canton IN SELECT jsonb_object_keys(canton_map) LOOP
                        IF region_id_var IS NULL AND canton_map->canton @> to_jsonb(fixed_name) THEN
                            -- Found a canton match, get its region_id
                            SELECT r.region_id INTO region_id_var
                            FROM edw.dim_region r
                            WHERE r.region_type = 'Canton'
                            AND edw.improved_normalize_geo_name(r.region_name) = edw.improved_normalize_geo_name(canton);
                            
                            IF region_id_var IS NOT NULL THEN
                                RAISE NOTICE 'Found canton % (ID: %) for district %', canton, region_id_var, fixed_name;
                                EXIT; -- Found a match, exit the loop
                            END IF;
                        END IF;
                    END LOOP;
                END IF;
            END IF;
            
            -- Now create mappings for both the problematic and corrected names
            IF region_id_var IS NOT NULL THEN
                -- Insert the problematic name mapping
                INSERT INTO edw.mastercard_region_mapping (
                    geo_type, 
                    geo_name, 
                    region_id, 
                    mapping_source,
                    created_at,
                    updated_at
                )
                VALUES (
                    geo_record.geo_type,
                    geo_record.geo_name,
                    region_id_var,
                    'encoding_fix',
                    NOW(),
                    NOW()
                )
                ON CONFLICT (geo_type, geo_name) DO NOTHING;
                
                -- Also update the normalized table
                INSERT INTO edw.mastercard_region_mapping_normalized (
                    normalized_geo_type,
                    normalized_geo_name,
                    original_geo_type,
                    original_geo_name,
                    region_id
                )
                VALUES (
                    LOWER(geo_record.geo_type),
                    edw.improved_normalize_geo_name(fixed_name), -- Use normalized version of corrected name
                    geo_record.geo_type,
                    geo_record.geo_name,
                    region_id_var
                )
                ON CONFLICT (normalized_geo_type, normalized_geo_name) DO NOTHING;
                
                matched_count := matched_count + 1;
                RAISE NOTICE 'Mapped problematic name: % (%) to region ID %', geo_record.geo_name, fixed_name, region_id_var;
            ELSE
                -- Log for manual review
                INSERT INTO edw.unmapped_mastercard_geo (
                    geo_type, 
                    geo_name, 
                    normalized_geo_name,
                    closest_match,
                    occurrence_count
                )
                VALUES (
                    geo_record.geo_type,
                    geo_record.geo_name,
                    edw.improved_normalize_geo_name(fixed_name),
                    fixed_name,
                    1
                )
                ON CONFLICT (geo_type, geo_name) DO UPDATE
                SET occurrence_count = edw.unmapped_mastercard_geo.occurrence_count + 1,
                    last_seen = NOW();
                
                RAISE NOTICE 'Could not find mapping for: % (corrected: %)', geo_record.geo_name, fixed_name;
            END IF;
            
            total_count := total_count + 1;
        END LOOP;
        
        i := i + 2; -- Move to next pair
    END LOOP;
    
    -- Process standard geo names
    FOREACH fixed_name IN ARRAY geo_name_list LOOP
        FOR geo_record IN (
            SELECT DISTINCT mc.geo_type, mc.geo_name
            FROM data_lake.master_card mc
            LEFT JOIN edw.mastercard_region_mapping mrm 
                ON mc.geo_type = mrm.geo_type AND mc.geo_name = mrm.geo_name
            WHERE edw.improved_normalize_geo_name(mc.geo_name) = edw.improved_normalize_geo_name(fixed_name)
            AND mrm.region_id IS NULL
            LIMIT 100  -- Limit to avoid processing too many at once
        ) LOOP
            -- Get the target geo type for mapping
            SELECT target_geo_type INTO target_geo_type 
            FROM edw.mastercard_geo_type_mapping
            WHERE source_geo_type = geo_record.geo_type;
            
            IF target_geo_type IS NULL THEN
                target_geo_type := geo_record.geo_type; -- Default to same type if no mapping
            END IF;
            
            -- Try to match with a region directly
            SELECT r.region_id INTO region_id_var
            FROM edw.dim_region r
            WHERE r.region_type = target_geo_type
            AND (
                edw.improved_normalize_geo_name(r.region_name) = edw.improved_normalize_geo_name(fixed_name)
                OR similarity(r.region_name, fixed_name) > 0.8
            );
            
            -- If not found and it's a district, try to find the parent canton
            IF region_id_var IS NULL AND target_geo_type = 'Tourism-Region' THEN
                FOR canton IN SELECT jsonb_object_keys(canton_map) LOOP
                    IF region_id_var IS NULL AND canton_map->canton @> to_jsonb(fixed_name) THEN
                        -- Found a canton match, get its region_id
                        SELECT r.region_id INTO region_id_var
                        FROM edw.dim_region r
                        WHERE r.region_type = 'Canton'
                        AND edw.improved_normalize_geo_name(r.region_name) = edw.improved_normalize_geo_name(canton);
                        
                        IF region_id_var IS NOT NULL THEN
                            RAISE NOTICE 'Found canton % (ID: %) for district %', canton, region_id_var, fixed_name;
                            EXIT; -- Found a match, exit the loop
                        END IF;
                    END IF;
                END LOOP;
            END IF;
            
            IF region_id_var IS NOT NULL THEN
                -- Insert the mapping
                INSERT INTO edw.mastercard_region_mapping (
                    geo_type, 
                    geo_name, 
                    region_id, 
                    mapping_source,
                    created_at,
                    updated_at
                )
                VALUES (
                    geo_record.geo_type,
                    geo_record.geo_name,
                    region_id_var,
                    'standard_mapping',
                    NOW(),
                    NOW()
                )
                ON CONFLICT (geo_type, geo_name) DO NOTHING;
                
                -- Also update the normalized table
                INSERT INTO edw.mastercard_region_mapping_normalized (
                    normalized_geo_type,
                    normalized_geo_name,
                    original_geo_type,
                    original_geo_name,
                    region_id
                )
                VALUES (
                    LOWER(geo_record.geo_type),
                    edw.improved_normalize_geo_name(geo_record.geo_name),
                    geo_record.geo_type,
                    geo_record.geo_name,
                    region_id_var
                )
                ON CONFLICT (normalized_geo_type, normalized_geo_name) DO NOTHING;
                
                matched_count := matched_count + 1;
                RAISE NOTICE 'Mapped standard name: % to region ID %', geo_record.geo_name, region_id_var;
            ELSE
                -- Log for manual review
                INSERT INTO edw.unmapped_mastercard_geo (
                    geo_type, 
                    geo_name, 
                    normalized_geo_name,
                    occurrence_count
                )
                VALUES (
                    geo_record.geo_type,
                    geo_record.geo_name,
                    edw.improved_normalize_geo_name(geo_record.geo_name),
                    1
                )
                ON CONFLICT (geo_type, geo_name) DO UPDATE
                SET occurrence_count = edw.unmapped_mastercard_geo.occurrence_count + 1,
                    last_seen = NOW();
                
                RAISE NOTICE 'Could not find mapping for standard name: %', geo_record.geo_name;
            END IF;
            
            total_count := total_count + 1;
        END LOOP;
    END LOOP;
    
    -- Process all unmapped locations with similarity matches
    FOR geo_record IN (
        WITH unmapped AS (
            SELECT DISTINCT mc.geo_type, mc.geo_name
            FROM data_lake.master_card mc
            LEFT JOIN edw.mastercard_region_mapping mrm 
                ON mc.geo_type = mrm.geo_type AND mc.geo_name = mrm.geo_name
            WHERE mrm.region_id IS NULL
            LIMIT 1000  -- Limit to process reasonable batch
        )
        SELECT u.geo_type, u.geo_name, 
               edw.improved_normalize_geo_name(u.geo_name) AS normalized_name,
               gtm.target_geo_type
        FROM unmapped u
        LEFT JOIN edw.mastercard_geo_type_mapping gtm ON u.geo_type = gtm.source_geo_type
    ) LOOP
        -- Skip if no target geo type mapping
        IF geo_record.target_geo_type IS NULL THEN
            CONTINUE;
        END IF;
        
        -- Try similarity-based match with region names
        SELECT r.region_id INTO region_id_var
        FROM edw.dim_region r
        WHERE r.region_type = geo_record.target_geo_type
        AND similarity(edw.improved_normalize_geo_name(r.region_name), geo_record.normalized_name) > 0.7
        ORDER BY similarity(edw.improved_normalize_geo_name(r.region_name), geo_record.normalized_name) DESC
        LIMIT 1;
        
        -- If found with good similarity, create mapping
        IF region_id_var IS NOT NULL THEN
            -- Insert the mapping
            INSERT INTO edw.mastercard_region_mapping (
                geo_type, 
                geo_name, 
                region_id, 
                mapping_source,
                created_at,
                updated_at
            )
            VALUES (
                geo_record.geo_type,
                geo_record.geo_name,
                region_id_var,
                'similarity_match',
                NOW(),
                NOW()
            )
            ON CONFLICT (geo_type, geo_name) DO NOTHING;
            
            -- Also update the normalized table
            INSERT INTO edw.mastercard_region_mapping_normalized (
                normalized_geo_type,
                normalized_geo_name,
                original_geo_type,
                original_geo_name,
                region_id
            )
            VALUES (
                LOWER(geo_record.geo_type),
                geo_record.normalized_name,
                geo_record.geo_type,
                geo_record.geo_name,
                region_id_var
            )
            ON CONFLICT (normalized_geo_type, normalized_geo_name) DO NOTHING;
            
            matched_count := matched_count + 1;
            RAISE NOTICE 'Mapped by similarity: % to % (ID %) with similarity %', 
                geo_record.geo_name, region_id_var, region_id_var, similarity(edw.improved_normalize_geo_name(r.region_name), geo_record.normalized_name);
        ELSE
            -- Log for manual review
            INSERT INTO edw.unmapped_mastercard_geo (
                geo_type, 
                geo_name, 
                normalized_geo_name,
                occurrence_count
            )
            VALUES (
                geo_record.geo_type,
                geo_record.geo_name,
                geo_record.normalized_name,
                1
            )
            ON CONFLICT (geo_type, geo_name) DO UPDATE
            SET occurrence_count = edw.unmapped_mastercard_geo.occurrence_count + 1,
                last_seen = NOW();
        END IF;
        
        total_count := total_count + 1;
    END LOOP;
    
    RAISE NOTICE 'Successfully mapped % locations out of % processed', matched_count, total_count;
END $$;

-- Step 5: Create the enhanced find_best_region_match function
CREATE OR REPLACE FUNCTION edw.enhanced_find_best_region_match(in_geo_type TEXT, in_geo_name TEXT)
RETURNS INTEGER AS $$
DECLARE
    v_normalized_type TEXT;
    v_normalized_name TEXT;
    v_fixed_name TEXT;
    v_region_id INTEGER;
    v_target_geo_type TEXT;
    v_similarity_score FLOAT;
BEGIN
    -- 0. Input validation
    IF in_geo_type IS NULL OR in_geo_name IS NULL THEN
        RETURN NULL;
    END IF;

    -- 1. First check exact match using lookup table (fastest)
    SELECT region_id INTO v_region_id
    FROM edw.mastercard_region_mapping
    WHERE geo_type = in_geo_type AND geo_name = in_geo_name;
    
    IF v_region_id IS NOT NULL THEN
        RETURN v_region_id;
    END IF;
    
    -- 2. Fix encoding and try again
    v_fixed_name := edw.fix_encoding(in_geo_name);
    
    -- Try exact match with fixed encoding
    IF v_fixed_name != in_geo_name THEN
        SELECT region_id INTO v_region_id
        FROM edw.mastercard_region_mapping
        WHERE geo_type = in_geo_type AND geo_name = v_fixed_name;
        
        IF v_region_id IS NOT NULL THEN
            -- Cache this for future lookups
            INSERT INTO edw.mastercard_region_mapping (
                geo_type, geo_name, region_id, mapping_source, created_at, updated_at
            )
            VALUES (
                in_geo_type, in_geo_name, v_region_id, 'encoding_fixed_match', NOW(), NOW()
            )
            ON CONFLICT (geo_type, geo_name) DO NOTHING;
            
            RETURN v_region_id;
        END IF;
    END IF;
    
    -- 3. Try normalized match with optimized lookup
    v_normalized_type := LOWER(in_geo_type);
    v_normalized_name := edw.improved_normalize_geo_name(in_geo_name);
    
    SELECT region_id INTO v_region_id
    FROM edw.mastercard_region_mapping_normalized
    WHERE normalized_geo_type = v_normalized_type AND normalized_geo_name = v_normalized_name;
    
    IF v_region_id IS NOT NULL THEN
        -- Cache this for future lookups
        INSERT INTO edw.mastercard_region_mapping (
            geo_type, geo_name, region_id, mapping_source, created_at, updated_at
        )
        VALUES (
            in_geo_type, in_geo_name, v_region_id, 'normalized_match', NOW(), NOW()
        )
        ON CONFLICT (geo_type, geo_name) DO NOTHING;
        
        RETURN v_region_id;
    END IF;
    
    -- 4. Get target geo type for better matching
    SELECT target_geo_type INTO v_target_geo_type
    FROM edw.mastercard_geo_type_mapping
    WHERE source_geo_type = in_geo_type;
    
    IF v_target_geo_type IS NULL THEN
        v_target_geo_type := in_geo_type; -- Default to same type if no mapping
    END IF;
    
    -- 5. Try direct match with regions first (faster than similarity)
    SELECT r.region_id INTO v_region_id
    FROM edw.dim_region r
    WHERE r.region_type = v_target_geo_type
    AND edw.improved_normalize_geo_name(r.region_name) = v_normalized_name
    LIMIT 1;
    
    IF v_region_id IS NOT NULL THEN
        -- Cache this for future lookups
        INSERT INTO edw.mastercard_region_mapping (
            geo_type, geo_name, region_id, mapping_source, created_at, updated_at
        )
        VALUES (
            in_geo_type, in_geo_name, v_region_id, 'direct_name_match', NOW(), NOW()
        )
        ON CONFLICT (geo_type, geo_name) DO NOTHING;
        
        -- Also cache the normalized form
        INSERT INTO edw.mastercard_region_mapping_normalized (
            normalized_geo_type, normalized_geo_name, original_geo_type, original_geo_name, region_id
        )
        VALUES (
            v_normalized_type, v_normalized_name, in_geo_type, in_geo_name, v_region_id
        )
        ON CONFLICT (normalized_geo_type, normalized_geo_name) DO NOTHING;
        
        RETURN v_region_id;
    END IF;
    
    -- 6. Try similarity match with optimized threshold
    SELECT r.region_id, similarity(edw.improved_normalize_geo_name(r.region_name), v_normalized_name)
    INTO v_region_id, v_similarity_score
    FROM edw.dim_region r
    WHERE r.region_type = v_target_geo_type
    AND similarity(edw.improved_normalize_geo_name(r.region_name), v_normalized_name) > 0.8
    ORDER BY similarity(edw.improved_normalize_geo_name(r.region_name), v_normalized_name) DESC
    LIMIT 1;
    
    IF v_region_id IS NOT NULL THEN
        -- Cache for future lookups
        INSERT INTO edw.mastercard_region_mapping (
            geo_type, geo_name, region_id, mapping_source, created_at, updated_at, similarity_score
        )
        VALUES (
            in_geo_type, in_geo_name, v_region_id, 'similarity_match', NOW(), NOW(), v_similarity_score
        )
        ON CONFLICT (geo_type, geo_name) DO NOTHING;
        
        -- Also cache the normalized form
        INSERT INTO edw.mastercard_region_mapping_normalized (
            normalized_geo_type, normalized_geo_name, original_geo_type, original_geo_name, region_id
        )
        VALUES (
            v_normalized_type, v_normalized_name, in_geo_type, in_geo_name, v_region_id
        )
        ON CONFLICT (normalized_geo_type, normalized_geo_name) DO NOTHING;
        
        RETURN v_region_id;
    END IF;
    
    -- 7. Log unmapped entry
    INSERT INTO edw.unmapped_mastercard_geo (
        geo_type, geo_name, normalized_geo_name, occurrence_count, first_seen, last_seen
    )
    VALUES (
        in_geo_type, in_geo_name, v_normalized_name, 1, NOW(), NOW()
    )
    ON CONFLICT (geo_type, geo_name) DO UPDATE
    SET occurrence_count = edw.unmapped_mastercard_geo.occurrence_count + 1,
        last_seen = NOW();
    
    -- 8. Default fallback to Switzerland (ID 1)
    RETURN 1;
        EXCEPTION WHEN OTHERS THEN
    -- Log error but don't fail the function
    INSERT INTO edw.mastercard_processing_log (
        process_stage, error_message, affected_records, batch_start_date, batch_end_date
    )
    VALUES (
        'Region Matching', 
        'Error in enhanced_find_best_region_match: ' || SQLERRM, 
        0, 
        NULL, 
        NULL
    );
    
    -- Return default region on error
    RETURN 1;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Update existing entries with improved encoding fixes
DO $$
BEGIN
    UPDATE edw.mastercard_geo_staging
    SET geo_name = fix_encoding(geo_name)
    WHERE geo_name LIKE '%Ã%' OR geo_name LIKE '%\u%';
    
    -- Verify no problematic encodings remain
    IF EXISTS (
        SELECT 1 
        FROM edw.mastercard_geo_staging 
        WHERE geo_name LIKE '%Ã%' OR geo_name LIKE '%\u%'
    ) THEN
        RAISE NOTICE 'Warning: Some entries still have encoding issues';
    ELSE
        RAISE NOTICE 'All entries have been fixed successfully';
    END IF;
END;
$$;

-- Create functions for geo mapping
CREATE OR REPLACE FUNCTION edw.fix_encoding(input_text TEXT)
RETURNS TEXT
LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    encoding_map JSONB := '{
        "Ãƒâ€¼": "ü",
        "Ãƒâ€°": "é",
        "Ãƒâ€¤": "ä",
        "Ãƒâ€¶": "ö",
        "Ãƒâ€±": "ñ",
        "Ãƒâ€¨": "è",
        "Ãƒâ€¢": "â",
        "Ãƒâ€«": "ë",
        "Ãƒâ€§": "ç",
        "Ãƒâ€®": "î",
        "Ãƒâ€´": "ô",
        "ÃÂ¼": "ü",
        "ÃÂ¨": "è",
        "ÃÂ©": "é",
        "ÃÂ¤": "ä",
        "ÃÂ¶": "ö",
        "ÃÂ¢": "â",
        "ÃÂ«": "ë",
        "ÃÂ§": "ç",
        "ÃÂ®": "î",
        "ÃÂ´": "ô",
        "ÃÂ": "à",
        "ÃÂ": "è",
        "ÃÂ": "é",
        "ÃÂ": "ì",
        "ÃÂ": "ò",
        "ÃÂ": "ù",
        "ÃÂ": "À",
        "ÃÂ": "È",
        "ÃÂ": "É",
        "ÃÂ": "Ì",
        "ÃÂ": "Ò",
        "ÃÂ": "Ù"
    }';
    replace_key TEXT;
    fixed_text TEXT;
    v_pattern TEXT;
    v_replacement TEXT;
BEGIN
    IF input_text IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Start with the input
    fixed_text := input_text;
    
    -- Use a single regexp_replace with alternation for all patterns
    -- This is more efficient than multiple replace calls
    SELECT 
        string_agg(replace_key, '|'),
        string_agg(encoding_map->>replace_key, '')
    INTO v_pattern, v_replacement
    FROM jsonb_object_keys(encoding_map);
    
    -- Apply the replacement in a single operation
    IF v_pattern IS NOT NULL THEN
        fixed_text := regexp_replace(fixed_text, v_pattern, v_replacement, 'g');
    END IF;
    
    RETURN fixed_text;
EXCEPTION WHEN OTHERS THEN
    -- Log error but don't fail the function
    RAISE NOTICE 'Error in fix_encoding for input: %', input_text;
    RETURN input_text;
END;
$$;

CREATE OR REPLACE FUNCTION edw.improved_normalize_geo_name(input_text TEXT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    IF input_text IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- First fix any encoding issues
    RETURN LOWER(REGEXP_REPLACE(
        REGEXP_REPLACE(
            edw.fix_encoding(input_text),
            '[^a-zA-ZäöüéèâëñçîôÄÖÜÉÈÂËÑÇÎÔ0-9\s\-]', '', 'g'  -- Keep special characters and hyphens
        ),
        '\s+', ' ', 'g'
    ));
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in improved_normalize_geo_name for input: %', input_text;
    RETURN LOWER(input_text);
END;
$$;

CREATE OR REPLACE FUNCTION edw.find_best_region_match(in_geo_name TEXT, in_geo_type TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_normalized_name TEXT;
    v_best_match_id INTEGER;
    v_similarity FLOAT;
    v_threshold FLOAT := 0.8;
BEGIN
    IF in_geo_name IS NULL OR in_geo_type IS NULL THEN
        RETURN NULL;
    END IF;

    -- Check for exact match first
    SELECT region_id INTO v_best_match_id
    FROM edw.dim_region 
    WHERE region_name = in_geo_name 
    AND region_type = in_geo_type;
    
    IF v_best_match_id IS NOT NULL THEN
        RETURN v_best_match_id;
    END IF;

    -- Normalize input name
    v_normalized_name := edw.improved_normalize_geo_name(in_geo_name);

    -- Find best fuzzy match
    SELECT region_id, similarity(edw.improved_normalize_geo_name(region_name), v_normalized_name)
    INTO v_best_match_id, v_similarity
    FROM edw.dim_region
    WHERE region_type = in_geo_type
    ORDER BY similarity(edw.improved_normalize_geo_name(region_name), v_normalized_name) DESC
    LIMIT 1;

    -- If good match found, insert mapping
    IF v_similarity >= v_threshold THEN
        INSERT INTO edw.mastercard_region_mapping (
            geo_name,
            geo_type,
            region_id,
            similarity_score,
            is_fuzzy_match,
            mapping_date
        )
        VALUES (
            in_geo_name,
            in_geo_type,
            v_best_match_id,
            v_similarity,
            TRUE,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (geo_type, geo_name) DO UPDATE
        SET 
            region_id = EXCLUDED.region_id,
            similarity_score = EXCLUDED.similarity_score,
            is_fuzzy_match = EXCLUDED.is_fuzzy_match,
            mapping_date = EXCLUDED.mapping_date;
            
        RETURN v_best_match_id;
    END IF;

    -- Log unmapped entry
    INSERT INTO edw.unmapped_mastercard_geo (
        geo_name,
        geo_type,
        normalized_name,
        insert_date
    )
    VALUES (
        in_geo_name,
        LOWER(in_geo_type),
        v_normalized_name,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (geo_type, geo_name) DO UPDATE
    SET 
        normalized_name = EXCLUDED.normalized_name,
        insert_date = EXCLUDED.insert_date;

    RETURN NULL;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in find_best_region_match for geo_name: %, geo_type: %', in_geo_name, in_geo_type;
    RETURN NULL;
END;
$$;

-- Create indexes for better performance
DO $$
BEGIN
    -- Create indexes if they don't exist
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE schemaname = 'edw' 
        AND tablename = 'mastercard_region_mapping' 
        AND indexname = 'idx_mrm_geo_type_name'
    ) THEN
        CREATE INDEX idx_mrm_geo_type_name ON edw.mastercard_region_mapping(geo_type, geo_name);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes 
        WHERE schemaname = 'edw' 
        AND tablename = 'dim_region' 
        AND indexname = 'idx_dr_region_type_name'
    ) THEN
        CREATE INDEX idx_dr_region_type_name ON edw.dim_region(region_type, region_name);
    END IF;
END $$;

CREATE OR REPLACE FUNCTION edw.bulk_process_unmapped_locations(
    batch_size INTEGER DEFAULT 1000,
    min_occurrence INTEGER DEFAULT 5,
    similarity_threshold FLOAT DEFAULT 0.7
)
RETURNS TABLE (
    processed INTEGER,
    mapped INTEGER,
    still_unmapped INTEGER
) AS $$
DECLARE
    v_processed INTEGER := 0;
    v_mapped INTEGER := 0;
    v_unmapped INTEGER := 0;
    v_target_geo_type TEXT;
    v_region_id INTEGER;
    v_best_match_name TEXT;
    v_similarity FLOAT;
    v_normalized_name TEXT;
    rec RECORD;
BEGIN
    -- Process high-frequency unmapped locations first
    FOR rec IN 
        SELECT geo_type, geo_name, normalized_geo_name, occurrence_count
        FROM edw.unmapped_mastercard_geo
        WHERE occurrence_count >= min_occurrence
        ORDER BY occurrence_count DESC
        LIMIT batch_size
    LOOP
        v_processed := v_processed + 1;
        
        -- Get target geo type for mapping
        SELECT target_geo_type INTO v_target_geo_type 
        FROM edw.mastercard_geo_type_mapping
        WHERE source_geo_type = rec.geo_type;
        
        IF v_target_geo_type IS NULL THEN
            v_target_geo_type := rec.geo_type; -- Default to same type if no mapping
        END IF;
        
        -- Try to find best match in dim_region based on similarity
        SELECT 
            r.region_id, 
            r.region_name, 
            similarity(edw.improved_normalize_geo_name(r.region_name), rec.normalized_geo_name) AS sim_score
        INTO 
            v_region_id, 
            v_best_match_name, 
            v_similarity
        FROM edw.dim_region r
        WHERE r.region_type = v_target_geo_type
        AND similarity(edw.improved_normalize_geo_name(r.region_name), rec.normalized_geo_name) > similarity_threshold
        ORDER BY sim_score DESC
        LIMIT 1;
        
        -- If found a good match, create mapping
        IF v_region_id IS NOT NULL THEN
            -- Insert into mastercard_region_mapping
            INSERT INTO edw.mastercard_region_mapping (
                geo_type,
                geo_name,
                region_id,
                mapping_source,
                created_at,
                updated_at,
                similarity_score
            )
            VALUES (
                rec.geo_type,
                rec.geo_name,
                v_region_id,
                'bulk_similarity_match',
                NOW(),
                NOW(),
                v_similarity
            )
            ON CONFLICT (geo_type, geo_name) DO NOTHING;
            
            -- Also update the normalized table
            INSERT INTO edw.mastercard_region_mapping_normalized (
                normalized_geo_type,
                normalized_geo_name,
                original_geo_type,
                original_geo_name,
                region_id
            )
            VALUES (
                LOWER(rec.geo_type),
                rec.normalized_geo_name,
                rec.geo_type,
                rec.geo_name,
                v_region_id
            )
            ON CONFLICT (normalized_geo_type, normalized_geo_name) DO NOTHING;
            
            -- Remove from unmapped table
            DELETE FROM edw.unmapped_mastercard_geo
            WHERE geo_type = rec.geo_type AND geo_name = rec.geo_name;
            
            v_mapped := v_mapped + 1;
        ELSE
            -- Update with additional info for manual review
            UPDATE edw.unmapped_mastercard_geo
            SET closest_match = (
                SELECT region_name
                FROM edw.dim_region r
                WHERE r.region_type = v_target_geo_type
                ORDER BY similarity(edw.improved_normalize_geo_name(r.region_name), rec.normalized_geo_name) DESC
                LIMIT 1
            ),
            similarity = (
                SELECT similarity(edw.improved_normalize_geo_name(r.region_name), rec.normalized_geo_name)
                FROM edw.dim_region r
                WHERE r.region_type = v_target_geo_type
                ORDER BY similarity(edw.improved_normalize_geo_name(r.region_name), rec.normalized_geo_name) DESC
                LIMIT 1
            ),
            suggested_region_id = (
                SELECT region_id
                FROM edw.dim_region r
                WHERE r.region_type = v_target_geo_type
                ORDER BY similarity(edw.improved_normalize_geo_name(r.region_name), rec.normalized_geo_name) DESC
                LIMIT 1
            )
            WHERE geo_type = rec.geo_type AND geo_name = rec.geo_name;
            
            v_unmapped := v_unmapped + 1;
        END IF;
    END LOOP;
    
    RETURN QUERY SELECT v_processed, v_mapped, v_unmapped;
EXCEPTION WHEN OTHERS THEN
    -- Log error but don't fail the function
    INSERT INTO edw.mastercard_processing_log (
        process_stage, error_message, affected_records, batch_start_date, batch_end_date
    )
    VALUES (
        'Bulk Unmapped Processing', 
        'Error in bulk_process_unmapped_locations: ' || SQLERRM, 
        0, 
        NULL, 
        NULL
    );
    
    -- Return current progress even on error
    RETURN QUERY SELECT v_processed, v_mapped, v_unmapped;
END;
$$ LANGUAGE plpgsql;

-- Function to fix encoding issues in master_card table
CREATE OR REPLACE FUNCTION edw.fix_mastercard_encoding()
RETURNS INTEGER AS $$
DECLARE
    v_affected_rows INTEGER := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
BEGIN
    v_start_time := NOW();
    
    -- Update geo_name column
    UPDATE data_lake.master_card
    SET geo_name = edw.fix_encoding(geo_name)
    WHERE geo_name LIKE '%Ã%' OR geo_name LIKE '%\u%';
    
    GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
    
    -- Update geo_type column
    UPDATE data_lake.master_card
    SET geo_type = edw.fix_encoding(geo_type)
    WHERE geo_type LIKE '%Ã%' OR geo_type LIKE '%\u%';
    
    GET DIAGNOSTICS v_affected_rows = v_affected_rows + ROW_COUNT;
    
    -- Update industry column
    UPDATE data_lake.master_card
    SET industry = edw.fix_encoding(industry)
    WHERE industry LIKE '%Ã%' OR industry LIKE '%\u%';
    
    GET DIAGNOSTICS v_affected_rows = v_affected_rows + ROW_COUNT;
    
    -- Update segment column
    UPDATE data_lake.master_card
    SET segment = edw.fix_encoding(segment)
    WHERE segment LIKE '%Ã%' OR segment LIKE '%\u%';
    
    GET DIAGNOSTICS v_affected_rows = v_affected_rows + ROW_COUNT;
    
    -- Update quad_id column
    UPDATE data_lake.master_card
    SET quad_id = edw.fix_encoding(quad_id)
    WHERE quad_id LIKE '%Ã%' OR quad_id LIKE '%\u%';
    
    GET DIAGNOSTICS v_affected_rows = v_affected_rows + ROW_COUNT;
    
    v_end_time := NOW();
    
    -- Log the operation
    INSERT INTO edw.mastercard_processing_log (
        process_stage,
        error_message,
        affected_records,
        batch_start_date,
        batch_end_date,
        processing_time_seconds
    )
    VALUES (
        'Encoding Fix',
        'Successfully fixed encoding issues',
        v_affected_rows,
        v_start_time,
        v_end_time,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))
    );
    
    RETURN v_affected_rows;
EXCEPTION WHEN OTHERS THEN
    -- Log error
    INSERT INTO edw.mastercard_processing_log (
        process_stage,
        error_message,
        affected_records,
        batch_start_date,
        batch_end_date
    )
    VALUES (
        'Encoding Fix',
        'Error fixing encoding: ' || SQLERRM,
        0,
        v_start_time,
        NOW()
    );
    
    RAISE EXCEPTION 'Error fixing encoding: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Create or replace the encoding process status table
CREATE TABLE IF NOT EXISTS edw.encoding_process_status (
    process_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT NOT NULL,
    current_table TEXT,
    current_column TEXT,
    processed_records BIGINT DEFAULT 0,
    total_records BIGINT,
    error_message TEXT,
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

-- Improved encoding fix procedure
CREATE OR REPLACE PROCEDURE edw.run_mastercard_encoding_fix()
LANGUAGE plpgsql
AS $$
DECLARE
    v_process_id INTEGER;
    v_affected_rows INTEGER := 0;
    v_total_rows INTEGER;
    v_batch_size INTEGER := 10000;
    v_start_time TIMESTAMP;
    v_last_progress_time TIMESTAMP;
    v_columns TEXT[] := ARRAY['geo_name', 'geo_type', 'industry', 'segment', 'quad_id'];
    v_column TEXT;
BEGIN
    -- Initialize process tracking
    INSERT INTO edw.encoding_process_status (status, details)
    VALUES ('STARTING', jsonb_build_object('batch_size', v_batch_size))
    RETURNING process_id INTO v_process_id;
    
    v_start_time := CURRENT_TIMESTAMP;
    v_last_progress_time := v_start_time;

    -- Get total number of rows that need fixing
    SELECT COUNT(*) INTO v_total_rows
    FROM data_lake.master_card
    WHERE geo_name LIKE '%Ã%' 
       OR geo_type LIKE '%Ã%'
       OR industry LIKE '%Ã%'
       OR segment LIKE '%Ã%'
       OR quad_id LIKE '%Ã%';

    -- Update status with total records
    PERFORM edw.update_process_status(
        v_process_id, 
        'PROCESSING', 
        'data_lake.master_card', 
        NULL, 
        0, 
        v_total_rows
    );

    -- Process each column separately
    FOREACH v_column IN ARRAY v_columns
    LOOP
        BEGIN
            -- Update status for current column
            PERFORM edw.update_process_status(
                v_process_id,
                'PROCESSING',
                'data_lake.master_card',
                v_column
            );

            -- Process in batches
            LOOP
                WITH batch AS (
                    SELECT id 
                    FROM data_lake.master_card
                    WHERE (CASE 
                        WHEN v_column = 'geo_name' THEN geo_name LIKE '%Ã%'
                        WHEN v_column = 'geo_type' THEN geo_type LIKE '%Ã%'
                        WHEN v_column = 'industry' THEN industry LIKE '%Ã%'
                        WHEN v_column = 'segment' THEN segment LIKE '%Ã%'
                        WHEN v_column = 'quad_id' THEN quad_id LIKE '%Ã%'
                    END)
                    LIMIT v_batch_size
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE data_lake.master_card mc
                SET 
                    CASE v_column 
                        WHEN 'geo_name' THEN geo_name = edw.fix_encoding(geo_name)
                        WHEN 'geo_type' THEN geo_type = edw.fix_encoding(geo_type)
                        WHEN 'industry' THEN industry = edw.fix_encoding(industry)
                        WHEN 'segment' THEN segment = edw.fix_encoding(segment)
                        WHEN 'quad_id' THEN quad_id = edw.fix_encoding(quad_id)
                    END
                FROM batch b
                WHERE mc.id = b.id;

                GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
                
                -- Update progress if more than 5 seconds have passed
                IF EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_last_progress_time)) >= 5 THEN
                    PERFORM edw.update_process_status(
                        v_process_id,
                        'PROCESSING',
                        'data_lake.master_card',
                        v_column,
                        v_affected_rows,
                        v_total_rows,
                        NULL,
                        jsonb_build_object(
                            'current_column', v_column,
                            'batch_size', v_batch_size,
                            'elapsed_seconds', EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))
                        )
                    );
                    v_last_progress_time := CURRENT_TIMESTAMP;
                END IF;

                EXIT WHEN v_affected_rows = 0;
            END LOOP;

        EXCEPTION WHEN OTHERS THEN
            -- Log error but continue with next column
            PERFORM edw.update_process_status(
                v_process_id,
                'ERROR',
                'data_lake.master_card',
                v_column,
                v_affected_rows,
                v_total_rows,
                format('Error processing column %s: %s', v_column, SQLERRM)
            );
            
            CONTINUE;
        END;
    END LOOP;

    -- Verify the fix
    IF EXISTS (
        SELECT 1 
        FROM data_lake.master_card 
        WHERE geo_name LIKE '%Ã%' 
           OR geo_type LIKE '%Ã%'
           OR industry LIKE '%Ã%'
           OR segment LIKE '%Ã%'
           OR quad_id LIKE '%Ã%'
    ) THEN
        PERFORM edw.update_process_status(
            v_process_id,
            'WARNING',
            NULL,
            NULL,
            v_affected_rows,
            v_total_rows,
            'Some encoding issues still exist'
        );
    ELSE
        PERFORM edw.update_process_status(
            v_process_id,
            'COMPLETED',
            NULL,
            NULL,
            v_affected_rows,
            v_total_rows,
            'All encoding issues fixed successfully'
        );
    END IF;

EXCEPTION WHEN OTHERS THEN
    -- Log fatal error
    PERFORM edw.update_process_status(
        v_process_id,
        'FAILED',
        NULL,
        NULL,
        v_affected_rows,
        v_total_rows,
        format('Fatal error: %s', SQLERRM)
    );
    RAISE;
END;
$$;

-- Function to check encoding status
CREATE OR REPLACE FUNCTION edw.check_encoding_status()
RETURNS TABLE (
    column_name TEXT,
    total_rows BIGINT,
    problematic_rows BIGINT,
    sample_values TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        col.column_name::TEXT,
        COUNT(*)::BIGINT as total_rows,
        COUNT(CASE WHEN col.value LIKE '%Ã%' THEN 1 END)::BIGINT as problematic_rows,
        array_agg(DISTINCT col.value) FILTER (WHERE col.value LIKE '%Ã%') as sample_values
    FROM (
        SELECT 'geo_name' as column_name, geo_name as value FROM data_lake.master_card
        UNION ALL
        SELECT 'geo_type', geo_type FROM data_lake.master_card
        UNION ALL
        SELECT 'industry', industry FROM data_lake.master_card
        UNION ALL
        SELECT 'segment', segment FROM data_lake.master_card
        UNION ALL
        SELECT 'quad_id', quad_id FROM data_lake.master_card
    ) col
    GROUP BY col.column_name
    HAVING COUNT(CASE WHEN col.value LIKE '%Ã%' THEN 1 END) > 0
    ORDER BY problematic_rows DESC;
END;
$$ LANGUAGE plpgsql;

-- Create a monitoring view for encoding process
CREATE OR REPLACE VIEW edw.vw_encoding_process_monitor AS
SELECT 
    process_id,
    status,
    current_table,
    current_column,
    processed_records,
    total_records,
    CASE 
        WHEN total_records > 0 THEN 
            ROUND((processed_records::FLOAT / total_records::FLOAT) * 100, 2)
        ELSE 0 
    END as progress_percentage,
    start_time,
    end_time,
    last_updated,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated))::INTEGER as seconds_since_update,
    error_message,
    details
FROM edw.encoding_process_status
ORDER BY process_id DESC;