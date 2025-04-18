-- Create staging table for Mastercard data if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.stg_mastercard_transactions (
    id SERIAL PRIMARY KEY,
    date_id INTEGER REFERENCES edw.dim_transaction_date(date_id),
    region_id INTEGER REFERENCES edw.dim_region(region_id),
    industry_id INTEGER REFERENCES edw.dim_industry(industry_id),
    txn_date DATE NOT NULL,
    txn_amt NUMERIC(15,2),
    txn_cnt INTEGER,
    acct_cnt INTEGER,
    avg_ticket NUMERIC(15,2),
    avg_freq NUMERIC(10,2),
    avg_spend_amt NUMERIC(15,2),
    yoy_txn_amt NUMERIC(15,2),
    yoy_txn_cnt INTEGER,
    quad_id VARCHAR(255),
    central_latitude NUMERIC(10,6),
    central_longitude NUMERIC(10,6),
    bounding_box JSONB,
    source_keys JSONB,
    batch_id INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(date_id, region_id, industry_id)
);

-- Drop existing indexes if they exist
DROP INDEX IF EXISTS edw.idx_stg_mc_date;
DROP INDEX IF EXISTS edw.idx_stg_mc_region;
DROP INDEX IF EXISTS edw.idx_stg_mc_industry;
DROP INDEX IF EXISTS edw.idx_stg_mc_batch;

-- Create indexes for better performance
CREATE INDEX idx_stg_mc_date ON edw.stg_mastercard_transactions(date_id);
CREATE INDEX idx_stg_mc_region ON edw.stg_mastercard_transactions(region_id);
CREATE INDEX idx_stg_mc_industry ON edw.stg_mastercard_transactions(industry_id);
CREATE INDEX idx_stg_mc_batch ON edw.stg_mastercard_transactions(batch_id);

-- Create processing log table if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.mastercard_processing_log (
    id SERIAL PRIMARY KEY,
    process_stage VARCHAR(100) NOT NULL,
    error_message TEXT,
    affected_records INTEGER,
    batch_start_date DATE,
    batch_end_date DATE,
    processing_time_seconds INTEGER,
    records_per_second NUMERIC(10,2),
    chunk_size INTEGER,
    total_chunks INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create or replace the data loading function
CREATE OR REPLACE FUNCTION edw.load_mastercard_data(
    start_date DATE, 
    end_date DATE,
    batch_size INTEGER DEFAULT 100000
)
RETURNS INTEGER AS $$
DECLARE
    v_batch_id INTEGER;
    v_affected_rows INTEGER := 0;
    v_total_rows INTEGER := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_temp_table_name TEXT := 'temp_mastercard_batch_' || (EXTRACT(EPOCH FROM NOW())::INTEGER);
    v_progress INTEGER := 0;
    v_chunk_start INTEGER := 0;
BEGIN
    -- Generate batch ID and record start time
    v_batch_id := (EXTRACT(EPOCH FROM NOW())::INTEGER);
    v_start_time := NOW();
    
    -- Validate input parameters
    IF start_date IS NULL OR end_date IS NULL THEN
        RAISE EXCEPTION 'Start date and end date cannot be NULL';
    END IF;
    
    -- Create temporary table for initial data extraction
    EXECUTE format('
        CREATE TEMPORARY TABLE %I (
            txn_date DATE,
            geo_type TEXT,
            geo_name TEXT,
            industry_id INTEGER,
            date_id INTEGER,
            txn_amt NUMERIC(15,2),
            txn_cnt INTEGER,
            acct_cnt INTEGER,
            avg_ticket NUMERIC(15,2),
            avg_freq NUMERIC(10,2),
            avg_spend_amt NUMERIC(15,2),
            yoy_txn_amt NUMERIC(15,2),
            yoy_txn_cnt INTEGER,
            quad_id TEXT,
            central_latitude NUMERIC(10,6),
            central_longitude NUMERIC(10,6),
            bounding_box JSONB,
            industry TEXT,
            segment TEXT,
            yr INTEGER
        ) ON COMMIT DROP', v_temp_table_name);
    
    -- Insert data into temp table, fixing encoding issues
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
    
    -- Get total rows to process
    EXECUTE format('SELECT COUNT(*) FROM %I', v_temp_table_name) INTO v_total_rows;
    
    RAISE NOTICE 'Processing % rows from % to % in batches of %', 
        v_total_rows, start_date, end_date, batch_size;
    
    -- Add indexes to temp table for better performance
    EXECUTE format('CREATE INDEX idx_temp_mc_geo ON %I(geo_type, geo_name)', v_temp_table_name);
    EXECUTE format('CREATE INDEX idx_temp_mc_date ON %I(txn_date)', v_temp_table_name);
    
    -- Process in batches for better memory management
    WHILE v_chunk_start < v_total_rows LOOP
        -- Insert data in chunks with our new mapping function
        EXECUTE format('
            INSERT INTO edw.stg_mastercard_transactions
            SELECT 
                t.date_id,
                edw.map_region_using_dictionary(t.geo_name, t.geo_type) AS region_id,
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
                    ''processed_at'', NOW()::text,
                    ''batch_id'', $1
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
                source_keys = EXCLUDED.source_keys,
                batch_id = EXCLUDED.batch_id,
                updated_at = NOW()', v_temp_table_name)
        USING v_batch_id, batch_size, v_chunk_start;
        
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
        v_chunk_start := v_chunk_start + batch_size;
        v_progress := (v_chunk_start::NUMERIC / v_total_rows * 100)::INTEGER;
        
        RAISE NOTICE 'Processed % of % records (%.1f%%)', 
            v_chunk_start, v_total_rows, v_progress;
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
        'Data Load', 
        'Successfully loaded data', 
        v_total_rows, 
        start_date, 
        end_date,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time)),
        CASE 
            WHEN EXTRACT(EPOCH FROM (v_end_time - v_start_time)) > 0 
            THEN v_total_rows::NUMERIC / EXTRACT(EPOCH FROM (v_end_time - v_start_time))
            ELSE 0 
        END,
        batch_size,
        CEIL(v_total_rows::NUMERIC / batch_size)
    );
    
    RETURN v_total_rows;
EXCEPTION WHEN OTHERS THEN
    -- Log error and clean up
    INSERT INTO edw.mastercard_processing_log (
        process_stage, 
        error_message, 
        affected_records, 
        batch_start_date, 
        batch_end_date
    )
    VALUES (
        'Data Load', 
        'Error: ' || SQLERRM, 
        v_affected_rows, 
        start_date, 
        end_date
    );
    
    -- Clean up temp table
    EXECUTE format('DROP TABLE IF EXISTS %I', v_temp_table_name);
    
    RAISE EXCEPTION 'Error in load_mastercard_data: %', SQLERRM;
END;
$$ LANGUAGE plpgsql; 