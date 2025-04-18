-- MasterCard Region Dictionary Data Loader
-- This script loads data from data_lake.master_card into the dictionary mapping system

-- Step 1: First run the mapping dictionary initializer if it hasn't been run
DO $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Check how many records we already have
    SELECT COUNT(*) INTO v_count FROM edw.mastercard_region_dictionary;
    
    IF v_count = 0 THEN
        -- If empty, populate with existing mappings
        PERFORM edw.populate_region_dictionary_from_mappings();
        RAISE NOTICE 'Initialized dictionary with % records from existing mappings', v_count;
    ELSE
        RAISE NOTICE 'Dictionary already contains % records, skipping initialization', v_count;
    END IF;
END $$;

-- Step 2: Create a loader for MasterCard data specifically from the data lake
CREATE OR REPLACE PROCEDURE edw.load_mastercard_data_with_dictionary(
    p_start_date DATE DEFAULT NULL, 
    p_end_date DATE DEFAULT NULL,
    p_batch_size INTEGER DEFAULT 50000
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id INTEGER;
    v_affected_rows INTEGER := 0;
    v_total_rows INTEGER := 0;
    v_processed_rows INTEGER := 0;
    v_unmapped_rows INTEGER := 0;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_sql TEXT;
    v_temp_table_name TEXT := 'temp_mastercard_load_' || (EXTRACT(EPOCH FROM NOW())::INTEGER);
    v_progress INTEGER := 0;
    v_chunk_start INTEGER := 0;
    v_current_date DATE;
    v_date_range_clause TEXT;
BEGIN
    -- Generate batch ID and record start time
    v_batch_id := (EXTRACT(EPOCH FROM NOW())::INTEGER);
    v_start_time := NOW();
    
    -- Set date range
    IF p_start_date IS NULL THEN
        SELECT MIN(txn_date) INTO p_start_date FROM data_lake.master_card;
        RAISE NOTICE 'No start date provided, using earliest date: %', p_start_date;
    END IF;
    
    IF p_end_date IS NULL THEN
        SELECT MAX(txn_date) INTO p_end_date FROM data_lake.master_card;
        RAISE NOTICE 'No end date provided, using latest date: %', p_end_date;
    END IF;
    
    -- Validate input parameters
    IF p_start_date > p_end_date THEN
        RAISE EXCEPTION 'Start date cannot be after end date';
    END IF;
    
    RAISE NOTICE 'Loading MasterCard data from % to % in batches of %', 
        p_start_date, p_end_date, p_batch_size;
    
    -- Create working table with fixed encodings
    EXECUTE format('
        CREATE TEMPORARY TABLE %I (
            id SERIAL PRIMARY KEY,
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
            yr INTEGER,
            region_id INTEGER,
            is_mapped BOOLEAN
        ) ON COMMIT DROP', v_temp_table_name);
    
    -- Process one day at a time to manage memory
    v_current_date := p_start_date;
    
    WHILE v_current_date <= p_end_date LOOP
        RAISE NOTICE 'Processing data for date %', v_current_date;
        
        -- Extract data with fixed encoding for current date
        EXECUTE format('
            INSERT INTO %I (
                txn_date, geo_type, geo_name, industry_id, date_id,
                txn_amt, txn_cnt, acct_cnt, avg_ticket, avg_freq,
                avg_spend_amt, yoy_txn_amt, yoy_txn_cnt, quad_id,
                central_latitude, central_longitude, bounding_box,
                industry, segment, yr
            )
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
                mc.txn_date = $1', v_temp_table_name)
        USING v_current_date;
        
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
        v_total_rows := v_total_rows + v_affected_rows;
        
        -- Apply region mapping using the dictionary
        EXECUTE format('
            UPDATE %I
            SET region_id = edw.map_region_using_dictionary(geo_name, geo_type),
                is_mapped = TRUE
            WHERE region_id IS NULL', v_temp_table_name);
        
        -- Count unmapped rows for this date
        EXECUTE format('
            SELECT COUNT(*) 
            FROM %I 
            WHERE region_id IS NULL OR region_id = 1', v_temp_table_name) 
        INTO v_unmapped_rows;
        
        IF v_unmapped_rows > 0 THEN
            RAISE NOTICE '% records could not be mapped for date %', v_unmapped_rows, v_current_date;
        END IF;
        
        -- Insert data in chunks to target table
        v_chunk_start := 0;
        
        LOOP
            -- Insert data in chunks
            EXECUTE format('
                INSERT INTO edw.stg_mastercard_transactions
                SELECT 
                    t.date_id,
                    t.region_id,
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
                        ''batch_id'', $1,
                        ''is_mapped'', t.is_mapped
                    ) AS source_keys,
                    $1
                FROM 
                    %I t
                WHERE
                    t.region_id IS NOT NULL
                ORDER BY t.id
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
                    batch_id = EXCLUDED.batch_id', v_temp_table_name)
            USING v_batch_id, p_batch_size, v_chunk_start;
            
            GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
            v_processed_rows := v_processed_rows + v_affected_rows;
            
            EXIT WHEN v_affected_rows = 0;
            v_chunk_start := v_chunk_start + p_batch_size;
        END LOOP;
        
        -- Clear the temp table for next date
        EXECUTE format('TRUNCATE TABLE %I', v_temp_table_name);
        
        -- Move to next date
        v_current_date := v_current_date + INTERVAL '1 day';
    END LOOP;
    
    v_end_time := NOW();
    
    -- Log completion
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
        'Dictionary-Based Data Load', 
        format('Successfully loaded %s of %s records (%.2f%%) with dictionary-based mapping', 
            v_processed_rows, v_total_rows, 
            CASE WHEN v_total_rows > 0 THEN (v_processed_rows::NUMERIC / v_total_rows * 100) ELSE 0 END), 
        v_processed_rows, 
        p_start_date, 
        p_end_date,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time)),
        CASE 
            WHEN EXTRACT(EPOCH FROM (v_end_time - v_start_time)) > 0 
            THEN v_processed_rows::NUMERIC / EXTRACT(EPOCH FROM (v_end_time - v_start_time))
            ELSE 0 
        END,
        p_batch_size,
        CEIL(v_total_rows::NUMERIC / p_batch_size)
    );
    
    -- Update dictionary with any new mappings from unmapped geo locations
    CALL edw.auto_approve_high_confidence_mappings(0.9, 2);
    
    -- Final report
    RAISE NOTICE 'Load complete: % records processed, % loaded successfully', 
        v_total_rows, v_processed_rows;
    RAISE NOTICE 'Processing time: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time));
    
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
        'Dictionary-Based Data Load', 
        'Error: ' || SQLERRM, 
        v_processed_rows, 
        p_start_date, 
        p_end_date
    );
    
    -- Clean up temp table
    EXECUTE format('DROP TABLE IF EXISTS %I', v_temp_table_name);
    
    RAISE EXCEPTION 'Error in load_mastercard_data_with_dictionary: %', SQLERRM;
END;
$$;

-- Step 3: Create a procedure to update the dictionary with mappings for the most frequent unmapped locations
CREATE OR REPLACE PROCEDURE edw.populate_unmapped_locations_with_suggestions() 
LANGUAGE plpgsql
AS $$
DECLARE
    unmapped_record RECORD;
    source_geo_type TEXT;
    target_geo_type TEXT;
    v_count INTEGER := 0;
BEGIN
    -- Find most frequent unmapped locations
    FOR unmapped_record IN
        SELECT 
            geo_type, 
            geo_name, 
            normalized_geo_name,
            occurrence_count
        FROM 
            edw.unmapped_mastercard_geo
        WHERE 
            occurrence_count > 5
        ORDER BY 
            occurrence_count DESC
        LIMIT 500
    LOOP
        -- Get target geo type
        SELECT target_geo_type INTO target_geo_type 
        FROM edw.mastercard_geo_type_mapping
        WHERE source_geo_type = unmapped_record.geo_type;
        
        IF target_geo_type IS NULL THEN
            target_geo_type := unmapped_record.geo_type;
        END IF;
        
        -- Try to find the best match
        UPDATE edw.unmapped_mastercard_geo
        SET 
            suggested_region_id = subquery.region_id,
            closest_match = subquery.region_name,
            similarity = subquery.similarity
        FROM (
            SELECT 
                r.region_id,
                r.region_name,
                similarity(edw.improved_normalize_geo_name(r.region_name), unmapped_record.normalized_geo_name) AS similarity
            FROM 
                edw.dim_region r
            WHERE 
                r.region_type = target_geo_type
            ORDER BY 
                similarity DESC
            LIMIT 1
        ) AS subquery
        WHERE 
            geo_type = unmapped_record.geo_type 
            AND geo_name = unmapped_record.geo_name;
            
        v_count := v_count + 1;
    END LOOP;
    
    RAISE NOTICE 'Populated suggestions for % unmapped locations', v_count;
END;
$$;

-- Step 4: Create a data audit procedure
CREATE OR REPLACE PROCEDURE edw.audit_mastercard_data_mapping() 
LANGUAGE plpgsql
AS $$
DECLARE
    total_records BIGINT;
    mapped_records BIGINT;
    default_region_records BIGINT;
    unmapped_records BIGINT;
    distinct_locations BIGINT;
    mapped_locations BIGINT;
    error_prone_locations INTEGER;
BEGIN
    -- Get overall stats
    SELECT COUNT(*) INTO total_records FROM data_lake.master_card;
    
    -- Count mapped records
    WITH mapping_check AS (
        SELECT 
            mc.geo_type, 
            mc.geo_name,
            mrd.region_id,
            COUNT(*) as record_count
        FROM 
            data_lake.master_card mc
        LEFT JOIN 
            edw.mastercard_region_dictionary mrd 
                ON edw.fix_encoding(mc.geo_type) = mrd.geo_type 
                AND edw.fix_encoding(mc.geo_name) = mrd.source_name
        GROUP BY 
            mc.geo_type, mc.geo_name, mrd.region_id
    )
    SELECT 
        COUNT(*) FILTER (WHERE region_id IS NOT NULL),
        COUNT(*) FILTER (WHERE region_id = 1),
        COUNT(*) FILTER (WHERE region_id IS NULL)
    INTO 
        mapped_records, default_region_records, unmapped_records
    FROM 
        mapping_check;
    
    -- Count distinct locations
    SELECT 
        COUNT(DISTINCT geo_type || '_' || geo_name),
        COUNT(DISTINCT mrd.id)
    INTO 
        distinct_locations, mapped_locations
    FROM 
        data_lake.master_card mc
    LEFT JOIN 
        edw.mastercard_region_dictionary mrd 
            ON edw.fix_encoding(mc.geo_type) = mrd.geo_type 
            AND edw.fix_encoding(mc.geo_name) = mrd.source_name;
    
    -- Find locations with potential encoding issues
    SELECT COUNT(*) INTO error_prone_locations
    FROM (
        SELECT geo_name, COUNT(DISTINCT edw.fix_encoding(geo_name)) AS encoding_variants
        FROM data_lake.master_card
        GROUP BY geo_name
        HAVING COUNT(DISTINCT edw.fix_encoding(geo_name)) > 1
    ) AS encoding_issues;
    
    -- Display audit results
    RAISE NOTICE 'MasterCard Data Mapping Audit';
    RAISE NOTICE '-----------------------------';
    RAISE NOTICE 'Total Records: %', total_records;
    RAISE NOTICE 'Mapped Records: % (%.2f%%)', 
        mapped_records, 
        CASE WHEN total_records > 0 THEN mapped_records::NUMERIC / total_records * 100 ELSE 0 END;
    RAISE NOTICE 'Default Region Records: % (%.2f%%)', 
        default_region_records, 
        CASE WHEN total_records > 0 THEN default_region_records::NUMERIC / total_records * 100 ELSE 0 END;
    RAISE NOTICE 'Unmapped Records: % (%.2f%%)', 
        unmapped_records, 
        CASE WHEN total_records > 0 THEN unmapped_records::NUMERIC / total_records * 100 ELSE 0 END;
    RAISE NOTICE 'Distinct Locations: %', distinct_locations;
    RAISE NOTICE 'Mapped Locations: % (%.2f%%)', 
        mapped_locations, 
        CASE WHEN distinct_locations > 0 THEN mapped_locations::NUMERIC / distinct_locations * 100 ELSE 0 END;
    RAISE NOTICE 'Locations with Encoding Issues: %', error_prone_locations;
    RAISE NOTICE '-----------------------------';
    
    -- Store audit results
    INSERT INTO edw.mastercard_processing_log (
        process_stage, 
        error_message, 
        affected_records, 
        batch_start_date, 
        batch_end_date,
        processing_time_seconds,
        details
    )
    VALUES (
        'Mapping Audit', 
        'Completed data mapping audit', 
        total_records, 
        NULL, 
        NULL,
        0,
        jsonb_build_object(
            'total_records', total_records,
            'mapped_records', mapped_records,
            'mapped_percentage', CASE WHEN total_records > 0 THEN ROUND(mapped_records::NUMERIC / total_records * 100, 2) ELSE 0 END,
            'default_region_records', default_region_records,
            'unmapped_records', unmapped_records,
            'distinct_locations', distinct_locations,
            'mapped_locations', mapped_locations,
            'locations_mapped_percentage', CASE WHEN distinct_locations > 0 THEN ROUND(mapped_locations::NUMERIC / distinct_locations * 100, 2) ELSE 0 END,
            'encoding_issue_locations', error_prone_locations
        )
    );
END;
$$;

-- Step 5: Create script to load a specified date range
DO $$
DECLARE
    v_start_date DATE := '2023-01-01';  -- Replace with your actual start date
    v_end_date DATE := '2023-01-31';    -- Replace with your actual end date
    v_batch_size INTEGER := 50000;      -- Adjust batch size based on your system resources
BEGIN
    -- First populate the dictionary with existing mappings
    PERFORM edw.populate_region_dictionary_from_mappings();
    
    -- Generate suggestions for unmapped locations
    CALL edw.populate_unmapped_locations_with_suggestions();
    
    -- Auto-approve high-confidence matches
    CALL edw.auto_approve_high_confidence_mappings(0.9, 5);
    
    -- Load the data for the specified date range
    CALL edw.load_mastercard_data_with_dictionary(v_start_date, v_end_date, v_batch_size);
    
    -- Run an audit to verify mapping quality
    CALL edw.audit_mastercard_data_mapping();
    
    -- Report completion
    RAISE NOTICE 'MasterCard data load completed successfully for date range % to %', 
        v_start_date, v_end_date;
END $$;

-- Step 6: Add additional dictionary entries for specific problematic regions
DO $$
DECLARE
    additional_entries TEXT[][] := ARRAY[
        -- Additional mappings for problematic regions
        -- FORMAT: [source_name, standard_name, geo_type, region_type]
        ARRAY['Basel-Land', 'Basel-Landschaft', 'State', 'Canton'],
        ARRAY['Basel-Landschaft', 'Basel-Landschaft', 'State', 'Canton'],
        ARRAY['St. Gallen', 'St. Gallen', 'State', 'Canton'],
        ARRAY['Sankt Gallen', 'St. Gallen', 'State', 'Canton'],
        ARRAY['St Gallen', 'St. Gallen', 'State', 'Canton'],
        ARRAY['St.Gallen', 'St. Gallen', 'State', 'Canton'],
        ARRAY['Geneva', 'Genève', 'State', 'Canton'],
        ARRAY['Genf', 'Genève', 'State', 'Canton'],
        ARRAY['Ginevra', 'Genève', 'State', 'Canton'],
        ARRAY['Berne', 'Bern', 'State', 'Canton'],
        ARRAY['Berna', 'Bern', 'State', 'Canton'],
        ARRAY['Zurich', 'Zürich', 'State', 'Canton'],
        ARRAY['Zurigo', 'Zürich', 'State', 'Canton'],
        ARRAY['Lucerne', 'Luzern', 'State', 'Canton'],
        ARRAY['Lucerna', 'Luzern', 'State', 'Canton'],
        ARRAY['Grisons', 'Graubünden', 'State', 'Canton'],
        ARRAY['Grigioni', 'Graubünden', 'State', 'Canton'],
        ARRAY['Neuchatel', 'Neuchâtel', 'State', 'Canton'],
        ARRAY['Fribourg', 'Fribourg', 'State', 'Canton'],
        ARRAY['Friburgo', 'Fribourg', 'State', 'Canton'],
        ARRAY['Freiburg', 'Fribourg', 'State', 'Canton'],
        ARRAY['Valais', 'Valais', 'State', 'Canton'],
        ARRAY['Vallese', 'Valais', 'State', 'Canton'],
        ARRAY['Wallis', 'Valais', 'State', 'Canton'],
        ARRAY['Ticino', 'Ticino', 'State', 'Canton'],
        ARRAY['Tessin', 'Ticino', 'State', 'Canton'],
        ARRAY['Vaud', 'Vaud', 'State', 'Canton'],
        ARRAY['Waadt', 'Vaud', 'State', 'Canton']
    ];
    region_id INTEGER;
    entry TEXT[];
BEGIN
    -- Process each additional entry
    FOREACH entry SLICE 1 IN ARRAY additional_entries
    LOOP
        -- Get region_id from dim_region
        SELECT r.region_id INTO region_id
        FROM edw.dim_region r
        WHERE r.region_type = entry[4] 
          AND (r.region_name = entry[2] OR 
               edw.improved_normalize_geo_name(r.region_name) = edw.improved_normalize_geo_name(entry[2]));
        
        IF region_id IS NULL THEN
            -- Try to find it by similarity
            SELECT r.region_id INTO region_id
            FROM edw.dim_region r
            WHERE r.region_type = entry[4]
              AND similarity(edw.improved_normalize_geo_name(r.region_name), 
                           edw.improved_normalize_geo_name(entry[2])) > 0.8
            ORDER BY similarity(edw.improved_normalize_geo_name(r.region_name), 
                              edw.improved_normalize_geo_name(entry[2])) DESC
            LIMIT 1;
        END IF;
        
        IF region_id IS NULL THEN
            RAISE NOTICE 'Could not find region_id for %', entry[2];
            CONTINUE;
        END IF;
        
        -- Insert mapping
        INSERT INTO edw.mastercard_region_dictionary (
            source_name, standard_name, region_id, geo_type, region_type
        ) VALUES
        (entry[1], entry[2], region_id, entry[3], entry[4])
        ON CONFLICT (source_name, geo_type) DO NOTHING;
    END LOOP;
    
    RAISE NOTICE 'Added additional dictionary entries for problematic regions';
END $$;

-- Step 7: Create a maintenance procedure to keep the dictionary up-to-date
CREATE OR REPLACE PROCEDURE edw.maintain_region_dictionary(
    similarity_threshold FLOAT DEFAULT 0.7
)
LANGUAGE plpgsql
AS $$
DECLARE
    new_mappings_count INTEGER := 0;
    fixed_mappings_count INTEGER := 0;
BEGIN
    -- First run auto-approval of high confidence mappings
    CALL edw.auto_approve_high_confidence_mappings(0.9, 5);
    
    -- Find any new geo_name/geo_type combinations that aren't in the dictionary
    INSERT INTO edw.mastercard_region_dictionary (
        source_name,
        standard_name,
        region_id,
        geo_type,
        region_type
    )
    WITH new_locations AS (
        SELECT DISTINCT 
            edw.fix_encoding(mc.geo_type) AS geo_type,
            edw.fix_encoding(mc.geo_name) AS geo_name
        FROM 
            data_lake.master_card mc
        LEFT JOIN 
            edw.mastercard_region_dictionary mrd 
                ON edw.fix_encoding(mc.geo_type) = mrd.geo_type 
                AND edw.fix_encoding(mc.geo_name) = mrd.source_name
        WHERE 
            mrd.id IS NULL
    ),
    normalized_locations AS (
        SELECT 
            nl.geo_type,
            nl.geo_name,
            edw.improved_normalize_geo_name(nl.geo_name) AS normalized_name
        FROM 
            new_locations nl
    ),
    normalized_matches AS (
        SELECT 
            nl.geo_type,
            nl.geo_name,
            mrd.region_id,
            mrd.standard_name,
            mrd.region_type
        FROM 
            normalized_locations nl
        JOIN 
            edw.mastercard_region_dictionary mrd
                ON edw.improved_normalize_geo_name(mrd.source_name) = nl.normalized_name
                AND nl.geo_type = mrd.geo_type
    )
    SELECT 
        nm.geo_name,
        nm.standard_name,
        nm.region_id,
        nm.geo_type,
        nm.region_type
    FROM 
        normalized_matches nm
    ON CONFLICT (source_name, geo_type) DO NOTHING;
    
    GET DIAGNOSTICS new_mappings_count = ROW_COUNT;
    
    -- Fix any inconsistent mappings
    WITH inconsistent_mappings AS (
        SELECT 
            source_name,
            geo_type,
            array_agg(region_id) AS region_ids,
            array_agg(standard_name) AS standard_names
        FROM 
            edw.mastercard_region_dictionary
        GROUP BY 
            source_name, geo_type
        HAVING 
            COUNT(DISTINCT region_id) > 1
    ),
    preferred_mapping AS (
        SELECT 
            im.source_name,
            im.geo_type,
            (SELECT mrd.region_id
             FROM edw.mastercard_region_dictionary mrd
             WHERE mrd.source_name = im.source_name AND mrd.geo_type = im.geo_type
             ORDER BY mrd.created_at DESC
             LIMIT 1) AS preferred_region_id
        FROM 
            inconsistent_mappings im
    )
    DELETE FROM edw.mastercard_region_dictionary mrd
    USING preferred_mapping pm
    WHERE mrd.source_name = pm.source_name
      AND mrd.geo_type = pm.geo_type
      AND mrd.region_id != pm.preferred_region_id;
    
    GET DIAGNOSTICS fixed_mappings_count = ROW_COUNT;
    
    RAISE NOTICE 'Dictionary maintenance complete: % new mappings added, % inconsistent mappings fixed',
        new_mappings_count, fixed_mappings_count;
END;
$$;

-- Execute maintenance to ensure the dictionary is up-to-date
CALL edw.maintain_region_dictionary();