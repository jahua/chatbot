-- Complete encoding fix script for MasterCard geo_name column
-- Save this as encoding_fix.sql and run with:
-- PGPASSWORD=336699 psql -h 3.76.40.121 -U postgres -d trip_dw -f encoding_fix.sql | cat

-- Create or replace the fix_encoding function
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
    fixed_text TEXT;
    key_name TEXT;
    key_value TEXT;
BEGIN
    IF input_text IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Start with the input
    fixed_text := input_text;
    
    -- Apply all replacements
    FOR key_name, key_value IN SELECT * FROM jsonb_each_text(encoding_map)
    LOOP
        fixed_text := replace(fixed_text, key_name, key_value);
    END LOOP;
    
    RETURN fixed_text;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in fix_encoding for input: %', input_text;
    RETURN input_text;
END;
$$;

-- Execute the fix immediately
DO $$
DECLARE
    v_start_time TIMESTAMP;
    v_total_rows BIGINT;
    v_affected_rows BIGINT;
    v_batch_size CONSTANT INT := 5000;
    v_processed BIGINT := 0;
    v_batch_count INT := 0;
    v_elapsed_seconds INT;
BEGIN
    -- Count total affected rows
    SELECT COUNT(*) INTO v_total_rows
    FROM data_lake.master_card
    WHERE geo_name LIKE '%Ã%';
    
    RAISE NOTICE '================================================================';
    RAISE NOTICE '            MASTERCARD GEO_NAME ENCODING FIX TOOL              ';
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'Starting encoding fix for % rows in geo_name column', v_total_rows;
    RAISE NOTICE 'Batch size: % rows', v_batch_size;
    RAISE NOTICE '----------------------------------------------------------------';
    
    v_start_time := clock_timestamp();
    
    -- Process in batches until no rows left
    LOOP
        v_batch_count := v_batch_count + 1;
        
        -- Fix one batch
        WITH batch AS (
            SELECT id
            FROM data_lake.master_card
            WHERE geo_name LIKE '%Ã%'
            LIMIT v_batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE data_lake.master_card mc
        SET geo_name = edw.fix_encoding(geo_name)
        FROM batch b
        WHERE mc.id = b.id;
        
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
        v_processed := v_processed + v_affected_rows;
        
        RAISE NOTICE 'Batch %: Fixed % rows. Total progress: % of % (%.1f%%)',
            v_batch_count,
            v_affected_rows,
            v_processed,
            v_total_rows,
            (v_processed::FLOAT / NULLIF(v_total_rows, 0) * 100);
        
        EXIT WHEN v_affected_rows = 0;
        
        -- Small delay to prevent database overload
        PERFORM pg_sleep(0.1);
    END LOOP;
    
    v_elapsed_seconds := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time))::INT;
    
    RAISE NOTICE '----------------------------------------------------------------';
    RAISE NOTICE 'Completed! Fixed % rows in % batches', v_processed, v_batch_count;
    RAISE NOTICE 'Time taken: % seconds (%.2f rows/sec)', 
        v_elapsed_seconds,
        v_processed::FLOAT / NULLIF(v_elapsed_seconds, 0);
    
    -- Check if any rows still have issues
    SELECT COUNT(*) INTO v_affected_rows
    FROM data_lake.master_card
    WHERE geo_name LIKE '%Ã%';
    
    IF v_affected_rows > 0 THEN
        RAISE NOTICE 'WARNING: % rows still have encoding issues in geo_name', v_affected_rows;
    ELSE
        RAISE NOTICE 'SUCCESS: All encoding issues fixed in geo_name column';
    END IF;
    RAISE NOTICE '================================================================';
END $$;

-- Check the results (display some sample rows)
SELECT 
    'Before fix' AS stage,
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE geo_name LIKE '%Ã%') AS problematic_rows,
    ROUND((COUNT(*) FILTER (WHERE geo_name LIKE '%Ã%')::NUMERIC / NULLIF(COUNT(*)::NUMERIC, 0)) * 100, 2) AS percentage
FROM data_lake.master_card;
