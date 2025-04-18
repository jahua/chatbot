-- Create the fix_encoding function if it doesn't already exist
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
    key_value RECORD;
BEGIN
    IF input_text IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Start with the input
    fixed_text := input_text;
    
    -- Apply all replacements
    FOR key_value IN SELECT * FROM jsonb_each_text(encoding_map)
    LOOP
        fixed_text := replace(fixed_text, key_value.key, key_value.value);
    END LOOP;
    
    RETURN fixed_text;
EXCEPTION WHEN OTHERS THEN
    -- Log error but don't fail the function
    RAISE NOTICE 'Error in fix_encoding for input: % - %', input_text, SQLERRM;
    RETURN input_text;
END;
$$;

-- Simplified procedure to fix just the geo_name column
CREATE OR REPLACE PROCEDURE edw.fix_geo_name_encoding(
    p_batch_size INTEGER DEFAULT 1000
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_affected_rows BIGINT := 0;
    v_total_rows BIGINT;
    v_batch_count INTEGER := 0;
    v_total_processed BIGINT := 0;
    v_start_time TIMESTAMP;
    v_percentage NUMERIC(5,2);
BEGIN
    -- Print header
    RAISE NOTICE '======================================================';
    RAISE NOTICE '          MASTERCARD GEO_NAME ENCODING FIX           ';
    RAISE NOTICE '======================================================';
    
    -- Get total number of rows to fix
    SELECT COUNT(*) INTO v_total_rows
    FROM data_lake.master_card
    WHERE geo_name LIKE '%Ã%';
    
    RAISE NOTICE 'Found % rows with encoding issues in geo_name', v_total_rows;
    
    -- Exit if nothing to do
    IF v_total_rows = 0 THEN
        RAISE NOTICE 'No encoding issues found. Nothing to do.';
        RETURN;
    END IF;
    
    v_start_time := clock_timestamp();
    
    -- Process in batches
    LOOP
        v_batch_count := v_batch_count + 1;
        
        -- Process one batch
        WITH batch AS (
            SELECT id 
            FROM data_lake.master_card
            WHERE geo_name LIKE '%Ã%'
            LIMIT p_batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE data_lake.master_card mc
        SET geo_name = edw.fix_encoding(geo_name)
        FROM batch b
        WHERE mc.id = b.id;
        
        GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
        v_total_processed := v_total_processed + v_affected_rows;
        
        -- Calculate progress
        IF v_total_rows > 0 THEN
            v_percentage := ROUND((v_total_processed::NUMERIC / v_total_rows::NUMERIC) * 100, 2);
        ELSE
            v_percentage := 0;
        END IF;
        
        -- Print progress
        RAISE NOTICE 'Batch %: Processed %/% rows (%% complete)', 
            v_batch_count, v_total_processed, v_total_rows, v_percentage;
        
        -- Exit when done
        EXIT WHEN v_affected_rows = 0;
    END LOOP;
    
    -- Print summary
    RAISE NOTICE '======================================================';
    RAISE NOTICE 'Completed encoding fix for geo_name column';
    RAISE NOTICE 'Processed % rows in % batches', v_total_processed, v_batch_count;
    RAISE NOTICE 'Time elapsed: % seconds', 
        EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time))::INTEGER;
    RAISE NOTICE '======================================================';
END;
$$;
