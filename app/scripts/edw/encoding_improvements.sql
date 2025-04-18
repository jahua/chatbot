-- Database-wide encoding improvements
-- Part 1: Audit and Configuration

-- Function to validate UTF-8 encoding
CREATE OR REPLACE FUNCTION edw.is_valid_utf8(text_to_check TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN text_to_check IS NOT NULL AND 
           convert_from(convert_to(text_to_check, 'UTF8'), 'UTF8') = text_to_check;
END;
$$ LANGUAGE plpgsql;

-- Function to audit encoding issues across all tables
CREATE OR REPLACE FUNCTION edw.audit_encoding_issues()
RETURNS TABLE (
    schema_name TEXT,
    table_name TEXT,
    column_name TEXT,
    data_type TEXT,
    issue_count BIGINT,
    sample_value TEXT,
    is_valid_utf8 BOOLEAN
) AS $$
DECLARE
    v_query TEXT;
BEGIN
    -- Create temporary table to store results
    CREATE TEMPORARY TABLE temp_encoding_audit (
        schema_name TEXT,
        table_name TEXT,
        column_name TEXT,
        data_type TEXT,
        issue_count BIGINT,
        sample_value TEXT,
        is_valid_utf8 BOOLEAN
    );

    -- Build and execute dynamic query to check all text columns
    FOR v_query IN 
        SELECT format(
            'INSERT INTO temp_encoding_audit 
            SELECT 
                %L as schema_name,
                %L as table_name,
                %L as column_name,
                %L as data_type,
                COUNT(*) as issue_count,
                MAX(%I) as sample_value,
                edw.is_valid_utf8(MAX(%I)) as is_valid_utf8
            FROM %I.%I 
            WHERE %I LIKE %L OR %I LIKE %L',
            table_schema,
            table_name,
            column_name,
            data_type,
            column_name,
            column_name,
            table_schema,
            table_name,
            column_name,
            '%Ã%',
            column_name,
            '%\u%'
        )
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND data_type IN ('character varying', 'text', 'character')
        AND table_schema = 'data_lake'  -- Focus on data_lake schema first
    LOOP
        EXECUTE v_query;
    END LOOP;

    -- Return results
    RETURN QUERY SELECT * FROM temp_encoding_audit WHERE issue_count > 0 ORDER BY issue_count DESC;
    
    -- Clean up
    DROP TABLE temp_encoding_audit;
END;
$$ LANGUAGE plpgsql;

-- Function to create backup of data with encoding issues
CREATE OR REPLACE FUNCTION edw.create_encoding_backup(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_column_name TEXT
)
RETURNS VOID AS $$
DECLARE
    v_backup_table TEXT;
BEGIN
    v_backup_table := format('%s_%s_encoding_backup_%s', 
        p_table_name, 
        p_column_name,
        to_char(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS')
    );
    
    EXECUTE format(
        'CREATE TABLE %I.%I AS 
         SELECT * FROM %I.%I 
         WHERE %I LIKE %L OR %I LIKE %L',
        p_schema_name,
        v_backup_table,
        p_schema_name,
        p_table_name,
        p_column_name,
        '%Ã%',
        p_column_name,
        '%\u%'
    );
    
    -- Log backup creation
    INSERT INTO edw.mastercard_processing_log (
        process_stage,
        error_message,
        affected_records,
        batch_start_date,
        batch_end_date
    )
    VALUES (
        'Encoding Backup',
        format('Created backup table %s.%s', p_schema_name, v_backup_table),
        0,
        NOW(),
        NOW()
    );
END;
$$ LANGUAGE plpgsql;

-- Function to fix encoding issues in batches with improved resource management
CREATE OR REPLACE FUNCTION edw.fix_encoding_batch(
    p_schema_name TEXT,
    p_table_name TEXT,
    p_column_name TEXT,
    p_batch_size INTEGER DEFAULT 10000,
    p_max_runtime INTEGER DEFAULT 3600,
    p_throttle_delay FLOAT DEFAULT 0.1
)
RETURNS INTEGER AS $$
DECLARE
    v_total_affected INTEGER := 0;
    v_batch_affected INTEGER;
    v_continue BOOLEAN := TRUE;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_original_value TEXT;
    v_fixed_value TEXT;
BEGIN
    v_start_time := NOW();
    
    -- Create backup before processing
    PERFORM edw.create_encoding_backup(p_schema_name, p_table_name, p_column_name);
    
    WHILE v_continue AND EXTRACT(EPOCH FROM (NOW() - v_start_time)) < p_max_runtime LOOP
        BEGIN
            -- Get sample of original and fixed values
            EXECUTE format(
                'SELECT %I, edw.fix_encoding(%I) 
                 FROM %I.%I 
                 WHERE %I LIKE %L OR %I LIKE %L 
                 LIMIT 1',
                p_column_name,
                p_column_name,
                p_schema_name,
                p_table_name,
                p_column_name,
                '%Ã%',
                p_column_name,
                '%\u%'
            ) INTO v_original_value, v_fixed_value;
            
            -- Process batch
            EXECUTE format(
                'WITH batch AS (
                    SELECT ctid
                    FROM %I.%I
                    WHERE %I LIKE %L OR %I LIKE %L
                    LIMIT %s
                )
                UPDATE %I.%I t
                SET %I = edw.fix_encoding(t.%I)
                FROM batch b
                WHERE t.ctid = b.ctid
                RETURNING 1',
                p_schema_name,
                p_table_name,
                p_column_name,
                '%Ã%',
                p_column_name,
                '%\u%',
                p_batch_size,
                p_schema_name,
                p_table_name,
                p_column_name,
                p_column_name
            );
            
            GET DIAGNOSTICS v_batch_affected = ROW_COUNT;
            v_total_affected := v_total_affected + v_batch_affected;
            
            -- Log progress with before/after examples
            INSERT INTO edw.mastercard_processing_log (
                process_stage,
                error_message,
                affected_records,
                batch_start_date,
                batch_end_date,
                processing_time_seconds,
                additional_info
            )
            VALUES (
                'Encoding Fix Batch',
                format('Processed batch for %s.%s.%s', p_schema_name, p_table_name, p_column_name),
                v_batch_affected,
                v_start_time,
                NOW(),
                EXTRACT(EPOCH FROM (NOW() - v_start_time)),
                jsonb_build_object(
                    'original_value', v_original_value,
                    'fixed_value', v_fixed_value
                )
            );
            
            -- Check if we should continue
            v_continue := v_batch_affected > 0;
            
            -- Add throttling delay
            PERFORM pg_sleep(p_throttle_delay);
            
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
                'Encoding Fix Error',
                format('Error in batch processing: %s', SQLERRM),
                0,
                v_start_time,
                NOW()
            );
            
            RAISE WARNING 'Error in batch processing: %', SQLERRM;
            v_continue := FALSE;
        END;
    END LOOP;
    
    RETURN v_total_affected;
END;
$$ LANGUAGE plpgsql;

-- Trigger function to prevent bad encoding
CREATE OR REPLACE FUNCTION edw.prevent_bad_encoding()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.geo_name LIKE '%Ã%' OR NEW.geo_name LIKE '%\u%' THEN
        NEW.geo_name := edw.fix_encoding(NEW.geo_name);
    END IF;
    
    IF NEW.geo_type LIKE '%Ã%' OR NEW.geo_type LIKE '%\u%' THEN
        NEW.geo_type := edw.fix_encoding(NEW.geo_type);
    END IF;
    
    IF NEW.industry LIKE '%Ã%' OR NEW.industry LIKE '%\u%' THEN
        NEW.industry := edw.fix_encoding(NEW.industry);
    END IF;
    
    IF NEW.segment LIKE '%Ã%' OR NEW.segment LIKE '%\u%' THEN
        NEW.segment := edw.fix_encoding(NEW.segment);
    END IF;
    
    IF NEW.quad_id LIKE '%Ã%' OR NEW.quad_id LIKE '%\u%' THEN
        NEW.quad_id := edw.fix_encoding(NEW.quad_id);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to create optimized indexes for encoding issues
CREATE OR REPLACE FUNCTION edw.create_encoding_indexes()
RETURNS VOID AS $$
DECLARE
    v_query TEXT;
BEGIN
    -- Create indexes for known problematic tables
    FOR v_query IN 
        SELECT format(
            'CREATE INDEX IF NOT EXISTS idx_%s_%s_encoding 
            ON %I.%I(%I) 
            WHERE %I LIKE %L OR %I LIKE %L',
            table_name,
            column_name,
            table_schema,
            table_name,
            column_name,
            column_name,
            '%Ã%',
            column_name,
            '%\u%'
        )
        FROM information_schema.columns
        WHERE table_schema = 'data_lake'
        AND table_name = 'master_card'
        AND column_name IN ('geo_name', 'geo_type', 'industry', 'segment', 'quad_id')
    LOOP
        EXECUTE v_query;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Add status tracking table
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
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Function to update process status
CREATE OR REPLACE FUNCTION edw.update_process_status(
    p_process_id INTEGER,
    p_status TEXT,
    p_current_table TEXT DEFAULT NULL,
    p_current_column TEXT DEFAULT NULL,
    p_processed_records BIGINT DEFAULT NULL,
    p_total_records BIGINT DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    UPDATE edw.encoding_process_status
    SET status = p_status,
        current_table = p_current_table,
        current_column = p_current_column,
        processed_records = COALESCE(p_processed_records, processed_records),
        total_records = COALESCE(p_total_records, total_records),
        error_message = p_error_message,
        last_updated = CURRENT_TIMESTAMP,
        end_time = CASE WHEN p_status = 'COMPLETED' OR p_status = 'FAILED' THEN CURRENT_TIMESTAMP ELSE NULL END
    WHERE process_id = p_process_id;
END;
$$ LANGUAGE plpgsql;

-- Modified main procedure with status tracking
CREATE OR REPLACE PROCEDURE edw.run_encoding_improvements()
LANGUAGE plpgsql
AS $$
DECLARE
    v_process_id INTEGER;
    v_audit RECORD;
    v_affected INTEGER;
    v_total INTEGER;
    v_start_time TIMESTAMP;
    v_last_progress_time TIMESTAMP;
BEGIN
    -- Initialize process tracking
    INSERT INTO edw.encoding_process_status (status)
    VALUES ('STARTING')
    RETURNING process_id INTO v_process_id;
    
    v_start_time := CURRENT_TIMESTAMP;
    v_last_progress_time := v_start_time;
    
    -- Set client encoding
    EXECUTE 'SET client_encoding = ''UTF8''';
    PERFORM edw.update_process_status(v_process_id, 'SETTING_ENCODING');
    
    -- Create indexes
    PERFORM edw.create_encoding_indexes();
    PERFORM edw.update_process_status(v_process_id, 'CREATING_INDEXES');
    
    -- Create prevention trigger
    DROP TRIGGER IF EXISTS tr_prevent_bad_encoding ON data_lake.master_card;
    CREATE TRIGGER tr_prevent_bad_encoding
    BEFORE INSERT OR UPDATE ON data_lake.master_card
    FOR EACH ROW EXECUTE FUNCTION edw.prevent_bad_encoding();
    
    PERFORM edw.update_process_status(v_process_id, 'RUNNING_AUDIT');
    
    -- Run audit and fix issues
    FOR v_audit IN 
        SELECT * FROM edw.audit_encoding_issues()
    LOOP
        -- Update status for current table/column
        PERFORM edw.update_process_status(
            v_process_id,
            'PROCESSING',
            v_audit.schema_name || '.' || v_audit.table_name,
            v_audit.column_name,
            0,
            v_audit.issue_count
        );
        
        RAISE NOTICE 'Processing % %.%: % issues found (Valid UTF-8: %)',
            v_audit.schema_name,
            v_audit.table_name,
            v_audit.column_name,
            v_audit.issue_count,
            v_audit.is_valid_utf8;
        
        -- Process in batches with progress updates
        v_total := 0;
        LOOP
            -- Process batch
            EXECUTE format(
                'WITH batch AS (
                    SELECT id FROM %I.%I
                    WHERE %I LIKE %L OR %I LIKE %L
                    LIMIT 1000
                )
                UPDATE %I.%I t
                SET %I = edw.fix_encoding(t.%I)
                FROM batch b
                WHERE t.id = b.id
                RETURNING 1',
                v_audit.schema_name,
                v_audit.table_name,
                v_audit.column_name,
                '%Ã%',
                v_audit.column_name,
                '%\u%',
                v_audit.schema_name,
                v_audit.table_name,
                v_audit.column_name,
                v_audit.column_name
            );
            
            GET DIAGNOSTICS v_affected = ROW_COUNT;
            v_total := v_total + v_affected;
            
            -- Update progress every 5 seconds
            IF EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_last_progress_time)) >= 5 THEN
                PERFORM edw.update_process_status(
                    v_process_id,
                    'PROCESSING',
                    v_audit.schema_name || '.' || v_audit.table_name,
                    v_audit.column_name,
                    v_total,
                    v_audit.issue_count
                );
                v_last_progress_time := CURRENT_TIMESTAMP;
                RAISE NOTICE 'Progress: %/% records processed for % %.%',
                    v_total,
                    v_audit.issue_count,
                    v_audit.schema_name,
                    v_audit.table_name,
                    v_audit.column_name;
            END IF;
            
            EXIT WHEN v_affected = 0;
        END LOOP;
        
        PERFORM edw.update_process_status(
            v_process_id,
            'COMPLETED_COLUMN',
            v_audit.schema_name || '.' || v_audit.table_name,
            v_audit.column_name,
            v_total,
            v_audit.issue_count
        );
    END LOOP;
    
    -- Final verification
    IF EXISTS (
        SELECT 1 FROM edw.audit_encoding_issues() LIMIT 1
    ) THEN
        PERFORM edw.update_process_status(
            v_process_id,
            'WARNING',
            NULL,
            NULL,
            v_total,
            NULL,
            'Some encoding issues may still exist'
        );
        RAISE WARNING 'Some encoding issues may still exist. Please check the audit results.';
    ELSE
        PERFORM edw.update_process_status(
            v_process_id,
            'COMPLETED',
            NULL,
            NULL,
            v_total,
            NULL,
            'All encoding issues have been fixed successfully'
        );
        RAISE NOTICE 'All encoding issues have been fixed successfully.';
    END IF;
END;
$$; 