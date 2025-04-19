-- Final simplified function to load AOI days data into fact_visitor
CREATE OR REPLACE FUNCTION dw.load_aoi_days_data(
    p_start_date DATE,
    p_end_date DATE,
    p_batch_size INTEGER DEFAULT 100
)
RETURNS TABLE (
    status TEXT,
    records_processed INTEGER,
    records_failed INTEGER,
    execution_time_ms INTEGER,
    error_message TEXT
) AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_record RECORD;
    v_processed INTEGER := 0;
    v_failed INTEGER := 0;
    v_last_error TEXT;
    v_record_count INTEGER;
    v_batch_count INTEGER;
    v_batch_start INTEGER;
    v_date_id INTEGER;
    v_bellinzona_region_id INTEGER;
    v_avg_dwell_time NUMERIC;
    v_records_cursor REFCURSOR;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    
    -- Get the region_id for Bellinzona
    SELECT region_id INTO v_bellinzona_region_id
    FROM dw.dim_region
    WHERE region_name = 'Bellinzona'
    LIMIT 1;

    IF v_bellinzona_region_id IS NULL THEN
        RAISE EXCEPTION 'Bellinzona region not found in dim_region table';
    END IF;
    
    -- Count total records to process
    SELECT COUNT(*) INTO v_record_count
    FROM data_lake.aoi_days_raw
    WHERE aoi_date BETWEEN p_start_date AND p_end_date;

    RAISE NOTICE 'Found % records to process', v_record_count;

    -- Calculate number of batches
    v_batch_count := CEIL(v_record_count::NUMERIC / p_batch_size);

    -- Process records in batches
    FOR v_batch_start IN 0..(v_batch_count-1) LOOP
        RAISE NOTICE 'Processing batch % of %', v_batch_start + 1, v_batch_count;
        
        BEGIN
            -- Open a cursor for this batch
            OPEN v_records_cursor FOR
                SELECT * 
                FROM data_lake.aoi_days_raw
                WHERE aoi_date BETWEEN p_start_date AND p_end_date
                ORDER BY aoi_date
                LIMIT p_batch_size
                OFFSET v_batch_start * p_batch_size;

            LOOP
                FETCH v_records_cursor INTO v_record;
                EXIT WHEN NOT FOUND;

                BEGIN
                    -- Get date_id from dim_date or convert to YYYYMMDD format
                    SELECT date_id INTO v_date_id
                    FROM dw.dim_date
                    WHERE full_date = v_record.aoi_date;

                    IF v_date_id IS NULL THEN
                        v_date_id := TO_CHAR(v_record.aoi_date, 'YYYYMMDD')::INTEGER;
            END IF;
            
                    -- Calculate average dwell time
                    v_avg_dwell_time := NULL;
                    IF v_record.dwelltimes IS NOT NULL AND jsonb_array_length(v_record.dwelltimes) > 0 THEN
            SELECT 
                            SUM(visitors * mins) / NULLIF(SUM(visitors), 0)
                        INTO v_avg_dwell_time
                FROM (
                    SELECT 
                        elem::NUMERIC as visitors,
                        CASE 
                            WHEN idx = 0 THEN 15
                            WHEN idx = 1 THEN 30
                            WHEN idx = 2 THEN 60
                            WHEN idx = 3 THEN 120
                            WHEN idx = 4 THEN 180
                            WHEN idx = 5 THEN 240
                                    WHEN idx = 6 THEN 360
                            ELSE 480
                        END as mins
                        FROM jsonb_array_elements(v_record.dwelltimes) WITH ORDINALITY AS arr(elem, idx)
                        ) as dwell_data;
                    END IF;

                    -- Insert or update fact_visitor (using top_swiss_municipalities instead of top_municipalities)
                    INSERT INTO dw.fact_visitor (
                        date_id,
                        region_id,
                        source_system,
                        total_visitors,
                        swiss_tourists,
                        foreign_tourists,
                        swiss_locals,
                        swiss_commuters,
                        foreign_workers,
                        demographics,
                        dwell_time,
                        top_foreign_countries,
                        top_swiss_cantons,
                        top_municipalities,        -- Using top_swiss_municipalities for this field
                        top_last_cantons,
                        top_last_municipalities,
                        overnights_from_yesterday,
                        transaction_metrics,
                        raw_content,
                        data_quality_metrics,
                        created_at,
                        updated_at
                    ) VALUES (
                        v_date_id,
                        v_bellinzona_region_id,
                        'aoi',
                        COALESCE((v_record.visitors->>'total')::INTEGER, 0),
                        COALESCE((v_record.visitors->>'swissTourist')::INTEGER, 0),
                        COALESCE((v_record.visitors->>'foreignTourist')::INTEGER, 0),
                        COALESCE((v_record.visitors->>'swissLocal')::INTEGER, 0),
                        COALESCE((v_record.visitors->>'swissCommuter')::INTEGER, 0),
                        COALESCE((v_record.visitors->>'foreignWorker')::INTEGER, 0),
                        v_record.demographics,
                        jsonb_build_object('avg_dwell_time_mins', v_avg_dwell_time),
                        v_record.top_foreign_countries,
                        v_record.top_swiss_cantons,
                        v_record.top_swiss_municipalities,  -- Using top_swiss_municipalities instead of non-existent top_municipalities
                        v_record.top_last_cantons,
                        v_record.top_last_municipalities,
                        v_record.overnights_from_yesterday,
                        jsonb_build_object(
                            'total_transactions', COALESCE((v_record.visitors->>'total')::INTEGER, 0),
                            'avg_transaction_amount', NULL
                        ),
                        v_record.raw_content,
                        jsonb_build_object(
                            'data_completeness', 
                            CASE 
                                WHEN v_record.visitors IS NOT NULL AND v_record.demographics IS NOT NULL 
                                THEN 100 ELSE 50 
                            END,
                            'last_validation', CURRENT_TIMESTAMP
                        ),
                        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
        )
                    ON CONFLICT (date_id, region_id, source_system) 
    DO UPDATE SET
                        total_visitors = EXCLUDED.total_visitors,
                        swiss_tourists = EXCLUDED.swiss_tourists,
                        foreign_tourists = EXCLUDED.foreign_tourists,
                        swiss_locals = EXCLUDED.swiss_locals,
                        swiss_commuters = EXCLUDED.swiss_commuters,
                        foreign_workers = EXCLUDED.foreign_workers,
                        demographics = EXCLUDED.demographics,
                        dwell_time = EXCLUDED.dwell_time,
                        top_foreign_countries = EXCLUDED.top_foreign_countries,
                        top_swiss_cantons = EXCLUDED.top_swiss_cantons,
                        top_municipalities = EXCLUDED.top_municipalities,
                        top_last_cantons = EXCLUDED.top_last_cantons,
                        top_last_municipalities = EXCLUDED.top_last_municipalities,
                        overnights_from_yesterday = EXCLUDED.overnights_from_yesterday,
                        transaction_metrics = EXCLUDED.transaction_metrics,
                        raw_content = EXCLUDED.raw_content,
                        data_quality_metrics = EXCLUDED.data_quality_metrics,
                        updated_at = CURRENT_TIMESTAMP;

                    v_processed := v_processed + 1;
                    
                    -- Log progress for every 10 records
                    IF v_processed % 10 = 0 THEN
                        RAISE NOTICE 'Processed % records so far', v_processed;
    END IF;
        
                EXCEPTION
                    WHEN OTHERS THEN
                        v_failed := v_failed + 1;
                        GET STACKED DIAGNOSTICS v_last_error = MESSAGE_TEXT;
                        RAISE WARNING 'Error processing record for date %: % (%)', 
                            v_record.aoi_date, SQLERRM, SQLSTATE;
                END;
            END LOOP;

            CLOSE v_records_cursor;

            RAISE NOTICE 'Processed batch % of %, processed so far: %', 
                v_batch_start + 1, v_batch_count, v_processed;
                
        EXCEPTION
            WHEN OTHERS THEN
                -- Handle cursor errors
                IF v_records_cursor IS NOT NULL AND v_records_cursor % FOUND THEN
                    CLOSE v_records_cursor;
    END IF;
    
                v_failed := v_failed + p_batch_size;
                GET STACKED DIAGNOSTICS v_last_error = MESSAGE_TEXT;
                RAISE WARNING 'Error processing batch: % (%)', SQLERRM, SQLSTATE;
        END;
    END LOOP;
    
    v_end_time := CURRENT_TIMESTAMP;

    RETURN QUERY SELECT 
        CASE 
            WHEN v_failed = 0 THEN 'SUCCESS'
            WHEN v_processed > 0 THEN 'PARTIAL'
            ELSE 'FAILED'
        END::TEXT as status,
        v_processed::INTEGER as records_processed,
        v_failed::INTEGER as records_failed,
        EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER * 1000 as execution_time_ms,
        COALESCE(v_last_error, '')::TEXT as error_message;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION dw.load_aoi_days_data(DATE, DATE, INTEGER) IS 
'Loads AOI visitor data from data_lake.aoi_days_raw into dw.fact_visitor.
Example usage:
SELECT * FROM dw.load_aoi_days_data(''2023-01-01'', ''2023-12-31'', 50);

The function takes three parameters:
- p_start_date: Start date (inclusive)
- p_end_date: End date (inclusive)
- p_batch_size: Number of records to process in each batch

Returns a table with:
- status: SUCCESS, PARTIAL, or FAILED
- records_processed: Number of records successfully processed
- records_failed: Number of records that failed to process
- execution_time_ms: Total execution time in milliseconds
- error_message: Error message if any errors occurred';
