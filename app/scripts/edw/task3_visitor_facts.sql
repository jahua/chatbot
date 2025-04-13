-- Task 3: Process Tourism Visitor Facts
-- Drop the existing table or view if it exists
DO $$
BEGIN
    PERFORM edw.report_status('Task 3: Starting visitor facts processing');
    
    -- Check if the object exists as a table or view and drop it if requested
    IF EXISTS (SELECT 1 FROM information_schema.tables 
              WHERE table_schema = 'edw' AND table_name = 'fact_tourism_visitors' 
              AND table_type = 'BASE TABLE') THEN
        PERFORM edw.report_status('Dropping existing fact_tourism_visitors table');
        DROP TABLE IF EXISTS edw.fact_tourism_visitors;
    ELSIF EXISTS (SELECT 1 FROM information_schema.views 
                 WHERE table_schema = 'edw' AND table_name = 'fact_tourism_visitors') THEN
        PERFORM edw.report_status('Dropping existing fact_tourism_visitors view');
        DROP VIEW IF EXISTS edw.fact_tourism_visitors;
    END IF;
END;
$$;

-- Process Visitor fact table with dynamic column handling
DO $$
DECLARE
    start_time TIMESTAMP;
    temp_count INTEGER;
    final_count INTEGER;
    column_list TEXT;
    core_columns TEXT;
    demographic_columns TEXT;
    temp_demographic_columns TEXT;
    transport_columns TEXT;
    temp_transport_columns TEXT;
    education_columns TEXT;
    temp_education_columns TEXT;
    dimension_columns TEXT;
    col_record RECORD;
BEGIN
    -- Skip if already exists (check for both table and view)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'edw' AND table_name = 'fact_tourism_visitors'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.views
        WHERE table_schema = 'edw' AND table_name = 'fact_tourism_visitors'
    )
    THEN
        start_time := clock_timestamp();
        PERFORM edw.report_status('Creating fact_tourism_visitors table');
        
        -- First get a list of all actual columns in the fact_tourism table
        PERFORM edw.report_status('Getting column list from inervista.fact_tourism');
        SELECT string_agg(column_name, ', ') INTO column_list
        FROM information_schema.columns
        WHERE table_schema = 'inervista' AND table_name = 'fact_tourism';
        
        PERFORM edw.report_status('Available columns: ' || column_list);
        
        -- Core dimensional columns - required
        core_columns := 'ft.fact_id, ft.date_id, ft.region_id, ft.object_type_id, 
                        ft.visit_type_id, ft.data_type_id, ft.total AS total_visitors_structured';
        
        -- Initialize other column categories
        demographic_columns := '';
        transport_columns := '';
        education_columns := '';
        
        -- Check for demographic columns
        FOR col_record IN 
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema = 'inervista' 
            AND table_name = 'fact_tourism'
            AND column_name IN ('staydays', 'basis', 'sex_male', 'sex_female', 
                               'age_15_29', 'age_30_44', 'age_45_59', 'age_60_plus',
                               'origin_d', 'origin_f', 'origin_i', 'size_hh_1_2', 'size_hh_3_plus')
            ORDER BY column_name
        LOOP
            demographic_columns := demographic_columns || ', ft.' || col_record.column_name;
        END LOOP;
        
        PERFORM edw.report_status('Found demographic columns: ' || demographic_columns);
        
        -- Check for transport columns
        FOR col_record IN 
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema = 'inervista' 
            AND table_name = 'fact_tourism'
            AND column_name LIKE 'transport_%'
            ORDER BY column_name
        LOOP
            transport_columns := transport_columns || ', ft.' || col_record.column_name;
        END LOOP;
        
        PERFORM edw.report_status('Found transport columns: ' || transport_columns);
        
        -- Check for education columns
        FOR col_record IN 
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema = 'inervista' 
            AND table_name = 'fact_tourism'
            AND column_name LIKE 'educ_%'
            ORDER BY column_name
        LOOP
            education_columns := education_columns || ', ft.' || col_record.column_name;
        END LOOP;
        
        PERFORM edw.report_status('Found education columns: ' || education_columns);
        
        -- Prepare dimensions for date calculation
        dimension_columns := 'DATE(dd.year || ''-'' || dd.month_number || ''-01'') AS start_date,
                            (DATE(dd.year || ''-'' || dd.month_number || ''-01'') + 
                            CASE
                                WHEN dd.month_number IN (1,3,5,7,8,10,12) THEN INTERVAL ''31 days''
                                WHEN dd.month_number IN (4,6,9,11) THEN INTERVAL ''30 days''
                                WHEN dd.month_number = 2 AND dd.year % 4 = 0 AND (dd.year % 100 <> 0 OR dd.year % 400 = 0) THEN INTERVAL ''29 days''
                                ELSE INTERVAL ''28 days''
                            END - INTERVAL ''1 day'')::date AS end_date,
                            r.region_name';
        
        -- Step 1: Create a temp table with only columns that exist
        PERFORM edw.report_status('Step 1: Creating temporary visitor data with available columns');
        EXECUTE 'CREATE TEMP TABLE tmp_visitor_data AS 
                SELECT ' || core_columns || 
                demographic_columns || 
                transport_columns || 
                education_columns || ', ' ||
                dimension_columns || '
                FROM inervista.fact_tourism ft
                JOIN inervista.dim_date dd ON ft.date_id = dd.date_id
                JOIN inervista.dim_region r ON ft.region_id = r.region_id';
        
        GET DIAGNOSTICS temp_count = ROW_COUNT;
        PERFORM edw.report_status('Temporary visitor data prepared with ' || temp_count || ' rows');

        -- Create raw visitor data
        PERFORM edw.report_status('Step 2: Aggregating raw visitor data from data_lake');
        BEGIN
            CREATE TEMP TABLE tmp_raw_visitor_data AS
            SELECT 
                v.fact_id,
                -- Aggregate raw visitor data by month and region
                AVG((a.visitors->>'swissTourist')::numeric) AS swiss_tourists_raw,
                AVG((a.visitors->>'foreignTourist')::numeric) AS foreign_tourists_raw,
                AVG((a.visitors->>'swissLocal')::numeric) AS swiss_locals_raw,
                AVG((a.visitors->>'foreignWorker')::numeric) AS foreign_workers_raw,
                AVG((a.visitors->>'swissCommuter')::numeric) AS swiss_commuters_raw,
                -- Dwelltimes - new field
                AVG((a.dwelltimes->>'averageMins')::numeric) AS avg_dwell_time_mins,
                -- Demographics from raw data (if available)
                jsonb_object_agg(
                    a.aoi_date::text, 
                    a.demographics
                ) FILTER (WHERE a.demographics IS NOT NULL) AS daily_demographics,
                -- Top countries/cantons - new fields from raw data
                jsonb_object_agg(
                    a.aoi_date::text, 
                    a.top_foreign_countries
                ) FILTER (WHERE a.top_foreign_countries IS NOT NULL) AS daily_top_countries,
                jsonb_object_agg(
                    a.aoi_date::text, 
                    a.top_swiss_cantons
                ) FILTER (WHERE a.top_swiss_cantons IS NOT NULL) AS daily_top_cantons,
                -- Cardinality tracking
                COUNT(a.id) AS matched_daily_records,
                COUNT(DISTINCT a.aoi_date) AS unique_days_matched,
                MIN(a.aoi_date) AS first_matched_date,
                MAX(a.aoi_date) AS last_matched_date
            FROM tmp_visitor_data v
            LEFT JOIN data_lake.aoi_days_raw a ON 
                a.aoi_date BETWEEN v.start_date AND v.end_date
                AND a.aoi_id = v.region_name  -- Match region names
            GROUP BY v.fact_id;
            
            GET DIAGNOSTICS temp_count = ROW_COUNT;
            PERFORM edw.report_status('Raw visitor data aggregated with ' || temp_count || ' rows');
        EXCEPTION
            WHEN OTHERS THEN
                PERFORM edw.report_status('Warning: Error creating raw visitor data - ' || SQLERRM);
                PERFORM edw.report_status('Creating empty raw visitor data as fallback');
                CREATE TEMP TABLE tmp_raw_visitor_data (
                    fact_id INTEGER PRIMARY KEY,
                    swiss_tourists_raw NUMERIC,
                    foreign_tourists_raw NUMERIC,
                    swiss_locals_raw NUMERIC,
                    foreign_workers_raw NUMERIC,
                    swiss_commuters_raw NUMERIC,
                    avg_dwell_time_mins NUMERIC,
                    daily_demographics JSONB,
                    daily_top_countries JSONB,
                    daily_top_cantons JSONB,
                    matched_daily_records INTEGER,
                    unique_days_matched INTEGER,
                    first_matched_date DATE,
                    last_matched_date DATE
                );
                
                INSERT INTO tmp_raw_visitor_data (fact_id)
                SELECT fact_id FROM tmp_visitor_data;
                
                PERFORM edw.report_status('Empty raw visitor data created as fallback - will proceed with core data only');
        END;

        -- Create final fact table - But now change all ft. references to v. for correct reference in the SELECT
        PERFORM edw.report_status('Step 3: Creating final fact_tourism_visitors table');
        
        -- Create versions of the column lists with "v." instead of "ft." for selecting from the temp table
        temp_demographic_columns := REPLACE(demographic_columns, 'ft.', 'v.');
        temp_transport_columns := REPLACE(transport_columns, 'ft.', 'v.');
        temp_education_columns := REPLACE(education_columns, 'ft.', 'v.');
        
        -- Create a dynamic query to build the final fact table with only available columns
        -- And fix the EXTRACT function issue by casting the interval to integer for day calculation
        EXECUTE 'CREATE TABLE edw.fact_tourism_visitors AS
                SELECT 
                    v.fact_id,
                    v.date_id,
                    v.region_id,
                    v.object_type_id,
                    v.visit_type_id,
                    v.data_type_id,
                    -- Base metric
                    v.total_visitors_structured' ||
                    
                    -- Add demographic columns if they exist (with v. prefix)
                    CASE WHEN temp_demographic_columns != '' THEN temp_demographic_columns ELSE '' END ||
                    
                    -- Add transport columns if they exist (with v. prefix)
                    CASE WHEN temp_transport_columns != '' THEN temp_transport_columns ELSE '' END ||
                    
                    -- Add education columns if they exist (with v. prefix)
                    CASE WHEN temp_education_columns != '' THEN temp_education_columns ELSE '' END ||
                    
                    -- Add raw data columns - these come from our manual aggregation
                    ',
                    -- Raw data from data_lake
                    r.swiss_tourists_raw,
                    r.foreign_tourists_raw,
                    r.swiss_locals_raw,
                    r.foreign_workers_raw,
                    r.swiss_commuters_raw,
                    r.avg_dwell_time_mins,
                    -- Condensed JSON data
                    r.daily_demographics,
                    r.daily_top_countries,
                    r.daily_top_cantons,
                    -- Cardinality and data quality metrics
                    r.matched_daily_records,
                    r.unique_days_matched,
                    r.first_matched_date,
                    r.last_matched_date,
                    -- Quality metadata
                    jsonb_build_object(
                        ''inervista'', jsonb_build_object(''fact_id'', v.fact_id),
                        ''data_lake'', 
                        CASE WHEN r.matched_daily_records > 0
                            THEN jsonb_build_object(
                                ''matched_days'', r.unique_days_matched,
                                ''date_range'', jsonb_build_object(
                                    ''first_date'', r.first_matched_date, 
                                    ''last_date'', r.last_matched_date
                                ),
                                ''cardinality'', r.matched_daily_records
                            )
                            ELSE NULL
                        END
                    ) AS source_keys,
                    CASE WHEN r.matched_daily_records > 0 THEN TRUE ELSE FALSE END AS has_raw_data_match,
                    -- Compute data completion percentage with proper date handling
                    CASE
                        WHEN r.unique_days_matched > 0 THEN
                            ROUND(
                                (r.unique_days_matched::numeric / 
                                ((v.end_date - v.start_date + 1)::integer)) * 100,
                                2
                            )
                        ELSE 0 
                    END AS data_completion_pct,
                    -- Sources
                    CASE WHEN r.matched_daily_records > 0 
                        THEN ''inervista.fact_tourism + data_lake.aoi_days_raw'' 
                        ELSE ''inervista.fact_tourism''
                    END AS data_sources,
                    -- Add creation timestamp for ETL tracking
                    CURRENT_TIMESTAMP AS created_at,
                    CURRENT_TIMESTAMP AS updated_at
                FROM tmp_visitor_data v
                LEFT JOIN tmp_raw_visitor_data r ON v.fact_id = r.fact_id';
        
        GET DIAGNOSTICS final_count = ROW_COUNT;
        
        PERFORM edw.report_status('Step 4: Creating indexes on fact_tourism_visitors');
        ALTER TABLE edw.fact_tourism_visitors ADD PRIMARY KEY (fact_id);
        CREATE INDEX idx_ftv_dimensions ON edw.fact_tourism_visitors(date_id, region_id, object_type_id, visit_type_id, data_type_id);
        CREATE INDEX idx_ftv_date ON edw.fact_tourism_visitors(date_id);
        CREATE INDEX idx_ftv_region ON edw.fact_tourism_visitors(region_id);
        CREATE INDEX idx_ftv_has_raw_data ON edw.fact_tourism_visitors(has_raw_data_match);
        
        PERFORM edw.report_status('fact_tourism_visitors created with ' || final_count || ' rows');
        PERFORM edw.report_status('Processing time: ' || 
                               EXTRACT(EPOCH FROM (clock_timestamp() - start_time))/60 || ' minutes');
        
        -- Clean up temp tables
        PERFORM edw.report_status('Step 5: Cleaning up temporary tables');
        DROP TABLE IF EXISTS tmp_visitor_data;
        DROP TABLE IF EXISTS tmp_raw_visitor_data;
        
        -- Record ETL statistics
        INSERT INTO edw.etl_metadata (
            table_name, 
            load_start_time, 
            load_end_time, 
            rows_processed,
            rows_inserted,
            load_status, 
            additional_info
        )
        VALUES (
            'fact_tourism_visitors',
            start_time,
            clock_timestamp(),
            temp_count,
            final_count,
            'COMPLETE',
            jsonb_build_object(
                'processing_time_minutes', EXTRACT(EPOCH FROM (clock_timestamp() - start_time))/60,
                'data_sources', 'inervista.fact_tourism + data_lake.aoi_days_raw',
                'records_with_raw_data', (SELECT COUNT(*) FROM edw.fact_tourism_visitors WHERE has_raw_data_match = TRUE),
                'avg_data_completion', (SELECT AVG(data_completion_pct) FROM edw.fact_tourism_visitors)
            )
        );
        
        PERFORM edw.report_status('Task 3: Visitor facts processing completed successfully');
    ELSE
        PERFORM edw.report_status('fact_tourism_visitors already exists - skipping processing');
        
        -- Provide summary of existing table/view
        PERFORM edw.report_status('Summary of existing fact_tourism_visitors:');
        
        -- Type of object
        IF EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'edw' AND table_name = 'fact_tourism_visitors' 
                  AND table_type = 'BASE TABLE') THEN
            PERFORM edw.report_status('Object type: Table');
        ELSE
            PERFORM edw.report_status('Object type: View');
        END IF;
        
        -- Count total rows
        PERFORM edw.report_status('Total rows: ' || (SELECT COUNT(*) FROM edw.fact_tourism_visitors));
        
        -- Check if has_raw_data_match column exists before accessing it
        IF EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_schema = 'edw' AND table_name = 'fact_tourism_visitors' 
                  AND column_name = 'has_raw_data_match') THEN
            PERFORM edw.report_status('Records with raw data: ' || 
                                 (SELECT COUNT(*) FROM edw.fact_tourism_visitors WHERE has_raw_data_match = TRUE));
        ELSE
            PERFORM edw.report_status('Records with raw data: Not available (column does not exist)');
        END IF;
        
        -- Check if data_completion_pct column exists before accessing it
        IF EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_schema = 'edw' AND table_name = 'fact_tourism_visitors' 
                  AND column_name = 'data_completion_pct') THEN
            PERFORM edw.report_status('Average data completion: ' || 
                                 (SELECT ROUND(AVG(data_completion_pct), 2) FROM edw.fact_tourism_visitors) || '%');
        ELSE
            PERFORM edw.report_status('Average data completion: Not available (column does not exist)');
        END IF;
    END IF;
END;
$$;