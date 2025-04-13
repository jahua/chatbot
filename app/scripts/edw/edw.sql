-- Create the ETL metadata table if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.etl_metadata (
    etl_run_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    load_start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_end_time TIMESTAMP,
    rows_processed INTEGER,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    load_status VARCHAR(20) CHECK (load_status IN ('IN PROGRESS', 'COMPLETE', 'FAILED')),
    error_message TEXT,
    additional_info JSONB
);

-- Create a status reporting function
CREATE OR REPLACE FUNCTION edw.report_status(p_message TEXT) RETURNS VOID AS $$
BEGIN
    RAISE NOTICE '%: %', clock_timestamp()::timestamp(0), p_message;
    
    -- Also log to a table for persistent tracking
    INSERT INTO edw.etl_metadata (table_name, load_start_time, load_end_time, load_status, additional_info)
    VALUES ('status_message', clock_timestamp(), clock_timestamp(), 'COMPLETE', 
           jsonb_build_object('message', p_message));
END;
$$ LANGUAGE plpgsql;

-- Process dimensions first
DO $$
BEGIN
    PERFORM edw.report_status('Starting dimension table processing');
    
    -- Process Time Dimension
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'dim_time')
    THEN
        PERFORM edw.report_status('Creating dim_time table');
        
        CREATE TABLE edw.dim_time AS
        SELECT 
            date_id,
            month,
            month_short,
            month_number,
            year,
            CASE 
                WHEN month_number BETWEEN 3 AND 5 THEN 'Spring'
                WHEN month_number BETWEEN 6 AND 8 THEN 'Summer'
                WHEN month_number BETWEEN 9 AND 11 THEN 'Fall'
                ELSE 'Winter'
            END AS season,
            EXTRACT(WEEK FROM DATE(year || '-' || month_number || '-01')) AS week_of_year,
            DATE(year || '-' || month_number || '-01') AS period_start_date,
            (DATE(year || '-' || month_number || '-01') + 
                CASE
                    WHEN month_number IN (1,3,5,7,8,10,12) THEN INTERVAL '31 days'
                    WHEN month_number IN (4,6,9,11) THEN INTERVAL '30 days'
                    WHEN month_number = 2 AND year % 4 = 0 AND (year % 100 <> 0 OR year % 400 = 0) THEN INTERVAL '29 days'
                    ELSE INTERVAL '28 days'
                END - INTERVAL '1 day')::date AS period_end_date,
            'inervista.dim_date' AS data_source,
            TRUE AS is_complete_period,
            'monthly' AS granularity
        FROM inervista.dim_date;

        ALTER TABLE edw.dim_time ADD PRIMARY KEY (date_id);
        CREATE INDEX idx_dim_time_year_month ON edw.dim_time(year, month_number);
        
        PERFORM edw.report_status('dim_time created with ' || (SELECT COUNT(*) FROM edw.dim_time) || ' rows');
    ELSE
        PERFORM edw.report_status('dim_time already exists');
    END IF;
    
    -- Process Region Dimension
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'dim_region')
    THEN
        PERFORM edw.report_status('Creating dim_region table');
        
        CREATE TABLE edw.dim_region AS
        SELECT 
            r.region_id,
            r.region_name,
            r.region_type,
            r.parent_region_id,
            r.region_code,
            mc.central_latitude,
            mc.central_longitude,
            mc.bounding_box,
            CASE WHEN mc.geo_name IS NOT NULL THEN TRUE ELSE FALSE END AS has_geo_match,
            CASE 
                WHEN mc.geo_name IS NOT NULL AND LOWER(r.region_name) = LOWER(mc.geo_name) THEN 'exact'
                WHEN mc.geo_name IS NOT NULL THEN 'partial'
                ELSE 'none'
            END AS match_type,
            jsonb_build_object(
                'inervista', jsonb_build_object('region_id', r.region_id),
                'data_lake', CASE WHEN mc.geo_name IS NOT NULL 
                    THEN jsonb_build_object(
                        'geo_name', mc.geo_name, 
                        'geo_type', mc.geo_type
                    ) 
                    ELSE NULL
                END
            ) AS source_keys
        FROM inervista.dim_region r
        LEFT JOIN (
            SELECT DISTINCT ON (LOWER(geo_name)) 
                geo_name, 
                geo_type, 
                central_latitude, 
                central_longitude, 
                bounding_box
            FROM data_lake.master_card
            WHERE central_latitude IS NOT NULL AND central_longitude IS NOT NULL
            ORDER BY LOWER(geo_name), txn_date DESC
        ) mc ON LOWER(r.region_name) = LOWER(mc.geo_name);

        ALTER TABLE edw.dim_region ADD PRIMARY KEY (region_id);
        CREATE INDEX idx_dim_region_name ON edw.dim_region(region_name);
        CREATE INDEX idx_dim_region_parent ON edw.dim_region(parent_region_id);
        
        PERFORM edw.report_status('dim_region created with ' || (SELECT COUNT(*) FROM edw.dim_region) || ' rows');
    ELSE
        PERFORM edw.report_status('dim_region already exists');
    END IF;

    -- Process other dimension tables (similar pattern)
    -- Object Type Dimension
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'dim_object_type')
    THEN
        PERFORM edw.report_status('Creating dim_object_type table');
        
        CREATE TABLE edw.dim_object_type AS
        SELECT 
            object_type_id,
            object_type_name,
            object_type_description,
            'inervista.dim_object_type' AS data_source,
            jsonb_build_object('inervista', jsonb_build_object('object_type_id', object_type_id)) AS source_keys
        FROM inervista.dim_object_type;

        ALTER TABLE edw.dim_object_type ADD PRIMARY KEY (object_type_id);
        
        PERFORM edw.report_status('dim_object_type created with ' || (SELECT COUNT(*) FROM edw.dim_object_type) || ' rows');
    ELSE
        PERFORM edw.report_status('dim_object_type already exists');
    END IF;

    -- Visit Type Dimension
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'dim_visit_type')
    THEN
        PERFORM edw.report_status('Creating dim_visit_type table');
        
        CREATE TABLE edw.dim_visit_type AS
        SELECT 
            visit_type_id,
            visit_type_name,
            visit_type_description,
            'inervista.dim_visit_type' AS data_source,
            jsonb_build_object('inervista', jsonb_build_object('visit_type_id', visit_type_id)) AS source_keys
        FROM inervista.dim_visit_type;

        ALTER TABLE edw.dim_visit_type ADD PRIMARY KEY (visit_type_id);
        
        PERFORM edw.report_status('dim_visit_type created with ' || (SELECT COUNT(*) FROM edw.dim_visit_type) || ' rows');
    ELSE
        PERFORM edw.report_status('dim_visit_type already exists');
    END IF;

    -- Data Type Dimension
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'dim_data_type')
    THEN
        PERFORM edw.report_status('Creating dim_data_type table');
        
        CREATE TABLE edw.dim_data_type AS
        SELECT 
            data_type_id,
            data_type_name,
            data_type_description,
            'inervista.dim_data_type' AS data_source,
            jsonb_build_object('inervista', jsonb_build_object('data_type_id', data_type_id)) AS source_keys
        FROM inervista.dim_data_type;

        ALTER TABLE edw.dim_data_type ADD PRIMARY KEY (data_type_id);
        
        PERFORM edw.report_status('dim_data_type created with ' || (SELECT COUNT(*) FROM edw.dim_data_type) || ' rows');
    ELSE
        PERFORM edw.report_status('dim_data_type already exists');
    END IF;

    -- Industry Dimension
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'dim_industry')
    THEN
        PERFORM edw.report_status('Creating dim_industry table');
        
        CREATE TABLE edw.dim_industry AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY industry) AS industry_id,
            industry AS industry_name,
            COUNT(DISTINCT segment) AS segment_count,
            CASE 
                WHEN SUM(txn_amt) > 1000000 THEN 'Major Industry'
                WHEN SUM(txn_amt) > 500000 THEN 'Significant Industry'
                ELSE 'Minor Industry'
            END AS industry_category,
            'data_lake.master_card' AS data_source,
            jsonb_build_object('data_lake', jsonb_build_object('industry', industry)) AS source_keys
        FROM data_lake.master_card
        GROUP BY industry;

        ALTER TABLE edw.dim_industry ADD PRIMARY KEY (industry_id);
        CREATE INDEX idx_dim_industry_name ON edw.dim_industry(industry_name);
        
        PERFORM edw.report_status('dim_industry created with ' || (SELECT COUNT(*) FROM edw.dim_industry) || ' rows');
    ELSE
        PERFORM edw.report_status('dim_industry already exists');
    END IF;

    PERFORM edw.report_status('All dimension tables created successfully');
END;
$$;

-- Process Visitor fact table
DO $$
BEGIN
    -- Skip if already exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'fact_tourism_visitors')
    THEN
        PERFORM edw.report_status('Creating fact_tourism_visitors table');
        
        -- Prepare visitor data
        CREATE TEMP TABLE tmp_visitor_data AS
        SELECT 
            -- Dimensional keys
            ft.fact_id,
            ft.date_id,
            ft.region_id,
            ft.object_type_id,
            ft.visit_type_id,
            ft.data_type_id,
            -- Inervista metrics
            ft.total AS total_visitors_structured,
            ft.staydays,
            ft.basis,
            -- Demographics
            ft.sex_male,
            ft.sex_female,
            ft.age_15_29,
            ft.age_30_44,
            ft.age_45_59,
            ft.age_60_plus,
            -- Additional segmentation
            ft.origin_D,
            ft.origin_F,
            ft.origin_I,
            ft.size_hh_1_2,
            ft.size_hh_3_plus,
            -- Transport modes
            ft.transport_abroad,
            ft.transport_invehicle,
            ft.transport_onbicycle,
            ft.transport_onfoot,
            ft.transport_other,
            ft.transport_public,
            -- Education segments
            ft.educ_low,
            ft.educ_medium,
            ft.educ_high,
            -- Join key for data_lake
            DATE(dd.year || '-' || dd.month_number || '-01') AS start_date,
            (DATE(dd.year || '-' || dd.month_number || '-01') + 
            CASE
                WHEN dd.month_number IN (1,3,5,7,8,10,12) THEN INTERVAL '31 days'
                WHEN dd.month_number IN (4,6,9,11) THEN INTERVAL '30 days'
                WHEN dd.month_number = 2 AND dd.year % 4 = 0 AND (dd.year % 100 <> 0 OR dd.year % 400 = 0) THEN INTERVAL '29 days'
                ELSE INTERVAL '28 days'
            END - INTERVAL '1 day')::date AS end_date,
            r.region_name
        FROM inervista.fact_tourism ft
        JOIN inervista.dim_date dd ON ft.date_id = dd.date_id
        JOIN inervista.dim_region r ON ft.region_id = r.region_id;
        
        PERFORM edw.report_status('Temporary visitor data prepared with ' || (SELECT COUNT(*) FROM tmp_visitor_data) || ' rows');

        -- Create raw visitor data
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
        
        PERFORM edw.report_status('Raw visitor data aggregated');

        -- Create final fact table
        CREATE TABLE edw.fact_tourism_visitors AS
        SELECT 
            v.fact_id,
            v.date_id,
            v.region_id,
            v.object_type_id,
            v.visit_type_id,
            v.data_type_id,
            -- Inervista metrics
            v.total_visitors_structured,
            v.staydays,
            v.basis,
            -- Demographics
            v.sex_male,
            v.sex_female,
            v.age_15_29,
            v.age_30_44,
            v.age_45_59,
            v.age_60_plus,
            -- Additional segmentation
            v.origin_D,
            v.origin_F,
            v.origin_I,
            v.size_hh_1_2,
            v.size_hh_3_plus,
            -- Transport modes
            v.transport_abroad,
            v.transport_invehicle,
            v.transport_onbicycle,
            v.transport_onfoot,
            v.transport_other,
            v.transport_public,
            -- Education segments
            v.educ_low,
            v.educ_medium,
            v.educ_high,
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
                'inervista', jsonb_build_object('fact_id', v.fact_id),
                'data_lake', 
                CASE WHEN r.matched_daily_records > 0
                    THEN jsonb_build_object(
                        'matched_days', r.unique_days_matched,
                        'date_range', jsonb_build_object(
                            'first_date', r.first_matched_date, 
                            'last_date', r.last_matched_date
                        ),
                        'cardinality', r.matched_daily_records
                    )
                    ELSE NULL
                END
            ) AS source_keys,
            CASE WHEN r.matched_daily_records > 0 THEN TRUE ELSE FALSE END AS has_raw_data_match,
            -- Compute data completion percentage
            CASE
                WHEN r.unique_days_matched > 0 THEN
                    ROUND(
                        (r.unique_days_matched::numeric / 
                        (EXTRACT(DAY FROM v.end_date - v.start_date) + 1)) * 100,
                        2
                    )
                ELSE 0 
            END AS data_completion_pct,
            -- Sources
            CASE WHEN r.matched_daily_records > 0 
                THEN 'inervista.fact_tourism + data_lake.aoi_days_raw' 
                ELSE 'inervista.fact_tourism'
            END AS data_sources,
            -- Add creation timestamp for ETL tracking
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM tmp_visitor_data v
        LEFT JOIN tmp_raw_visitor_data r ON v.fact_id = r.fact_id;

        ALTER TABLE edw.fact_tourism_visitors ADD PRIMARY KEY (fact_id);
        CREATE INDEX idx_ftv_dimensions ON edw.fact_tourism_visitors(date_id, region_id, object_type_id, visit_type_id, data_type_id);
        CREATE INDEX idx_ftv_date ON edw.fact_tourism_visitors(date_id);
        CREATE INDEX idx_ftv_region ON edw.fact_tourism_visitors(region_id);
        CREATE INDEX idx_ftv_has_raw_data ON edw.fact_tourism_visitors(has_raw_data_match);
        
        PERFORM edw.report_status('fact_tourism_visitors created with ' || (SELECT COUNT(*) FROM edw.fact_tourism_visitors) || ' rows');
        
        -- Clean up temp tables
        DROP TABLE tmp_visitor_data;
        DROP TABLE tmp_raw_visitor_data;
    ELSE
        PERFORM edw.report_status('fact_tourism_visitors already exists');
    END IF;
END;
$$;

-- Optimized MasterCard Processing - Batched by Region
DO $$
DECLARE
    region_rec RECORD;
    total_regions INTEGER;
    processed_regions INTEGER := 0;
    start_time TIMESTAMP;
    region_count INTEGER;
BEGIN
    -- Skip if already exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'fact_tourism_spending')
    THEN
        PERFORM edw.report_status('Creating fact_tourism_spending table');
        start_time := clock_timestamp();
        
        -- First, create the table structure
        CREATE TABLE edw.fact_tourism_spending (
            fact_id INTEGER PRIMARY KEY,
            date_id INTEGER NOT NULL,
            region_id INTEGER NOT NULL,
            object_type_id INTEGER NOT NULL,
            visit_type_id INTEGER NOT NULL,
            data_type_id INTEGER NOT NULL,
            visitor_count NUMERIC,
            total_transaction_amount NUMERIC,
            total_transaction_count NUMERIC,
            average_transaction_all_industries NUMERIC,
            industry_count INTEGER,
            total_transaction_records INTEGER,
            max_days_with_transactions INTEGER,
            earliest_transaction_date DATE,
            latest_transaction_date DATE,
            total_account_count NUMERIC,
            industry_metrics JSONB,
            spend_per_visitor NUMERIC,
            source_keys JSONB,
            has_transaction_data BOOLEAN,
            data_completion_pct NUMERIC,
            data_sources VARCHAR(100),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Get total number of regions for progress reporting
        SELECT COUNT(DISTINCT region_id) INTO total_regions FROM inervista.fact_tourism;
        
        PERFORM edw.report_status('Processing ' || total_regions || ' regions for spending data');
        
        -- Process each region separately to avoid memory issues
        FOR region_rec IN (
            SELECT DISTINCT r.region_id, r.region_name 
            FROM inervista.dim_region r
            JOIN inervista.fact_tourism f ON r.region_id = f.region_id
            ORDER BY r.region_id
        ) LOOP
            processed_regions := processed_regions + 1;
            
            -- Prepare spending data for this region
            CREATE TEMP TABLE tmp_spending_data AS
            SELECT 
                ft.fact_id,
                ft.date_id,
                ft.region_id,
                ft.object_type_id,
                ft.visit_type_id,
                ft.data_type_id,
                ft.total AS visitor_count,
                DATE(dd.year || '-' || dd.month_number || '-01') AS start_date,
                (DATE(dd.year || '-' || dd.month_number || '-01') + 
                CASE
                    WHEN dd.month_number IN (1,3,5,7,8,10,12) THEN INTERVAL '31 days'
                    WHEN dd.month_number IN (4,6,9,11) THEN INTERVAL '30 days'
                    WHEN dd.month_number = 2 AND dd.year % 4 = 0 AND (dd.year % 100 <> 0 OR dd.year % 400 = 0) THEN INTERVAL '29 days'
                    ELSE INTERVAL '28 days'
                END - INTERVAL '1 day')::date AS end_date,
                dd.year
            FROM inervista.fact_tourism ft
            JOIN inervista.dim_date dd ON ft.date_id = dd.date_id
            WHERE ft.region_id = region_rec.region_id;
            
            GET DIAGNOSTICS region_count = ROW_COUNT;
            
            IF region_count > 0 THEN
                PERFORM edw.report_status('Processing region ' || processed_regions || '/' || total_regions || 
                                         ': ' || region_rec.region_name || ' (' || region_count || ' records)');
                                         
                -- Get MasterCard data just for this region (with index-friendly conditions)
                CREATE TEMP TABLE tmp_region_mc_data AS
                SELECT 
                    id, txn_date, industry, txn_amt, txn_cnt, avg_ticket, acct_cnt, avg_freq, avg_spend_amt,
                    yr, geo_name
                FROM data_lake.master_card
                WHERE LOWER(geo_name) = LOWER(region_rec.region_name)
                -- Optional date filter for older data
                -- AND txn_date >= '2019-01-01'
                ;
                
                PERFORM edw.report_status('Region ' || region_rec.region_name || 
                                         ': Found ' || (SELECT COUNT(*) FROM tmp_region_mc_data) || ' transactions');
                
                -- Aggregate card spending data with temp table
                CREATE TEMP TABLE tmp_card_spending_data AS
                SELECT 
                    s.fact_id,
                    mc.industry,
                    -- Aggregate spending metrics
                    SUM(mc.txn_amt) AS transaction_amount,
                    SUM(mc.txn_cnt) AS transaction_count,
                    AVG(mc.avg_ticket) AS average_transaction,
                    -- New metrics
                    COUNT(DISTINCT mc.txn_date) AS days_with_transactions,
                    COUNT(DISTINCT mc.id) AS transaction_records,
                    MIN(mc.txn_date) AS first_transaction_date,
                    MAX(mc.txn_date) AS last_transaction_date,
                    SUM(mc.acct_cnt) AS account_count,
                    AVG(mc.avg_freq) AS average_frequency,
                    AVG(mc.avg_spend_amt) AS average_spend,
                    -- For industry dimension
                    (SELECT industry_id FROM edw.dim_industry di WHERE di.industry_name = mc.industry) AS industry_id
                FROM tmp_spending_data s
                JOIN tmp_region_mc_data mc ON 
                    mc.txn_date BETWEEN s.start_date AND s.end_date
                    AND mc.yr = s.year
                GROUP BY s.fact_id, mc.industry;
                
                -- Insert region data into fact table
                INSERT INTO edw.fact_tourism_spending (
                    fact_id, date_id, region_id, object_type_id, visit_type_id, data_type_id,
                    visitor_count, total_transaction_amount, total_transaction_count, 
                    average_transaction_all_industries, industry_count, total_transaction_records,
                    max_days_with_transactions, earliest_transaction_date, latest_transaction_date,
                    total_account_count, industry_metrics, spend_per_visitor, source_keys,
                    has_transaction_data, data_completion_pct, data_sources
                )
                SELECT
                    sd.fact_id,
                    sd.date_id,
                    sd.region_id,
                    sd.object_type_id,
                    sd.visit_type_id,
                    sd.data_type_id,
                    sd.visitor_count,
                    -- Spending metrics summary
                    SUM(csd.transaction_amount) AS total_transaction_amount,
                    SUM(csd.transaction_count) AS total_transaction_count,
                    AVG(csd.average_transaction) AS average_transaction_all_industries,
                    -- Number of industries represented
                    COUNT(DISTINCT csd.industry) AS industry_count,
                    -- Cardinality metrics
                    SUM(csd.transaction_records) AS total_transaction_records,
                    MAX(csd.days_with_transactions) AS max_days_with_transactions,
                    MIN(csd.first_transaction_date) AS earliest_transaction_date,
                    MAX(csd.last_transaction_date) AS latest_transaction_date,
                    SUM(csd.account_count) AS total_account_count,
                    -- Create JSON object with data by industry 
                    jsonb_object_agg(
                        COALESCE(csd.industry, 'unknown'),
                        jsonb_build_object(
                            'transaction_amount', csd.transaction_amount,
                            'transaction_count', csd.transaction_count,
                            'average_transaction', csd.average_transaction,
                            'account_count', csd.account_count,
                            'industry_id', csd.industry_id
                        )
                    ) FILTER (WHERE csd.industry IS NOT NULL) AS industry_metrics,
                    -- Computed fields
                    CASE 
                        WHEN SUM(csd.transaction_amount) > 0 AND sd.visitor_count > 0 
                        THEN SUM(csd.transaction_amount) / sd.visitor_count 
                        ELSE NULL 
                    END AS spend_per_visitor,
                    -- Quality metadata
                    jsonb_build_object(
                        'inervista', jsonb_build_object('fact_id', sd.fact_id),
                        'data_lake', 
                        CASE WHEN SUM(csd.transaction_records) > 0
                            THEN jsonb_build_object(
                                'transaction_records', SUM(csd.transaction_records),
                                'industries', COUNT(DISTINCT csd.industry),
                                'date_range', jsonb_build_object(
                                    'first_date', MIN(csd.first_transaction_date), 
                                    'last_date', MAX(csd.last_transaction_date)
                                ),
                                'days_with_data', MAX(csd.days_with_transactions)
                            )
                            ELSE NULL
                        END
                    ) AS source_keys,
                    CASE WHEN SUM(csd.transaction_records) > 0 THEN TRUE ELSE FALSE END AS has_transaction_data,
                    -- Compute data completion percentage
                    CASE
                        WHEN MAX(csd.days_with_transactions) > 0 THEN
                            ROUND(
                                (MAX(csd.days_with_transactions)::numeric / 
                                (EXTRACT(DAY FROM sd.end_date - sd.start_date) + 1)) * 100,
                                2
                            )
                        ELSE 0 
                    END AS data_completion_pct,
                    -- Sources
                    CASE WHEN SUM(csd.transaction_records) > 0 
                        THEN 'inervista.fact_tourism + data_lake.master_card' 
                        ELSE 'inervista.fact_tourism'
                    END AS data_sources
                FROM tmp_spending_data sd
                LEFT JOIN tmp_card_spending_data csd ON sd.fact_id = csd.fact_id
                GROUP BY sd.fact_id, sd.date_id, sd.region_id, sd.object_type_id, sd.visit_type_id, 
                         sd.data_type_id, sd.visitor_count, sd.start_date, sd.end_date;
                
                PERFORM edw.report_status('Inserted data for region ' || region_rec.region_name || 
                                         ' [' || processed_regions || '/' || total_regions || ']');
            ELSE
                PERFORM edw.report_status('Skipping region ' || region_rec.region_name || ' - no records found');
            END IF;
            
            -- Clean up temp tables for this region
            DROP TABLE IF EXISTS tmp_spending_data;
            DROP TABLE IF EXISTS tmp_region_mc_data;
            DROP TABLE IF EXISTS tmp_card_spending_data;
            
            -- Commit after each region to free up memory
            COMMIT;
        END LOOP;
        
        -- Create indexes after all data is loaded
        CREATE INDEX idx_fts_dimensions ON edw.fact_tourism_spending(date_id, region_id, object_type_id, visit_type_id, data_type_id);
        CREATE INDEX idx_fts_date ON edw.fact_tourism_spending(date_id);
        CREATE INDEX idx_fts_region ON edw.fact_tourism_spending(region_id);
        CREATE INDEX idx_fts_has_transaction_data ON edw.fact_tourism_spending(has_transaction_data);
        
        PERFORM edw.report_status('fact_tourism_spending created with ' || (SELECT COUNT(*) FROM edw.fact_tourism_spending) || ' rows');
        PERFORM edw.report_status('Processing time: ' || 
                                 EXTRACT(EPOCH FROM (clock_timestamp() - start_time))/60 || ' minutes');
    ELSE
        PERFORM edw.report_status('fact_tourism_spending already exists');
    END IF;
END;
$;

-- Create the unified fact table
DO $
BEGIN
    -- Skip if already exists
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'fact_tourism_unified')
    THEN
        PERFORM edw.report_status('Creating fact_tourism_unified table');
        
        CREATE TABLE edw.fact_tourism_unified AS
        SELECT
            -- Business key 
            f.fact_id, 
            -- Dimension keys
            f.date_id,
            f.region_id,
            f.object_type_id,
            f.visit_type_id,
            f.data_type_id,
            -- Dimension attributes for easy access
            d.month AS date_name,
            d.year,
            d.month_number,
            dt.season,  
            r.region_name,
            r.region_type,
            r.has_geo_match,
            -- Optional geo data (only when available)
            r.central_latitude,
            r.central_longitude,
            r.bounding_box,
            ot.object_type_name,
            vt.visit_type_name,
            dat.data_type_name,
            -- Tourism metrics from base fact
            f.total AS visitor_count,
            ftv.staydays,
            -- Demographics from visitor fact
            ftv.sex_male,
            ftv.sex_female,
            ftv.age_15_29,
            ftv.age_30_44,
            ftv.age_45_59,
            ftv.age_60_plus,
            -- Raw metrics from visitor fact view
            ftv.swiss_tourists_raw,
            ftv.foreign_tourists_raw,
            ftv.swiss_locals_raw,
            ftv.foreign_workers_raw,
            ftv.swiss_commuters_raw,
            ftv.avg_dwell_time_mins,
            -- Spending metrics from spending fact
            fts.total_transaction_amount AS spending_amount,
            fts.total_transaction_count AS transaction_count,
            fts.average_transaction_all_industries AS avg_transaction_size,
            fts.spend_per_visitor,
            fts.industry_count,
            fts.industry_metrics,
            -- Quality metrics
            ftv.data_completion_pct AS visitor_data_completion_pct,
            fts.data_completion_pct AS spending_data_completion_pct,
            -- Calculate overall data quality score (0-100)
            (
                COALESCE(CASE WHEN ftv.has_raw_data_match THEN 50 ELSE 0 END, 0) + 
                COALESCE(CASE WHEN fts.has_transaction_data THEN 50 ELSE 0 END, 0)
            ) AS data_quality_score,
            -- Quality flags
            CASE 
                WHEN fts.has_transaction_data AND ftv.has_raw_data_match THEN 'complete' -- Has spending and raw data
                WHEN fts.has_transaction_data OR ftv.has_raw_data_match THEN 'partial'  -- Has spending OR raw data
                ELSE 'minimal' -- Only base fact data
            END AS data_completeness,
            -- Unified metadata
            jsonb_build_object(
                'inervista', jsonb_build_object('fact_id', f.fact_id),
                'unified_sources', jsonb_build_object(
                    'visitor_data', ftv.source_keys,
                    'spending_data', fts.source_keys
                ),
                'created_at', CURRENT_TIMESTAMP
            ) AS metadata,
            -- Add creation timestamp for ETL tracking
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM inervista.fact_tourism f
        -- Dimension joins
        JOIN inervista.dim_date d ON f.date_id = d.date_id
        JOIN edw.dim_region r ON f.region_id = r.region_id 
        JOIN edw.dim_object_type ot ON f.object_type_id = ot.object_type_id
        JOIN edw.dim_visit_type vt ON f.visit_type_id = vt.visit_type_id
        JOIN edw.dim_data_type dat ON f.data_type_id = dat.data_type_id
        JOIN edw.dim_time dt ON f.date_id = dt.date_id
        -- Joins to materialized fact tables
        LEFT JOIN edw.fact_tourism_visitors ftv ON f.fact_id = ftv.fact_id 
        LEFT JOIN edw.fact_tourism_spending fts ON f.fact_id = fts.fact_id;

        ALTER TABLE edw.fact_tourism_unified ADD PRIMARY KEY (fact_id);
        CREATE INDEX idx_ftu_dimensions ON edw.fact_tourism_unified(date_id, region_id, object_type_id, visit_type_id, data_type_id);
        CREATE INDEX idx_ftu_date_region ON edw.fact_tourism_unified(date_id, region_id);
        CREATE INDEX idx_ftu_completeness ON edw.fact_tourism_unified(data_completeness);
        CREATE INDEX idx_ftu_geo ON edw.fact_tourism_unified(central_latitude, central_longitude) 
            WHERE central_latitude IS NOT NULL AND central_longitude IS NOT NULL;
        
        PERFORM edw.report_status('fact_tourism_unified created with ' || (SELECT COUNT(*) FROM edw.fact_tourism_unified) || ' rows');
    ELSE
        PERFORM edw.report_status('fact_tourism_unified already exists');
    END IF;
END;
$;

-- Create the analysis views
DO $
BEGIN
    PERFORM edw.report_status('Creating analysis views');
    
    -- Tourism Visitor Trends Analysis View
    DROP VIEW IF EXISTS edw.analysis_visitor_trends;
    CREATE OR REPLACE VIEW edw.analysis_visitor_trends AS
    SELECT
        dt.year,
        dt.month_number,
        dt.month,
        dt.season,
        -- Total metrics
        SUM(f.visitor_count) AS total_visitors,
        SUM(f.swiss_tourists_raw) AS total_swiss_tourists,
        SUM(f.foreign_tourists_raw) AS total_foreign_tourists,
        SUM(f.staydays) AS total_staydays,
        -- Demographics
        SUM(f.sex_male) AS male_visitors,
        SUM(f.sex_female) AS female_visitors,
        SUM(f.age_15_29) AS visitors_15_29,
        SUM(f.age_30_44) AS visitors_30_44,
        SUM(f.age_45_59) AS visitors_45_59,
        SUM(f.age_60_plus) AS visitors_60_plus,
        
        -- Percentages
        (100.0 * SUM(f.age_15_29) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_age_15_29,
        (100.0 * SUM(f.age_30_44) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_age_30_44,
        (100.0 * SUM(f.age_45_59) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_age_45_59,
        (100.0 * SUM(f.age_60_plus) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_age_60_plus,
        (100.0 * SUM(f.sex_male) / NULLIF(SUM(f.sex_male + f.sex_female), 0))::numeric(5,1) AS pct_male,
        
        -- Raw vs structured data
        SUM(f.visitor_count) AS structured_visitor_count,
        COALESCE(SUM(COALESCE(f.swiss_tourists_raw, 0) + 
            COALESCE(f.foreign_tourists_raw, 0) + 
            COALESCE(f.swiss_locals_raw, 0) + 
            COALESCE(f.foreign_workers_raw, 0) + 
            COALESCE(f.swiss_commuters_raw, 0)), 0) AS raw_visitor_count,
        
        -- Data quality metrics
        COUNT(*) AS total_records,
        SUM(CASE WHEN f.swiss_tourists_raw IS NOT NULL THEN 1 ELSE 0 END) AS records_with_raw_data, 
        (100.0 * SUM(CASE WHEN f.swiss_tourists_raw IS NOT NULL THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0))::numeric(5,1) AS pct_with_raw_data,
        AVG(COALESCE(f.visitor_data_completion_pct, 0))::numeric(5,1) AS avg_data_completion_pct,
        
        -- Metadata
        CURRENT_TIMESTAMP AS generated_at
    FROM edw.fact_tourism_unified f
    JOIN edw.dim_time dt ON f.date_id = dt.date_id
    GROUP BY dt.year, dt.month_number, dt.month, dt.season
    ORDER BY dt.year, dt.month_number;
    
    PERFORM edw.report_status('analysis_visitor_trends view created');

    -- Region Performance Analysis View
    DROP VIEW IF EXISTS edw.analysis_region_performance;
    CREATE OR REPLACE VIEW edw.analysis_region_performance AS
    SELECT
        r.region_name,
        r.region_type,
        dt.year,
        -- Optional geo data
        r.central_latitude,
        r.central_longitude,
        r.bounding_box,
        -- Visitor metrics
        SUM(f.visitor_count) AS total_visitors,
        (AVG(f.visitor_count))::numeric(10,1) AS avg_monthly_visitors,
        SUM(f.staydays) AS total_staydays,
        (AVG(f.staydays))::numeric(10,1) AS avg_staydays,
        (SUM(f.staydays) / NULLIF(SUM(f.visitor_count), 0))::numeric(5,2) AS avg_stay_per_visitor,
        -- Spending metrics
        SUM(f.spending_amount) AS total_spending,
        SUM(f.transaction_count) AS total_transactions,
        (SUM(f.spending_amount) / NULLIF(SUM(f.visitor_count), 0))::numeric(10,2) AS spend_per_visitor,
        (SUM(f.spending_amount) / NULLIF(SUM(f.transaction_count), 0))::numeric(10,2) AS avg_transaction_value,
        -- Demographics
        (100.0 * SUM(f.sex_male) / NULLIF(SUM(f.sex_male + f.sex_female), 0))::numeric(5,1) AS pct_male_visitors,
        (100.0 * SUM(f.age_15_29) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_young_visitors,
        -- Visitor mix
        (100.0 * SUM(COALESCE(f.swiss_tourists_raw, 0)) / 
            NULLIF(SUM(COALESCE(f.swiss_tourists_raw, 0) + COALESCE(f.foreign_tourists_raw, 0)), 0))::numeric(5,1) AS pct_swiss_tourists,
        (100.0 * SUM(COALESCE(f.swiss_locals_raw, 0)) / 
            NULLIF(SUM(COALESCE(f.swiss_tourists_raw, 0) + COALESCE(f.foreign_tourists_raw, 0) + 
            COALESCE(f.swiss_locals_raw, 0) + COALESCE(f.foreign_workers_raw, 0) + 
            COALESCE(f.swiss_commuters_raw, 0)), 0))::numeric(5,1) AS pct_locals,
        -- Industry metrics
        AVG(COALESCE(f.industry_count, 0))::numeric(5,1) AS avg_industries_per_record,
        -- Dwell time
        AVG(COALESCE(f.avg_dwell_time_mins, 0))::numeric(5,1) AS avg_dwell_time_mins,
        -- Data quality metrics
        COUNT(*) AS total_records,
        SUM(CASE WHEN f.spending_amount IS NOT NULL THEN 1 ELSE 0 END) AS records_with_spending,
        (100.0 * SUM(CASE WHEN f.spending_amount IS NOT NULL THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0))::numeric(5,1) AS pct_with_spending,
        AVG(COALESCE(f.data_quality_score, 0))::numeric(5,1) AS avg_data_quality_score,
        -- Region categorization
        CASE 
            WHEN SUM(f.visitor_count) > 100000 THEN 'High Volume'
            WHEN SUM(f.visitor_count) > 50000 THEN 'Medium Volume'
            ELSE 'Low Volume'
        END AS visitor_volume_category,
        -- Spending categorization
        CASE 
            WHEN SUM(COALESCE(f.spending_amount, 0)) > 1000000 THEN 'High Spend'
            WHEN SUM(COALESCE(f.spending_amount, 0)) > 500000 THEN 'Medium Spend'
            ELSE 'Low Spend'
        END AS spending_category,
        -- Metadata
        CURRENT_TIMESTAMP AS generated_at
    FROM edw.fact_tourism_unified f
    JOIN edw.dim_region r ON f.region_id = r.region_id 
    JOIN edw.dim_time dt ON f.date_id = dt.date_id
    GROUP BY r.region_name, r.region_type, r.central_latitude, r.central_longitude, r.bounding_box, dt.year
    ORDER BY total_visitors DESC;
    
    PERFORM edw.report_status('analysis_region_performance view created');

    -- Create the monitoring views
    DROP VIEW IF EXISTS edw.data_cardinality;
    CREATE OR REPLACE VIEW edw.data_cardinality AS
    SELECT
        'edw.dim_time' AS table_name,
        (SELECT COUNT(*) FROM edw.dim_time) AS row_count,
        'Dimension' AS table_type,
        'date_id' AS primary_key,
        NULL AS foreign_key_relationships
    UNION ALL
    SELECT
        'edw.dim_region' AS table_name,
        (SELECT COUNT(*) FROM edw.dim_region) AS row_count,
        'Dimension' AS table_type,
        'region_id' AS primary_key,
        'parent_region_id references edw.dim_region(region_id)' AS foreign_key_relationships
    UNION ALL
    SELECT
        'edw.dim_object_type' AS table_name,
        (SELECT COUNT(*) FROM edw.dim_object_type) AS row_count,
        'Dimension' AS table_type,
        'object_type_id' AS primary_key,
        NULL AS foreign_key_relationships
    UNION ALL
    SELECT
        'edw.dim_visit_type' AS table_name,
        (SELECT COUNT(*) FROM edw.dim_visit_type) AS row_count,
        'Dimension' AS table_type,
        'visit_type_id' AS primary_key,
        NULL AS foreign_key_relationships
    UNION ALL
    SELECT
        'edw.dim_data_type' AS table_name,
        (SELECT COUNT(*) FROM edw.dim_data_type) AS row_count,
        'Dimension' AS table_type,
        'data_type_id' AS primary_key,
        NULL AS foreign_key_relationships
    UNION ALL
    SELECT
        'edw.dim_industry' AS table_name,
        (SELECT COUNT(*) FROM edw.dim_industry) AS row_count,
        'Dimension' AS table_type,
        'industry_id' AS primary_key,
        NULL AS foreign_key_relationships
    UNION ALL
    SELECT
        'edw.fact_tourism_visitors' AS table_name,
        (SELECT COUNT(*) FROM edw.fact_tourism_visitors) AS row_count,
        'Fact' AS table_type,
        'fact_id' AS primary_key,
        'date_id, region_id, object_type_id, visit_type_id, data_type_id' AS foreign_key_relationships
    UNION ALL
    SELECT
        'edw.fact_tourism_spending' AS table_name,
        (SELECT COUNT(*) FROM edw.fact_tourism_spending) AS row_count,
        'Fact' AS table_type,
        'fact_id' AS primary_key,
        'date_id, region_id, object_type_id, visit_type_id, data_type_id' AS foreign_key_relationships
    UNION ALL
    SELECT
        'edw.fact_tourism_unified' AS table_name,
        (SELECT COUNT(*) FROM edw.fact_tourism_unified) AS row_count,
        'Fact' AS table_type,
        'fact_id' AS primary_key,
        'date_id, region_id, object_type_id, visit_type_id, data_type_id' AS foreign_key_relationships;
    
    PERFORM edw.report_status('data_cardinality view created');

    -- Create a view to monitor ETL execution history
    DROP VIEW IF EXISTS edw.etl_history;
    CREATE OR REPLACE VIEW edw.etl_history AS
    SELECT 
        etl_run_id,
        table_name,
        load_start_time,
        load_end_time,
        EXTRACT(EPOCH FROM (load_end_time - load_start_time))::numeric(10,2) AS duration_seconds,
        rows_processed,
        rows_inserted,
        rows_updated,
        load_status,
        error_message,
        -- Extract the message from status messages
        CASE WHEN table_name = 'status_message' THEN additional_info->>'message' ELSE NULL END AS status_message
    FROM edw.etl_metadata
    ORDER BY load_start_time DESC;
    
    PERFORM edw.report_status('etl_history view created');
    
    -- Summary of all ETL operations
    INSERT INTO edw.etl_metadata (
        table_name, 
        load_start_time, 
        load_end_time, 
        rows_processed,
        load_status, 
        additional_info
    )
    VALUES (
        'edw_optimization_complete',
        clock_timestamp() - interval '1 hour',
        clock_timestamp(),
        (
            SELECT COALESCE(SUM(row_count), 0) FROM edw.data_cardinality
        ),
        'COMPLETE',
        jsonb_build_object(
            'summary', 'Optimized EDW schema created successfully',
            'dimension_tables', (
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'edw' AND table_name LIKE 'dim_%'
            ),
            'fact_tables', (
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'edw' AND table_name LIKE 'fact_%'
            ),
            'views', (
                SELECT COUNT(*) FROM information_schema.views 
                WHERE table_schema = 'edw'
            )
        )
    );
    
    PERFORM edw.report_status('EDW optimization completed successfully');
END;
$;