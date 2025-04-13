-- Task 4: Process Tourism Spending Facts (Corrected version)
-- Drop the existing table or view if it exists
DO $$
BEGIN
    PERFORM edw.report_status('Task 4: Starting tourism spending facts processing');
    
    -- Check if the object exists as a table or view and drop it if requested
    IF EXISTS (SELECT 1 FROM information_schema.tables 
              WHERE table_schema = 'edw' AND table_name = 'fact_tourism_spending' 
              AND table_type = 'BASE TABLE') THEN
        PERFORM edw.report_status('Dropping existing fact_tourism_spending table');
        DROP TABLE IF EXISTS edw.fact_tourism_spending;
    ELSIF EXISTS (SELECT 1 FROM information_schema.views 
                 WHERE table_schema = 'edw' AND table_name = 'fact_tourism_spending') THEN
        PERFORM edw.report_status('Dropping existing fact_tourism_spending view');
        DROP VIEW IF EXISTS edw.fact_tourism_spending;
    END IF;
END;
$$;

-- Process Tourism Spending facts
DO $$
DECLARE
    start_time TIMESTAMP;
    table_creation_time TIMESTAMP;
    region_rec RECORD;
    total_regions INTEGER;
    processed_regions INTEGER := 0;
    region_count INTEGER;
    final_count INTEGER;
    mc_count INTEGER;
    industry_count INTEGER;
BEGIN
    -- Skip if already exists (check for both table and view)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'edw' AND table_name = 'fact_tourism_spending'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.views
        WHERE table_schema = 'edw' AND table_name = 'fact_tourism_spending'
    )
    THEN
        start_time := clock_timestamp();
        PERFORM edw.report_status('Creating fact_tourism_spending table');
        
        -- Check if industry dimension exists
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                      WHERE table_schema = 'edw' AND table_name = 'dim_industry') THEN
            PERFORM edw.report_status('Creating dim_industry table');
            
            -- Create industry dimension from MasterCard data if it doesn't exist
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
            WHERE industry IS NOT NULL
            GROUP BY industry;
            
            ALTER TABLE edw.dim_industry ADD PRIMARY KEY (industry_id);
            CREATE INDEX idx_dim_industry_name ON edw.dim_industry(industry_name);
            
            SELECT COUNT(*) INTO industry_count FROM edw.dim_industry;
            PERFORM edw.report_status('dim_industry created with ' || industry_count || ' industries');
        ELSE
            SELECT COUNT(*) INTO industry_count FROM edw.dim_industry;
            PERFORM edw.report_status('dim_industry already exists with ' || industry_count || ' industries');
        END IF;
        
        -- First, create the table structure
        PERFORM edw.report_status('Creating fact_tourism_spending table structure');
        table_creation_time := clock_timestamp();
        
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
        
        PERFORM edw.report_status('Table structure created in ' || 
                               EXTRACT(EPOCH FROM (clock_timestamp() - table_creation_time)) || ' seconds');
        
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
            
            BEGIN
                -- Prepare spending data for this region
                PERFORM edw.report_status('Processing region ' || processed_regions || '/' || total_regions || 
                                       ': ' || region_rec.region_name);
                                     
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
                PERFORM edw.report_status('Found ' || region_count || ' records for this region');
                
                IF region_count > 0 THEN
                    -- Get MasterCard data just for this region (with index-friendly conditions)
                    CREATE TEMP TABLE tmp_region_mc_data AS
                    SELECT 
                        id, txn_date, industry, txn_amt, txn_cnt, avg_ticket, acct_cnt, avg_freq, avg_spend_amt,
                        yr, geo_name
                    FROM data_lake.master_card
                    WHERE LOWER(geo_name) = LOWER(region_rec.region_name);
                    
                    GET DIAGNOSTICS mc_count = ROW_COUNT;
                    
                    PERFORM edw.report_status('Region ' || region_rec.region_name || 
                                           ': Found ' || mc_count || ' transactions');
                    
                    -- For regions with zero transactions, insert empty records
                    IF mc_count = 0 THEN
                        INSERT INTO edw.fact_tourism_spending (
                            fact_id, date_id, region_id, object_type_id, visit_type_id, data_type_id,
                            visitor_count, total_transaction_amount, total_transaction_count, 
                            average_transaction_all_industries, industry_count, total_transaction_records,
                            max_days_with_transactions, has_transaction_data, data_completion_pct, data_sources
                        )
                        SELECT
                            sd.fact_id,
                            sd.date_id,
                            sd.region_id,
                            sd.object_type_id,
                            sd.visit_type_id,
                            sd.data_type_id,
                            sd.visitor_count,
                            0 AS total_transaction_amount, -- No transactions
                            0 AS total_transaction_count,
                            NULL AS average_transaction_all_industries,
                            0 AS industry_count,
                            0 AS total_transaction_records,
                            0 AS max_days_with_transactions,
                            FALSE AS has_transaction_data,
                            0 AS data_completion_pct,
                            'inervista.fact_tourism' AS data_sources
                        FROM tmp_spending_data sd;
                        
                        PERFORM edw.report_status('Inserted empty data for region ' || region_rec.region_name || 
                                               ' [' || processed_regions || '/' || total_regions || ']');
                    ELSE
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
                                        ((sd.end_date - sd.start_date + 1)::integer)) * 100,
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
                    END IF;
                ELSE
                    PERFORM edw.report_status('Skipping region ' || region_rec.region_name || ' - no records found');
                END IF;
                
                -- Clean up temp tables for this region
                DROP TABLE IF EXISTS tmp_spending_data;
                DROP TABLE IF EXISTS tmp_region_mc_data;
                DROP TABLE IF EXISTS tmp_card_spending_data;
                
            EXCEPTION WHEN OTHERS THEN
                -- Log error and continue with next region
                PERFORM edw.report_status('Error processing region ' || region_rec.region_name || ': ' || SQLERRM);
                
                -- Clean up any temp tables that might exist
                DROP TABLE IF EXISTS tmp_spending_data;
                DROP TABLE IF EXISTS tmp_region_mc_data;
                DROP TABLE IF EXISTS tmp_card_spending_data;
            END;
        END LOOP;
        
        -- Create indexes after all data is loaded
        PERFORM edw.report_status('Creating indexes on fact_tourism_spending');
        CREATE INDEX idx_fts_dimensions ON edw.fact_tourism_spending(date_id, region_id, object_type_id, visit_type_id, data_type_id);
        CREATE INDEX idx_fts_date ON edw.fact_tourism_spending(date_id);
        CREATE INDEX idx_fts_region ON edw.fact_tourism_spending(region_id);
        CREATE INDEX idx_fts_has_transaction_data ON edw.fact_tourism_spending(has_transaction_data);
        
        SELECT COUNT(*) INTO final_count FROM edw.fact_tourism_spending;
        PERFORM edw.report_status('fact_tourism_spending created with ' || final_count || ' rows');
        PERFORM edw.report_status('Processing time: ' || 
                               EXTRACT(EPOCH FROM (clock_timestamp() - start_time))/60 || ' minutes');
                               
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
            'fact_tourism_spending',
            start_time,
            clock_timestamp(),
            total_regions,
            final_count,
            'COMPLETE',
            jsonb_build_object(
                'processing_time_minutes', EXTRACT(EPOCH FROM (clock_timestamp() - start_time))/60,
                'data_sources', 'inervista.fact_tourism + data_lake.master_card',
                'processed_regions', processed_regions,
                'industry_count', industry_count,
                'records_with_transactions', (SELECT COUNT(*) FROM edw.fact_tourism_spending WHERE has_transaction_data = TRUE),
                'avg_data_completion', (SELECT AVG(data_completion_pct) FROM edw.fact_tourism_spending)
            )
        );
        
        PERFORM edw.report_status('Task 4: Tourism spending facts processing completed successfully');
    ELSE
        PERFORM edw.report_status('fact_tourism_spending already exists - skipping processing');
        
        -- Provide summary of existing table/view
        PERFORM edw.report_status('Summary of existing fact_tourism_spending:');
        
        -- Type of object
        IF EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'edw' AND table_name = 'fact_tourism_spending' 
                  AND table_type = 'BASE TABLE') THEN
            PERFORM edw.report_status('Object type: Table');
        ELSE
            PERFORM edw.report_status('Object type: View');
        END IF;
        
        -- Count total rows
        PERFORM edw.report_status('Total rows: ' || (SELECT COUNT(*) FROM edw.fact_tourism_spending));
        
        -- Check if has_transaction_data column exists before accessing it
        IF EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_schema = 'edw' AND table_name = 'fact_tourism_spending' 
                  AND column_name = 'has_transaction_data') THEN
            PERFORM edw.report_status('Records with transaction data: ' || 
                                 (SELECT COUNT(*) FROM edw.fact_tourism_spending WHERE has_transaction_data = TRUE));
        ELSE
            PERFORM edw.report_status('Records with transaction data: Not available (column does not exist)');
        END IF;
        
        -- Check if data_completion_pct column exists before accessing it
        IF EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_schema = 'edw' AND table_name = 'fact_tourism_spending' 
                  AND column_name = 'data_completion_pct') THEN
            PERFORM edw.report_status('Average data completion: ' || 
                                 (SELECT ROUND(AVG(data_completion_pct), 2) FROM edw.fact_tourism_spending) || '%');
        ELSE
            PERFORM edw.report_status('Average data completion: Not available (column does not exist)');
        END IF;
    END IF;
END;
$$;