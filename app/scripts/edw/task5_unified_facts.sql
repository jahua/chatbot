-- Task 5: Create Unified Tourism Fact Table
-- Drop the existing table or view if it exists
DO $$
BEGIN
    PERFORM edw.report_status('Task 5: Starting unified fact table processing');
    
    -- Check if the object exists as a table or view and drop it if requested
    IF EXISTS (SELECT 1 FROM information_schema.tables 
              WHERE table_schema = 'edw' AND table_name = 'fact_tourism_unified' 
              AND table_type = 'BASE TABLE') THEN
        PERFORM edw.report_status('Dropping existing fact_tourism_unified table');
        DROP TABLE IF EXISTS edw.fact_tourism_unified;
    ELSIF EXISTS (SELECT 1 FROM information_schema.views 
                 WHERE table_schema = 'edw' AND table_name = 'fact_tourism_unified') THEN
        PERFORM edw.report_status('Dropping existing fact_tourism_unified view');
        DROP VIEW IF EXISTS edw.fact_tourism_unified;
    END IF;
END;
$$;

-- Create the unified fact table
DO $$
DECLARE
    start_time TIMESTAMP;
    final_count INTEGER;
    visitor_count INTEGER;
    spending_count INTEGER;
    missing_visitors INTEGER;
    missing_spending INTEGER;
BEGIN
    -- Skip if already exists (check for both table and view)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'edw' AND table_name = 'fact_tourism_unified'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.views
        WHERE table_schema = 'edw' AND table_name = 'fact_tourism_unified'
    )
    THEN
        start_time := clock_timestamp();
        
        -- First check that the prerequisite tables exist
        PERFORM edw.report_status('Verifying prerequisite tables');
        
        -- Count rows in prerequisite tables and report
        SELECT COUNT(*) INTO visitor_count FROM edw.fact_tourism_visitors;
        SELECT COUNT(*) INTO spending_count FROM edw.fact_tourism_spending;
        
        PERFORM edw.report_status('Found ' || visitor_count || ' visitor fact records');
        PERFORM edw.report_status('Found ' || spending_count || ' spending fact records');
        
        -- Check for any facts missing from either table
        SELECT COUNT(*) INTO missing_visitors 
        FROM inervista.fact_tourism f
        LEFT JOIN edw.fact_tourism_visitors v ON f.fact_id = v.fact_id
        WHERE v.fact_id IS NULL;
        
        SELECT COUNT(*) INTO missing_spending
        FROM inervista.fact_tourism f
        LEFT JOIN edw.fact_tourism_spending s ON f.fact_id = s.fact_id
        WHERE s.fact_id IS NULL;
        
        IF missing_visitors > 0 THEN
            PERFORM edw.report_status('Warning: ' || missing_visitors || ' fact records missing from visitor facts');
        END IF;
        
        IF missing_spending > 0 THEN
            PERFORM edw.report_status('Warning: ' || missing_spending || ' fact records missing from spending facts');
        END IF;
        
        -- Create the unified fact table
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
            
            -- Demographics from visitor fact - dynamically identify and include columns from the visitors fact
            COALESCE(ftv.staydays, 0) AS staydays,
            COALESCE(ftv.sex_male, 0) AS sex_male,
            COALESCE(ftv.sex_female, 0) AS sex_female,
            COALESCE(ftv.age_15_29, 0) AS age_15_29,
            COALESCE(ftv.age_30_44, 0) AS age_30_44,
            COALESCE(ftv.age_45_59, 0) AS age_45_59,
            COALESCE(ftv.age_60_plus, 0) AS age_60_plus,
            
            -- Raw metrics from visitor fact
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
            COALESCE(ftv.data_completion_pct, 0) AS visitor_data_completion_pct,
            COALESCE(fts.data_completion_pct, 0) AS spending_data_completion_pct,
            
            -- Calculate overall data quality score (0-100)
            (
                COALESCE(CASE WHEN ftv.has_raw_data_match THEN 50 ELSE 0 END, 0) + 
                COALESCE(CASE WHEN fts.has_transaction_data THEN 50 ELSE 0 END, 0)
            ) AS data_quality_score,
            
            -- Quality flags
            CASE 
                WHEN COALESCE(fts.has_transaction_data, FALSE) AND COALESCE(ftv.has_raw_data_match, FALSE) THEN 'complete' -- Has spending and raw data
                WHEN COALESCE(fts.has_transaction_data, FALSE) OR COALESCE(ftv.has_raw_data_match, FALSE) THEN 'partial'  -- Has spending OR raw data
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
        -- Joins to materialized fact tables (LEFT JOIN to handle missing data)
        LEFT JOIN edw.fact_tourism_visitors ftv ON f.fact_id = ftv.fact_id 
        LEFT JOIN edw.fact_tourism_spending fts ON f.fact_id = fts.fact_id;

        GET DIAGNOSTICS final_count = ROW_COUNT;
        
        PERFORM edw.report_status('Creating indexes on fact_tourism_unified');
        ALTER TABLE edw.fact_tourism_unified ADD PRIMARY KEY (fact_id);
        CREATE INDEX idx_ftu_dimensions ON edw.fact_tourism_unified(date_id, region_id, object_type_id, visit_type_id, data_type_id);
        CREATE INDEX idx_ftu_date_region ON edw.fact_tourism_unified(date_id, region_id);
        CREATE INDEX idx_ftu_completeness ON edw.fact_tourism_unified(data_completeness);
        CREATE INDEX idx_ftu_geo ON edw.fact_tourism_unified(central_latitude, central_longitude) 
            WHERE central_latitude IS NOT NULL AND central_longitude IS NOT NULL;
        
        PERFORM edw.report_status('fact_tourism_unified created with ' || final_count || ' rows');
        
        -- Calculate data quality metrics
        PERFORM edw.report_status('Data quality summary:');
        PERFORM edw.report_status('Complete records: ' || 
            (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'complete'));
        PERFORM edw.report_status('Partial records: ' || 
            (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'partial'));
        PERFORM edw.report_status('Minimal records: ' || 
            (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'minimal'));
        PERFORM edw.report_status('Average quality score: ' || 
            (SELECT ROUND(AVG(data_quality_score), 2) FROM edw.fact_tourism_unified));
        
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
            'fact_tourism_unified',
            start_time,
            clock_timestamp(),
            visitor_count + spending_count,
            final_count,
            'COMPLETE',
            jsonb_build_object(
                'processing_time_minutes', EXTRACT(EPOCH FROM (clock_timestamp() - start_time))/60,
                'visitor_facts_count', visitor_count,
                'spending_facts_count', spending_count,
                'missing_visitor_facts', missing_visitors,
                'missing_spending_facts', missing_spending,
                'data_quality', jsonb_build_object(
                    'complete_records', (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'complete'),
                    'partial_records', (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'partial'),
                    'minimal_records', (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'minimal'),
                    'avg_quality_score', (SELECT ROUND(AVG(data_quality_score), 2) FROM edw.fact_tourism_unified)
                )
            )
        );
        
        PERFORM edw.report_status('Task 5: Unified fact table processing completed successfully');
    ELSE
        PERFORM edw.report_status('fact_tourism_unified already exists - skipping processing');
        
        -- Provide summary of existing table/view
        PERFORM edw.report_status('Summary of existing fact_tourism_unified:');
        
        -- Type of object
        IF EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'edw' AND table_name = 'fact_tourism_unified' 
                  AND table_type = 'BASE TABLE') THEN
            PERFORM edw.report_status('Object type: Table');
        ELSE
            PERFORM edw.report_status('Object type: View');
        END IF;
        
        -- Count total rows
        PERFORM edw.report_status('Total rows: ' || (SELECT COUNT(*) FROM edw.fact_tourism_unified));
        
        -- Data quality metrics
        PERFORM edw.report_status('Data quality summary:');
        PERFORM edw.report_status('Complete records: ' || 
            (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'complete'));
        PERFORM edw.report_status('Partial records: ' || 
            (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'partial'));
        PERFORM edw.report_status('Minimal records: ' || 
            (SELECT COUNT(*) FROM edw.fact_tourism_unified WHERE data_completeness = 'minimal'));
        
        -- Check if data_quality_score column exists before accessing it
        IF EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_schema = 'edw' AND table_name = 'fact_tourism_unified' 
                  AND column_name = 'data_quality_score') THEN
            PERFORM edw.report_status('Average quality score: ' || 
                                 (SELECT ROUND(AVG(data_quality_score), 2) FROM edw.fact_tourism_unified));
        ELSE
            PERFORM edw.report_status('Average quality score: Not available (column does not exist)');
        END IF;
    END IF;
END;
$$;