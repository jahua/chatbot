-- ==============================================
-- OPTIMIZED ETL Script for AOI Days Data
-- Efficient and robust loading to DW schema
-- PostgreSQL compatible
-- ==============================================

-- ----------------------
-- HELPER FUNCTIONS
-- ----------------------

-- Function to safely insert a region and return its ID 
CREATE OR REPLACE FUNCTION dw.safe_insert_region(
    p_region_name TEXT,
    p_region_type TEXT DEFAULT 'tourism_region'
) RETURNS INTEGER AS $$
DECLARE
    v_region_id INTEGER;
BEGIN
    -- First check if the region exists with any type
    SELECT region_id INTO v_region_id
    FROM dw.dim_region
    WHERE LOWER(region_name) = LOWER(p_region_name);
    
    -- If found, return the ID
    IF v_region_id IS NOT NULL THEN
        RETURN v_region_id;
    END IF;
    
    -- Otherwise insert the new region
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        is_active,
        created_at
    ) VALUES (
        p_region_name,
        p_region_type,
        TRUE,
        CURRENT_TIMESTAMP
    )
    RETURNING region_id INTO v_region_id;
    
    RETURN v_region_id;
EXCEPTION WHEN unique_violation THEN
    -- If a unique constraint violation occurs, try to find the existing region
    SELECT region_id INTO v_region_id
    FROM dw.dim_region
    WHERE LOWER(region_name) = LOWER(p_region_name);
    
    RETURN v_region_id;
END;
$$ LANGUAGE plpgsql;

-- Function to ensure visitor types exist in the dimension table
CREATE OR REPLACE FUNCTION dw.ensure_visitor_types_exist() 
RETURNS VOID AS $$
BEGIN
    -- Make sure we have the basic visitor types for AOI data
    INSERT INTO dw.dim_visitor_type (
        visitor_code,
        visitor_name,
        visitor_category,
        visitor_subcategory,
        is_domestic,
        is_overnight,
        is_business,
        description,
        is_active,
        valid_from,
        valid_to,
        created_at
    )
    VALUES
        ('swiss_tourists', 'Swiss Tourists', 'tourist', 'domestic', TRUE, FALSE, FALSE, 
         'Swiss visitors from outside the local area', TRUE, '2000-01-01', '9999-12-31', CURRENT_TIMESTAMP),
        ('foreign_tourists', 'Foreign Tourists', 'tourist', 'international', FALSE, FALSE, FALSE, 
         'Non-Swiss visitors', TRUE, '2000-01-01', '9999-12-31', CURRENT_TIMESTAMP),
        ('swiss_locals', 'Swiss Locals', 'resident', 'local', TRUE, FALSE, FALSE, 
         'Swiss residents from the local area', TRUE, '2000-01-01', '9999-12-31', CURRENT_TIMESTAMP),
        ('swiss_commuters', 'Swiss Commuters', 'commuter', 'domestic', TRUE, FALSE, TRUE, 
         'Swiss visitors commuting for work', TRUE, '2000-01-01', '9999-12-31', CURRENT_TIMESTAMP),
        ('foreign_workers', 'Foreign Workers', 'worker', 'international', FALSE, FALSE, TRUE, 
         'Non-Swiss visitors working in the area', TRUE, '2000-01-01', '9999-12-31', CURRENT_TIMESTAMP)
    ON CONFLICT (visitor_code) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Function to ensure countries exist in the dimension table
CREATE OR REPLACE FUNCTION dw.ensure_country_exists(p_country_name TEXT) 
RETURNS INTEGER AS $$
DECLARE
    v_country_id INTEGER;
    v_normalized_name TEXT;
    v_country_code TEXT;
BEGIN
    -- Normalize the country name
    v_normalized_name := TRIM(INITCAP(p_country_name));
    
    -- First check if the country exists with any name
    SELECT country_id INTO v_country_id
    FROM dw.dim_country
    WHERE LOWER(country_name) = LOWER(v_normalized_name);
    
    -- If found, return the ID
    IF v_country_id IS NOT NULL THEN
        RETURN v_country_id;
    END IF;
    
    -- Generate a country code (first two letters of the country name)
    v_country_code := UPPER(LEFT(v_normalized_name, 2));
    
    -- If country doesn't exist, insert it
        INSERT INTO dw.dim_country (
            country_code,
        country_name,
            is_active,
            created_at
        ) VALUES (
        v_country_code,
            v_normalized_name,
            TRUE,
            CURRENT_TIMESTAMP
        )
    ON CONFLICT (country_code) DO UPDATE SET
        country_name = CASE 
            WHEN dim_country.country_name = dim_country.country_code THEN EXCLUDED.country_name
            ELSE dim_country.country_name
        END
        RETURNING country_id INTO v_country_id;
    
    RETURN v_country_id;
EXCEPTION WHEN unique_violation THEN
    -- If a unique constraint violation occurs, try to find the existing country
    SELECT country_id INTO v_country_id
    FROM dw.dim_country
    WHERE LOWER(country_name) = LOWER(v_normalized_name);
    
    IF v_country_id IS NULL THEN
        -- If still not found, try a different code by appending a number
        v_country_code := UPPER(LEFT(v_normalized_name, 1) || RIGHT(v_normalized_name, 1));
        
        INSERT INTO dw.dim_country (
            country_code,
            country_name,
            is_active,
            created_at
        ) VALUES (
            v_country_code,
            v_normalized_name,
            TRUE,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (country_code) DO NOTHING
        RETURNING country_id INTO v_country_id;
        
        IF v_country_id IS NULL THEN
            -- Last resort: Use country name as the "code" (it will be truncated to 2 chars)
            INSERT INTO dw.dim_country (
                country_code,
                country_name,
                is_active,
                created_at
            ) VALUES (
                LEFT(UPPER(REPLACE(v_normalized_name, ' ', '')), 2),
                v_normalized_name,
                TRUE,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (country_code) DO NOTHING
            RETURNING country_id INTO v_country_id;
        END IF;
    END IF;
    
    RETURN v_country_id;
END;
$$ LANGUAGE plpgsql;

-- Function to ensure cantons exist in the dimension table
CREATE OR REPLACE FUNCTION dw.ensure_canton_exists(p_canton_name TEXT) 
RETURNS INTEGER AS $$
DECLARE
    v_canton_id INTEGER;
    v_normalized_name TEXT;
    v_canton_code TEXT;
BEGIN
    -- Normalize the canton name
    v_normalized_name := TRIM(p_canton_name);
    
    -- Check if canton exists
    SELECT canton_id INTO v_canton_id
    FROM dw.dim_canton
    WHERE LOWER(canton_name) = LOWER(v_normalized_name);
    
    -- If not found, insert it
    IF v_canton_id IS NULL THEN
        -- Generate a simple canton code
        v_canton_code := UPPER(LEFT(v_normalized_name, 2));
        
        -- Handle special cases for canton codes
        IF LOWER(v_normalized_name) = 'zürich' OR LOWER(v_normalized_name) = 'zurich' THEN
            v_canton_code := 'ZH';
        ELSIF LOWER(v_normalized_name) = 'bern' OR LOWER(v_normalized_name) = 'berne' THEN
            v_canton_code := 'BE';
        ELSIF LOWER(v_normalized_name) = 'luzern' OR LOWER(v_normalized_name) = 'lucerne' THEN
            v_canton_code := 'LU';
        ELSIF LOWER(v_normalized_name) = 'uri' THEN
            v_canton_code := 'UR';
        ELSIF LOWER(v_normalized_name) = 'schwyz' THEN
            v_canton_code := 'SZ';
        ELSIF LOWER(v_normalized_name) = 'obwalden' THEN
            v_canton_code := 'OW';
        ELSIF LOWER(v_normalized_name) = 'nidwalden' THEN
            v_canton_code := 'NW';
        ELSIF LOWER(v_normalized_name) = 'glarus' THEN
            v_canton_code := 'GL';
        ELSIF LOWER(v_normalized_name) = 'zug' THEN
            v_canton_code := 'ZG';
        ELSIF LOWER(v_normalized_name) = 'fribourg' OR LOWER(v_normalized_name) = 'freiburg' THEN
            v_canton_code := 'FR';
        ELSIF LOWER(v_normalized_name) = 'solothurn' THEN
            v_canton_code := 'SO';
        ELSIF LOWER(v_normalized_name) = 'basel-stadt' THEN
            v_canton_code := 'BS';
        ELSIF LOWER(v_normalized_name) = 'basel-landschaft' THEN
            v_canton_code := 'BL';
        ELSIF LOWER(v_normalized_name) = 'schaffhausen' THEN
            v_canton_code := 'SH';
        ELSIF LOWER(v_normalized_name) = 'appenzell ausserrhoden' THEN
            v_canton_code := 'AR';
        ELSIF LOWER(v_normalized_name) = 'appenzell innerrhoden' THEN
            v_canton_code := 'AI';
        ELSIF LOWER(v_normalized_name) = 'st. gallen' OR LOWER(v_normalized_name) = 'saint gallen' THEN
            v_canton_code := 'SG';
        ELSIF LOWER(v_normalized_name) = 'graubünden' OR LOWER(v_normalized_name) = 'graubunden' OR LOWER(v_normalized_name) = 'grisons' THEN
            v_canton_code := 'GR';
        ELSIF LOWER(v_normalized_name) = 'aargau' THEN
            v_canton_code := 'AG';
        ELSIF LOWER(v_normalized_name) = 'thurgau' THEN
            v_canton_code := 'TG';
        ELSIF LOWER(v_normalized_name) = 'ticino' THEN
            v_canton_code := 'TI';
        ELSIF LOWER(v_normalized_name) = 'vaud' THEN
            v_canton_code := 'VD';
        ELSIF LOWER(v_normalized_name) = 'valais' OR LOWER(v_normalized_name) = 'wallis' THEN
            v_canton_code := 'VS';
        ELSIF LOWER(v_normalized_name) = 'neuchâtel' OR LOWER(v_normalized_name) = 'neuchatel' THEN
            v_canton_code := 'NE';
        ELSIF LOWER(v_normalized_name) = 'genève' OR LOWER(v_normalized_name) = 'geneva' OR LOWER(v_normalized_name) = 'geneve' THEN
            v_canton_code := 'GE';
        ELSIF LOWER(v_normalized_name) = 'jura' THEN
            v_canton_code := 'JU';
        END IF;
        
        INSERT INTO dw.dim_canton (
            canton_name,
            canton_code,
            is_active,
                created_at
            ) VALUES (
            v_normalized_name,
            v_canton_code,
            TRUE,
                CURRENT_TIMESTAMP
            )
        RETURNING canton_id INTO v_canton_id;
    END IF;
    
    RETURN v_canton_id;
EXCEPTION WHEN unique_violation THEN
    -- If a unique constraint violation occurs, try to find the existing canton
    SELECT canton_id INTO v_canton_id
    FROM dw.dim_canton
    WHERE LOWER(canton_name) = LOWER(v_normalized_name);
    
    RETURN v_canton_id;
END;
$$ LANGUAGE plpgsql;

-- ----------------------
-- SUPPLEMENTARY TABLES
-- ----------------------

-- Create supplementary tables if they don't exist
DO $$
BEGIN
    -- AOI demographics table
    IF NOT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'dw' 
        AND table_name = 'aoi_visitor_demographics'
    ) THEN
        CREATE TABLE dw.aoi_visitor_demographics (
            date_id INTEGER NOT NULL,
            region_id INTEGER NOT NULL,
            male_proportion NUMERIC,
            female_proportion NUMERIC,
            age_15_29_pct NUMERIC,
            age_30_44_pct NUMERIC,
            age_45_59_pct NUMERIC,
            age_60_plus_pct NUMERIC,
            avg_dwell_time_mins NUMERIC,
            source_aoi_id TEXT,
            etl_id INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date_id, region_id)
        );
    ELSE
        -- Ensure etl_id column exists if table exists
        ALTER TABLE dw.aoi_visitor_demographics ADD COLUMN IF NOT EXISTS etl_id INTEGER;
    END IF;

    -- AOI countries table
    IF NOT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'dw' 
        AND table_name = 'aoi_visitor_countries'
    ) THEN
        CREATE TABLE dw.aoi_visitor_countries (
            date_id INTEGER NOT NULL,
            region_id INTEGER NOT NULL,
            country_id INTEGER NOT NULL,
            country_name TEXT NOT NULL,
            visitor_count INTEGER NOT NULL,
            rank_order INTEGER,
            source_aoi_id TEXT,
            etl_id INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date_id, region_id, country_id)
        );
    ELSE
        -- Ensure etl_id column exists if table exists
        ALTER TABLE dw.aoi_visitor_countries ADD COLUMN IF NOT EXISTS etl_id INTEGER;
    END IF;
        
    -- Add index for better performance
    CREATE INDEX IF NOT EXISTS idx_aoi_visitor_countries_date_region 
    ON dw.aoi_visitor_countries(date_id, region_id);

    -- AOI cantons table
    IF NOT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'dw' 
        AND table_name = 'aoi_visitor_cantons'
    ) THEN
        CREATE TABLE dw.aoi_visitor_cantons (
            date_id INTEGER NOT NULL,
            region_id INTEGER NOT NULL,
            canton_id INTEGER NOT NULL,
            canton_name TEXT NOT NULL,
            visitor_count INTEGER NOT NULL,
            rank_order INTEGER,
            source_aoi_id TEXT,
            etl_id INTEGER,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date_id, region_id, canton_id)
        );
    ELSE
        -- Ensure etl_id column exists if table exists
        ALTER TABLE dw.aoi_visitor_cantons ADD COLUMN IF NOT EXISTS etl_id INTEGER;
    END IF;
        
    -- Add index for better performance
    CREATE INDEX IF NOT EXISTS idx_aoi_visitor_cantons_date_region 
    ON dw.aoi_visitor_cantons(date_id, region_id);
END;
$$;

-- ----------------------
-- MAIN LOADING FUNCTION
-- ----------------------

-- Drop old function version to avoid ambiguity
DROP FUNCTION IF EXISTS dw.load_aoi_days_data(DATE, DATE, BOOLEAN);
DROP FUNCTION IF EXISTS dw.load_aoi_days_data(DATE, DATE);
DROP FUNCTION IF EXISTS dw.load_aoi_days_data();

-- Create the updated function with explicit parameter types
CREATE OR REPLACE FUNCTION dw.load_aoi_days_data(
    p_date_from DATE DEFAULT NULL,
    p_date_to DATE DEFAULT NULL,
    p_bulk_insert BOOLEAN DEFAULT TRUE
)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER := 0;
    v_etl_id INTEGER;
    v_visitor_type_id INTEGER;
    v_region_id INTEGER;
    v_date_id INTEGER;
    v_country_id INTEGER;
    v_canton_id INTEGER;
    v_countries_added INTEGER := 0;
    v_cantons_added INTEGER := 0;
    v_fact_visitors_added INTEGER := 0;
    v_record RECORD;
    v_country_record RECORD;
    v_canton_record RECORD;
    v_start_time TIMESTAMP;
    v_exec_time_ms NUMERIC;
    v_status_message TEXT;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Set default date range if not provided
    IF p_date_from IS NULL THEN
        p_date_from := CURRENT_DATE - INTERVAL '30 days';
    END IF;
    
    IF p_date_to IS NULL THEN
        p_date_to := CURRENT_DATE - INTERVAL '1 day';
    END IF;
    
    -- Ensure we have the visitor types needed
    PERFORM dw.ensure_visitor_types_exist();
    
    -- Record the ETL run start and get the generated etl_id
    INSERT INTO dw.etl_metadata (
        process_name,
        source_system,
        status_code,
        start_time
    ) VALUES (
        'load_aoi_days_data',
        'aoi',
        'RUNNING',
        v_start_time
    ) RETURNING etl_id INTO v_etl_id;

    -- Cache visitor type IDs for better performance
    CREATE TEMP TABLE IF NOT EXISTS temp_visitor_types (
        visitor_code TEXT PRIMARY KEY,
        visitor_type_id INTEGER
    ) ON COMMIT DROP;
    
    TRUNCATE temp_visitor_types;
    
    INSERT INTO temp_visitor_types
    SELECT visitor_code, visitor_type_id
    FROM dw.dim_visitor_type
    WHERE visitor_code IN ('swiss_tourists', 'foreign_tourists', 'swiss_locals', 'swiss_commuters', 'foreign_workers');
    
    -- Process each record from the raw data
    FOR v_record IN 
        SELECT * FROM data_lake.aoi_days_raw
        WHERE aoi_date BETWEEN p_date_from AND p_date_to
    LOOP
        -- Map region - we'll use AOI ID as region name if this is a new region
        v_region_id := dw.safe_insert_region(v_record.aoi_id, 'tourism_region');
        
        IF v_region_id IS NULL THEN
            RAISE NOTICE 'Failed to map region for AOI ID: %', v_record.aoi_id;
            CONTINUE;
        END IF;
        
        -- Convert date to integer format YYYYMMDD
        v_date_id := TO_CHAR(v_record.aoi_date, 'YYYYMMDD')::INTEGER;
        
        -- Now we need to insert a record for each visitor type
        -- First, Swiss Tourists
        SELECT visitor_type_id INTO v_visitor_type_id
        FROM temp_visitor_types
        WHERE visitor_code = 'swiss_tourists';
        
        IF (v_record.visitors->>'swissTourist')::INTEGER > 0 THEN
            INSERT INTO dw.fact_visitors (
                date_id,
                region_id,
                visitor_type_id,
                visitor_count,
                source_system,
                created_at
            ) VALUES (
                v_date_id,
                v_region_id,
                v_visitor_type_id,
                (v_record.visitors->>'swissTourist')::INTEGER,
                'aoi',
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (date_id, region_id, visitor_type_id, source_system) 
            DO UPDATE SET
                visitor_count = EXCLUDED.visitor_count,
                created_at = CURRENT_TIMESTAMP;
                
            v_fact_visitors_added := v_fact_visitors_added + 1;
            END IF;
            
        -- Foreign Tourists
        SELECT visitor_type_id INTO v_visitor_type_id
        FROM temp_visitor_types
        WHERE visitor_code = 'foreign_tourists';
        
        IF (v_record.visitors->>'foreignTourist')::INTEGER > 0 THEN
            INSERT INTO dw.fact_visitors (
                date_id,
                region_id,
                visitor_type_id,
                visitor_count,
                source_system,
                created_at
            ) VALUES (
                v_date_id,
                v_region_id,
                v_visitor_type_id,
                (v_record.visitors->>'foreignTourist')::INTEGER,
                'aoi',
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (date_id, region_id, visitor_type_id, source_system) 
            DO UPDATE SET
                visitor_count = EXCLUDED.visitor_count,
                created_at = CURRENT_TIMESTAMP;
                
            v_fact_visitors_added := v_fact_visitors_added + 1;
        END IF;
        
        -- Swiss Locals
        SELECT visitor_type_id INTO v_visitor_type_id
        FROM temp_visitor_types
        WHERE visitor_code = 'swiss_locals';
        
        IF (v_record.visitors->>'swissLocal')::INTEGER > 0 THEN
            INSERT INTO dw.fact_visitors (
                date_id,
                region_id,
                visitor_type_id,
                visitor_count,
                source_system,
                created_at
            ) VALUES (
                v_date_id,
                v_region_id,
                v_visitor_type_id,
                (v_record.visitors->>'swissLocal')::INTEGER,
                'aoi',
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (date_id, region_id, visitor_type_id, source_system) 
            DO UPDATE SET
                visitor_count = EXCLUDED.visitor_count,
                created_at = CURRENT_TIMESTAMP;
                
            v_fact_visitors_added := v_fact_visitors_added + 1;
        END IF;
        
        -- Foreign Workers
        SELECT visitor_type_id INTO v_visitor_type_id
        FROM temp_visitor_types
        WHERE visitor_code = 'foreign_workers';
        
        IF (v_record.visitors->>'foreignWorker')::INTEGER > 0 THEN
            INSERT INTO dw.fact_visitors (
                date_id,
                region_id,
                visitor_type_id,
                visitor_count,
                source_system,
                created_at
            ) VALUES (
                v_date_id,
                v_region_id,
                v_visitor_type_id,
                (v_record.visitors->>'foreignWorker')::INTEGER,
                'aoi',
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (date_id, region_id, visitor_type_id, source_system) 
            DO UPDATE SET
                visitor_count = EXCLUDED.visitor_count,
                created_at = CURRENT_TIMESTAMP;
                
            v_fact_visitors_added := v_fact_visitors_added + 1;
        END IF;
        
        -- Swiss Commuters
        SELECT visitor_type_id INTO v_visitor_type_id
        FROM temp_visitor_types
        WHERE visitor_code = 'swiss_commuters';
        
        IF (v_record.visitors->>'swissCommuter')::INTEGER > 0 THEN
            INSERT INTO dw.fact_visitors (
                date_id,
                region_id,
                visitor_type_id,
                visitor_count,
                source_system,
                created_at
            ) VALUES (
                v_date_id,
                v_region_id,
                v_visitor_type_id,
                (v_record.visitors->>'swissCommuter')::INTEGER,
                'aoi',
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (date_id, region_id, visitor_type_id, source_system) 
            DO UPDATE SET
                visitor_count = EXCLUDED.visitor_count,
                created_at = CURRENT_TIMESTAMP;
                
            v_fact_visitors_added := v_fact_visitors_added + 1;
        END IF;
        
        v_count := v_count + 1;
        
        -- Process and insert country data
        FOR v_country_record IN 
            SELECT 
                country->>'name' AS country_name,
                (country->>'visitors')::INTEGER AS visitor_count,
                idx AS rank_order
            FROM jsonb_array_elements(v_record.top_foreign_countries) WITH ORDINALITY AS arr(country, idx)
        LOOP
            -- Ensure country exists in dimension table
            v_country_id := dw.ensure_country_exists(v_country_record.country_name);
            
            IF v_country_id IS NULL THEN
                RAISE NOTICE 'Failed to map country: %', v_country_record.country_name;
                CONTINUE;
            END IF;
            
            -- Insert country data
            INSERT INTO dw.aoi_visitor_countries (
                date_id,
                region_id,
                country_id,
                country_name,
                visitor_count,
                rank_order,
                source_aoi_id,
                etl_id,
                created_at
            ) VALUES (
                v_date_id,
                v_region_id,
                v_country_id,
                v_country_record.country_name,
                v_country_record.visitor_count,
                v_country_record.rank_order,
                v_record.aoi_id,
                v_etl_id,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (date_id, region_id, country_id) 
            DO UPDATE SET
                visitor_count = EXCLUDED.visitor_count,
                rank_order = EXCLUDED.rank_order,
                etl_id = EXCLUDED.etl_id,
                created_at = CURRENT_TIMESTAMP;
                
            v_countries_added := v_countries_added + 1;
        END LOOP;
        
        -- Process each canton
        FOR v_canton_record IN 
            SELECT 
                canton->>'name' AS canton_name,
                (canton->>'visitors')::INTEGER AS visitor_count,
                idx AS rank_order
            FROM jsonb_array_elements(v_record.top_swiss_cantons) WITH ORDINALITY AS arr(canton, idx)
        LOOP
            -- Ensure canton exists in dimension table
            v_canton_id := dw.ensure_canton_exists(v_canton_record.canton_name);
            
            IF v_canton_id IS NULL THEN
                RAISE NOTICE 'Failed to map canton: %', v_canton_record.canton_name;
                CONTINUE;
            END IF;
            
            -- Insert canton data
            INSERT INTO dw.aoi_visitor_cantons (
                date_id,
                region_id,
                canton_id,
                canton_name,
                visitor_count,
                rank_order,
                source_aoi_id,
                etl_id,
                created_at
            ) VALUES (
                v_date_id,
                v_region_id,
                v_canton_id,
                v_canton_record.canton_name,
                v_canton_record.visitor_count,
                v_canton_record.rank_order,
                v_record.aoi_id,
                v_etl_id,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (date_id, region_id, canton_id) 
            DO UPDATE SET
                visitor_count = EXCLUDED.visitor_count,
                rank_order = EXCLUDED.rank_order,
                etl_id = EXCLUDED.etl_id,
                created_at = CURRENT_TIMESTAMP;
                
            v_cantons_added := v_cantons_added + 1;
    END LOOP;
    
    -- Insert demographics data 
    INSERT INTO dw.aoi_visitor_demographics (
        date_id,
        region_id,
        male_proportion,
        female_proportion,
        age_15_29_pct,
        age_30_44_pct,
        age_45_59_pct,
        age_60_plus_pct,
        avg_dwell_time_mins,
        source_aoi_id,
            etl_id,
        created_at
        ) VALUES (
            v_date_id,
            v_region_id,
            (v_record.demographics->>'maleProportion')::NUMERIC,
            1 - (v_record.demographics->>'maleProportion')::NUMERIC,
            ((v_record.demographics->'ageDistribution'->0)::NUMERIC * 100),
            ((v_record.demographics->'ageDistribution'->1)::NUMERIC * 100),
            ((v_record.demographics->'ageDistribution'->2)::NUMERIC * 100),
            ((v_record.demographics->'ageDistribution'->3)::NUMERIC * 100),
        -- Calculate average dwell time from the array
            CASE WHEN jsonb_array_length(v_record.dwelltimes) > 0 THEN
            (
                SELECT SUM(visitors * mins) / NULLIF(SUM(visitors), 0)
                FROM (
                    SELECT 
                        idx, 
                        elem::NUMERIC as visitors,
                        CASE 
                            WHEN idx = 0 THEN 15
                            WHEN idx = 1 THEN 30
                            WHEN idx = 2 THEN 60
                            WHEN idx = 3 THEN 120
                            WHEN idx = 4 THEN 180
                            WHEN idx = 5 THEN 240
                            WHEN idx = 6 THEN 300
                            WHEN idx = 7 THEN 360
                            ELSE 480
                        END as mins
                        FROM jsonb_array_elements(v_record.dwelltimes) WITH ORDINALITY AS arr(elem, idx)
                ) as dwell_data
            )
            ELSE NULL END,
            v_record.aoi_id,
            v_etl_id,
        CURRENT_TIMESTAMP
        )
    ON CONFLICT (date_id, region_id) 
    DO UPDATE SET
        male_proportion = EXCLUDED.male_proportion,
        female_proportion = EXCLUDED.female_proportion,
        age_15_29_pct = EXCLUDED.age_15_29_pct,
        age_30_44_pct = EXCLUDED.age_30_44_pct,
        age_45_59_pct = EXCLUDED.age_45_59_pct,
        age_60_plus_pct = EXCLUDED.age_60_plus_pct,
        avg_dwell_time_mins = EXCLUDED.avg_dwell_time_mins,
            etl_id = EXCLUDED.etl_id,
            created_at = EXCLUDED.created_at;
    END LOOP;
    
    -- Calculate execution time
    v_exec_time_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
    
    -- Prepare success message
    v_status_message := FORMAT(
        'Load completed successfully. Date Range: %s to %s. Records Processed: %s. Fact Visitors Added: %s. Countries Added: %s. Cantons Added: %s. Execution Time: %s ms.',
        p_date_from, p_date_to, v_count, v_fact_visitors_added, v_countries_added, v_cantons_added, ROUND(v_exec_time_ms, 2)
    );
    
    -- Perform data quality checks
    PERFORM dw.check_aoi_data_quality(v_etl_id, p_date_from, p_date_to);
    
    -- Update the ETL metadata with completion status and execution time
    UPDATE dw.etl_metadata
    SET 
        records_processed = v_count,
        records_successful = v_fact_visitors_added,
        end_time = clock_timestamp(),
        status_code = 'COMPLETED',
        status_message = v_status_message || ' Quality check completed.'
    WHERE 
        etl_id = v_etl_id;
    
    RETURN v_count;
EXCEPTION WHEN OTHERS THEN
    -- Calculate execution time before error
    v_exec_time_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
    
    -- Prepare error message
    v_status_message := FORMAT(
        'Load failed. Error: %s. Date Range: %s to %s. Execution Time Before Error: %s ms.',
        SQLERRM, p_date_from, p_date_to, ROUND(v_exec_time_ms, 2)
    );
    
    -- Update ETL metadata with error status if v_etl_id was obtained
    IF v_etl_id IS NOT NULL THEN
        UPDATE dw.etl_metadata
        SET 
            end_time = clock_timestamp(),
            status_code = 'FAILED',
            status_message = v_status_message
        WHERE 
            etl_id = v_etl_id;
    ELSE
        -- If insert failed before getting etl_id, log to server log
        RAISE WARNING 'ETL metadata record (etl_id unknown) could not be updated due to early failure: %', v_status_message;
    END IF;
        
    RAISE NOTICE 'Error in load_aoi_days_data: %', SQLERRM;
    RETURN -1;
END;
$$ LANGUAGE plpgsql;

-- ----------------------
-- ANALYTICS VIEWS
-- ----------------------

-- Create views for analytics
CREATE OR REPLACE VIEW dw.vw_aoi_visitor_summary AS
SELECT 
    d.full_date,
    r.region_name,
    SUM(CASE WHEN vt.visitor_code = 'swiss_tourists' THEN f.visitor_count ELSE 0 END) AS swiss_tourists,
    SUM(CASE WHEN vt.visitor_code = 'foreign_tourists' THEN f.visitor_count ELSE 0 END) AS foreign_tourists,
    SUM(CASE WHEN vt.visitor_code = 'swiss_locals' THEN f.visitor_count ELSE 0 END) AS swiss_locals,
    SUM(CASE WHEN vt.visitor_code = 'swiss_commuters' THEN f.visitor_count ELSE 0 END) AS swiss_commuters,
    SUM(CASE WHEN vt.visitor_code = 'foreign_workers' THEN f.visitor_count ELSE 0 END) AS foreign_workers,
    SUM(f.visitor_count) AS total_visitors,
    vd.male_proportion * 100 AS male_percentage,
    vd.female_proportion * 100 AS female_percentage,
    vd.age_15_29_pct AS age_15_29_percentage,
    vd.age_30_44_pct AS age_30_44_percentage,
    vd.age_45_59_pct AS age_45_59_percentage,
    vd.age_60_plus_pct AS age_60_plus_percentage,
    vd.avg_dwell_time_mins
FROM 
    dw.fact_visitors f
JOIN 
    dw.dim_date d ON f.date_id = d.date_id
JOIN 
    dw.dim_region r ON f.region_id = r.region_id
JOIN 
    dw.dim_visitor_type vt ON f.visitor_type_id = vt.visitor_type_id
LEFT JOIN 
    dw.aoi_visitor_demographics vd ON f.date_id = vd.date_id AND f.region_id = vd.region_id
WHERE 
    f.source_system = 'aoi'
GROUP BY 
    d.full_date, r.region_name, vd.male_proportion, vd.female_proportion, 
    vd.age_15_29_pct, vd.age_30_44_pct, vd.age_45_59_pct, vd.age_60_plus_pct, vd.avg_dwell_time_mins
ORDER BY 
    d.full_date DESC, total_visitors DESC;

CREATE OR REPLACE VIEW dw.vw_aoi_top_countries AS
SELECT 
    d.full_date,
    r.region_name,
    c.country_name,
    vc.visitor_count,
    vc.rank_order,
    vc.visitor_count * 100.0 / NULLIF((
        SELECT SUM(visitor_count) 
        FROM dw.fact_visitors 
        WHERE date_id = vc.date_id 
        AND region_id = vc.region_id
        AND visitor_type_id = (SELECT visitor_type_id FROM dw.dim_visitor_type WHERE visitor_code = 'foreign_tourists')
    ), 0) AS percentage
FROM 
    dw.aoi_visitor_countries vc
JOIN 
    dw.dim_date d ON vc.date_id = d.date_id
JOIN 
    dw.dim_region r ON vc.region_id = r.region_id
JOIN 
    dw.dim_country c ON vc.country_id = c.country_id
ORDER BY 
    d.full_date DESC, r.region_name, vc.rank_order;

CREATE OR REPLACE VIEW dw.vw_aoi_top_cantons AS
SELECT 
    d.full_date,
    r.region_name,
    c.canton_name,
    c.canton_code,
    vc.visitor_count,
    vc.rank_order,
    vc.visitor_count * 100.0 / NULLIF((
        SELECT SUM(visitor_count) 
        FROM dw.fact_visitors 
        WHERE date_id = vc.date_id 
        AND region_id = vc.region_id
        AND visitor_type_id IN (
            SELECT visitor_type_id FROM dw.dim_visitor_type 
            WHERE visitor_code IN ('swiss_tourists', 'swiss_locals', 'swiss_commuters')
        )
    ), 0) AS percentage
FROM 
    dw.aoi_visitor_cantons vc
JOIN 
    dw.dim_date d ON vc.date_id = d.date_id
JOIN 
    dw.dim_region r ON vc.region_id = r.region_id
JOIN 
    dw.dim_canton c ON vc.canton_id = c.canton_id
ORDER BY 
    d.full_date DESC, r.region_name, vc.rank_order;

-- ----------------------
-- AUTOMATIC DATA LOADING
-- ----------------------

-- Execute the data loading process
DO $$
DECLARE
    v_start_date DATE;
    v_end_date DATE;
    v_batch_id INTEGER;
    v_records_loaded INTEGER;
    v_min_date DATE;
    v_max_date DATE;
    v_is_source_empty BOOLEAN;
    v_is_dw_empty BOOLEAN;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_status_message TEXT;
    v_force_full_reload BOOLEAN := TRUE; -- Set to TRUE to force full reload
BEGIN
    v_start_time := clock_timestamp();
    
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE 'STARTING AOI DATA LOADING PROCESS';
    RAISE NOTICE '---------------------------------------------------';
    
    -- Check if the source table is empty
    SELECT COUNT(*) = 0, MIN(aoi_date), MAX(aoi_date)
    INTO v_is_source_empty, v_min_date, v_max_date
    FROM data_lake.aoi_days_raw;

    IF v_is_source_empty THEN
        RAISE NOTICE 'Source table data_lake.aoi_days_raw is empty. No data to load.';
        RETURN;
    END IF;

    -- Check if the data warehouse fact table is empty for this source
    SELECT NOT EXISTS (SELECT 1 FROM dw.fact_visitors WHERE source_system = 'aoi' LIMIT 1)
    INTO v_is_dw_empty;
    
    -- Generate a new batch ID for this load
    SELECT COALESCE(MAX(batch_id), 0) + 1
    INTO v_batch_id
    FROM dw.etl_metadata
    WHERE source_system = 'aoi';
    
    IF v_is_dw_empty OR v_force_full_reload THEN
        -- First-time load: Load all available data from source
        v_start_date := v_min_date;
        v_end_date := v_max_date;
        
        IF v_force_full_reload AND NOT v_is_dw_empty THEN
            RAISE NOTICE 'Forcing full reload of AOI data from % to %', v_start_date, v_end_date;
        ELSE
            RAISE NOTICE 'DW is empty for AOI source. Performing first-time load from % to %', v_start_date, v_end_date;
        END IF;
    ELSE
        -- Incremental load: Load last 30 days (or adjust as needed)
        -- We load up to yesterday's data
        v_end_date := CURRENT_DATE - INTERVAL '1 day';
        v_start_date := v_end_date - INTERVAL '30 days';
        RAISE NOTICE 'DW contains AOI data. Performing incremental load from % to %', v_start_date, v_end_date;
        
        -- Optional: Adjust start_date if it's before the earliest source data
        IF v_start_date < v_min_date THEN
            v_start_date := v_min_date;
            RAISE NOTICE 'Adjusted incremental start date to earliest source date: %', v_start_date;
        END IF;
    END IF;
    
    -- Ensure end date is not after max source date
    IF v_end_date > v_max_date THEN
        v_end_date := v_max_date;
        RAISE NOTICE 'Adjusted end date to latest source date: %', v_end_date;
    END IF;

    -- Perform the data load if date range is valid
    IF v_start_date <= v_end_date THEN
        RAISE NOTICE 'Calling dw.load_aoi_days_data with batch ID: %', v_batch_id;
        SELECT dw.load_aoi_days_data(v_start_date, v_end_date, TRUE) INTO v_records_loaded;
    
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    
    -- Display success message
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE 'DATA LOADING COMPLETE';
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE 'Batch ID: %', v_batch_id;
        RAISE NOTICE 'Date Range Processed: % to %', v_start_date, v_end_date;
        RAISE NOTICE 'Records Processed (Raw Rows): %', v_records_loaded;
        RAISE NOTICE 'Total execution time: % seconds', EXTRACT(EPOCH FROM v_duration);
        RAISE NOTICE 'View summary: SELECT * FROM dw.vw_aoi_data_summary;';
    
        IF v_records_loaded > 0 THEN
    RAISE NOTICE '---------------------------------------------------';
            RAISE NOTICE 'DATA QUALITY SUMMARY:';
    RAISE NOTICE '---------------------------------------------------';
            
            -- Display quality summary
            WITH quality_summary AS (
                SELECT * FROM dw.vw_aoi_data_summary WHERE etl_id = v_batch_id
            )
        SELECT 
                'Total Records: ' || total_records || 
                ', Error Records: ' || error_records || 
                ', Discrepancy Records: ' || discrepancy_records || 
                ', Success Rate: ' || ROUND(success_rate, 2) || '%' || 
                ', Quality Score: ' || ROUND(quality_score, 2) || '%'
            FROM quality_summary
            INTO v_status_message;
            
            RAISE NOTICE '%', v_status_message;
            RAISE NOTICE 'View detailed summary: SELECT * FROM dw.vw_aoi_data_summary WHERE etl_id = %', v_batch_id;
        END IF;
    ELSE
        RAISE NOTICE 'Invalid date range (start date > end date). No data loaded.';
    END IF;

    RAISE NOTICE '---------------------------------------------------';
    
EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE 'ERROR DURING AOI DATA LOADING PROCESS';
    RAISE NOTICE 'Batch ID: %', COALESCE(v_batch_id::TEXT, 'N/A');
    RAISE NOTICE 'Error: %', SQLERRM;
    RAISE NOTICE 'Execution time before error: % seconds', EXTRACT(EPOCH FROM v_duration);
    RAISE NOTICE '---------------------------------------------------';
    -- Optionally re-raise the error if needed for transaction rollback
    -- RAISE;
END;
$$;

-- Create data quality tracking table
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'dw' 
        AND table_name = 'aoi_data_quality'
    ) THEN
        CREATE TABLE dw.aoi_data_quality (
            quality_id SERIAL PRIMARY KEY,
            etl_id INTEGER NOT NULL,
            date_id INTEGER NOT NULL,
            region_id INTEGER NOT NULL,
            total_visitor_count INTEGER,
            visitor_categories_sum INTEGER,
            demographics_complete BOOLEAN,
            dwelltimes_complete BOOLEAN,
            countries_count INTEGER,
            cantons_count INTEGER,
            has_discrepancy BOOLEAN,
            discrepancy_type TEXT,
            discrepancy_pct NUMERIC(5,2),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_aoi_data_quality_etl_id ON dw.aoi_data_quality(etl_id);
        CREATE INDEX idx_aoi_data_quality_date_region ON dw.aoi_data_quality(date_id, region_id);
        CREATE INDEX idx_aoi_data_quality_discrepancy ON dw.aoi_data_quality(has_discrepancy);
    END IF;
END;
$$;

-- Function to check data quality and consistency
CREATE OR REPLACE FUNCTION dw.check_aoi_data_quality(
    p_etl_id INTEGER,
    p_date_from DATE,
    p_date_to DATE
) RETURNS TABLE (
    total_records INTEGER,
    error_records INTEGER,
    discrepancy_records INTEGER,
    success_rate NUMERIC,
    quality_score NUMERIC,
    avg_discrepancy_pct NUMERIC
) AS $$
DECLARE
    v_record RECORD;
    v_total_records INTEGER := 0;
    v_error_records INTEGER := 0;
    v_discrepancy_records INTEGER := 0;
    v_date_id INTEGER;
    v_region_id INTEGER;
    v_total_visitors INTEGER;
    v_visitor_categories_sum INTEGER;
    v_has_discrepancy BOOLEAN;
    v_discrepancy_pct NUMERIC(5,2);
    v_discrepancy_type TEXT;
    v_demographics_complete BOOLEAN;
    v_dwelltimes_complete BOOLEAN;
    v_countries_count INTEGER;
    v_cantons_count INTEGER;
BEGIN
    -- Process each date+region combination in the fact_visitors table
    FOR v_record IN 
        SELECT 
            f.date_id,
            f.region_id,
            COALESCE(SUM(f.visitor_count), 0) AS total_visitors
        FROM dw.fact_visitors f
        WHERE f.source_system = 'aoi'
        AND f.date_id BETWEEN TO_CHAR(p_date_from, 'YYYYMMDD')::INTEGER AND TO_CHAR(p_date_to, 'YYYYMMDD')::INTEGER
        GROUP BY f.date_id, f.region_id
    LOOP
        v_date_id := v_record.date_id;
        v_region_id := v_record.region_id;
        v_total_visitors := v_record.total_visitors;
        v_total_records := v_total_records + 1;
        
        -- Calculate sum of visitor categories for comparison
        SELECT COALESCE(SUM(f.visitor_count), 0) INTO v_visitor_categories_sum
        FROM dw.fact_visitors f
        JOIN dw.dim_visitor_type vt ON f.visitor_type_id = vt.visitor_type_id
        WHERE f.date_id = v_date_id
        AND f.region_id = v_region_id
        AND f.source_system = 'aoi'
        AND vt.visitor_code IN ('swiss_tourists', 'foreign_tourists', 'swiss_locals', 'swiss_commuters', 'foreign_workers');
        
        -- Check if demographic data is complete
        SELECT COUNT(*) > 0 INTO v_demographics_complete
        FROM dw.aoi_visitor_demographics d
        WHERE d.date_id = v_date_id
        AND d.region_id = v_region_id
        AND d.male_proportion IS NOT NULL
        AND d.age_15_29_pct IS NOT NULL;
        
        -- Check if dwell times data is complete
        SELECT COUNT(*) > 0 INTO v_dwelltimes_complete
        FROM dw.aoi_visitor_demographics d
        WHERE d.date_id = v_date_id
        AND d.region_id = v_region_id
        AND d.avg_dwell_time_mins IS NOT NULL;
        
        -- Count foreign countries
        SELECT COUNT(*) INTO v_countries_count
        FROM dw.aoi_visitor_countries c
        WHERE c.date_id = v_date_id
        AND c.region_id = v_region_id;
        
        -- Count swiss cantons
        SELECT COUNT(*) INTO v_cantons_count
        FROM dw.aoi_visitor_cantons c
        WHERE c.date_id = v_date_id
        AND c.region_id = v_region_id;
        
        -- Check for discrepancies
        v_has_discrepancy := FALSE;
        v_discrepancy_type := NULL;
        v_discrepancy_pct := 0;
        
        -- Check if total visitors matches sum of categories
        IF v_total_visitors <> v_visitor_categories_sum AND v_total_visitors > 0 THEN
            v_has_discrepancy := TRUE;
            v_discrepancy_type := 'visitor_count_mismatch';
            v_discrepancy_pct := ABS(v_total_visitors - v_visitor_categories_sum) * 100.0 / NULLIF(v_total_visitors, 0);
            v_discrepancy_records := v_discrepancy_records + 1;
        END IF;
        
        -- Record quality metrics
        INSERT INTO dw.aoi_data_quality (
            etl_id,
                date_id,
                region_id,
            total_visitor_count,
            visitor_categories_sum,
            demographics_complete,
            dwelltimes_complete,
            countries_count,
            cantons_count,
            has_discrepancy,
            discrepancy_type,
            discrepancy_pct,
                created_at
            ) VALUES (
            p_etl_id,
                v_date_id,
                v_region_id,
            v_total_visitors,
            v_visitor_categories_sum,
            v_demographics_complete,
            v_dwelltimes_complete,
            v_countries_count,
            v_cantons_count,
            v_has_discrepancy,
            v_discrepancy_type,
            v_discrepancy_pct,
                CURRENT_TIMESTAMP
        );
        
        -- Count error records (either missing data or severe discrepancy)
        IF (NOT v_demographics_complete OR NOT v_dwelltimes_complete OR 
            (v_has_discrepancy AND v_discrepancy_pct > 20.0)) THEN
            v_error_records := v_error_records + 1;
        END IF;
    END LOOP;
    
    -- Return summary statistics
    RETURN QUERY
    SELECT 
        v_total_records AS total_records,
        v_error_records AS error_records,
        v_discrepancy_records AS discrepancy_records,
        CASE WHEN v_total_records > 0 
            THEN 100.0 - (v_error_records * 100.0 / v_total_records) 
            ELSE 0 
        END AS success_rate,
        CASE WHEN v_total_records > 0 
            THEN 100.0 - ((v_error_records * 0.7 + v_discrepancy_records * 0.3) * 100.0 / v_total_records) 
            ELSE 0 
        END AS quality_score,
        COALESCE((SELECT AVG(discrepancy_pct) FROM dw.aoi_data_quality WHERE etl_id = p_etl_id AND has_discrepancy), 0) AS avg_discrepancy_pct;
END;
$$ LANGUAGE plpgsql;

-- Create summary view
CREATE OR REPLACE VIEW dw.vw_aoi_data_summary AS
SELECT 
    e.etl_id,
    e.process_name,
    e.source_system,
    e.status_code,
    e.records_processed,
    e.records_successful,
    e.start_time,
    e.end_time,
    e.end_time - e.start_time AS duration,
    EXTRACT(EPOCH FROM (e.end_time - e.start_time)) AS duration_seconds,
    q.total_records,
    q.error_records,
    q.discrepancy_records,
    q.success_rate,
    q.quality_score,
    q.avg_discrepancy_pct,
    (SELECT COUNT(*) FROM dw.aoi_data_quality WHERE etl_id = e.etl_id AND has_discrepancy) AS discrepancy_count,
    (SELECT COUNT(*) FROM dw.aoi_data_quality WHERE etl_id = e.etl_id AND NOT demographics_complete) AS missing_demographics_count,
    (SELECT COUNT(*) FROM dw.aoi_data_quality WHERE etl_id = e.etl_id AND NOT dwelltimes_complete) AS missing_dwelltimes_count,
    (SELECT ROUND(AVG(countries_count),2) FROM dw.aoi_data_quality WHERE etl_id = e.etl_id) AS avg_countries_per_record,
    (SELECT ROUND(AVG(cantons_count),2) FROM dw.aoi_data_quality WHERE etl_id = e.etl_id) AS avg_cantons_per_record,
    e.status_message
FROM 
    dw.etl_metadata e
LEFT JOIN LATERAL (
    SELECT * FROM dw.check_aoi_data_quality(e.etl_id, 
        (SELECT TO_DATE(substring(min(date_id::TEXT), 1, 8), 'YYYYMMDD') 
         FROM dw.fact_visitors 
         WHERE source_system = 'aoi'), 
        (SELECT TO_DATE(substring(max(date_id::TEXT), 1, 8), 'YYYYMMDD') 
         FROM dw.fact_visitors 
         WHERE source_system = 'aoi')
    )
) q ON TRUE
WHERE 
    e.source_system = 'aoi'
ORDER BY 
    e.etl_id DESC;