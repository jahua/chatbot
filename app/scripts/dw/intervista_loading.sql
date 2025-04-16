-- Improved Intervista ETL script with constraint-compliant region_type

-- Step 0: Ensure pg_trgm extension is available
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Helper function to safely insert a region and return its ID
CREATE OR REPLACE FUNCTION dw.safe_insert_region(
    p_region_name TEXT,
    p_region_type TEXT
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

-- Create a more comprehensive region mapping table for Intervista data
DO $$
DECLARE
    v_mappings_count INTEGER := 0;
    v_region RECORD;
    v_affected_rows INTEGER;
    v_allowed_region_types TEXT[];
    v_region_id INTEGER;
BEGIN
    -- Define allowed region types based on the check constraint
    v_allowed_region_types := ARRAY['canton', 'tourism_region', 'district'];
    
    -- Create a temporary table with more comprehensive Italian-canonical mappings
    CREATE TEMP TABLE temp_italian_mappings (
        italian_name TEXT,
        canonical_name TEXT,
        region_type TEXT DEFAULT 'canton'
    );
    
    -- Insert known Italian-canonical mappings for cantons
    INSERT INTO temp_italian_mappings (italian_name, canonical_name) VALUES
        ('Zurigo', 'Zürich'),
        ('Berna', 'Bern'),
        ('Lucerna', 'Luzern'),
        ('Ginevra', 'Genève'),
        ('Basilea', 'Basel'),
        ('Losanna', 'Lausanne'),
        ('San Gallo', 'St. Gallen'),
        ('Lugano', 'Lugano'),
        ('Winterthur', 'Winterthur'),
        ('Svitto', 'Schwyz'),
        ('Friburgo', 'Fribourg'),
        ('Sciaffusa', 'Schaffhausen'),
        ('Coira', 'Chur'),
        ('Neuchâtel', 'Neuchâtel'),
        ('Bellinzona', 'Bellinzona'),
        ('Ticino', 'Ticino'),
        ('Vallese', 'Valais'),
        ('Grigioni', 'Graubünden'),
        ('Argovia', 'Aargau'),
        ('Turgovia', 'Thurgau'),
        ('Glarona', 'Glarus'),
        ('Soletta', 'Solothurn'),
        ('Obvaldo', 'Obwalden'),
        ('Nidvaldo', 'Nidwalden'),
        ('Appenzello Esterno', 'Appenzell Ausserrhoden'),
        ('Appenzello Interno', 'Appenzell Innerrhoden'),
        ('Zugo', 'Zug'),
        ('Uri', 'Uri'),
        ('Giura', 'Jura'),
        ('Vaud', 'Vaud');
    
    -- Add tourist regions and municipalities with Italian names
    INSERT INTO temp_italian_mappings (italian_name, canonical_name, region_type) VALUES
        -- Add direct mappings for regions found in the log
        ('Ascona-Locarno', 'Ascona-Locarno', 'tourism_region'),
        ('Solothurn', 'Solothurn', 'canton'),
        ('Zug', 'Zug', 'canton'),
        ('Bellinzonese', 'Bellinzona', 'district'),
        ('Nidwalden', 'Nidwalden', 'canton'),
        ('Basel-Landschaft', 'Basel-Landschaft', 'canton'),
        ('Switzerland', 'Switzerland', 'tourism_region'), -- Consider Switzerland as a tourism_region
        ('Bern', 'Bern', 'canton'),
        ('Thurgau', 'Thurgau', 'canton'),
        ('Appenzell Innerrhoden', 'Appenzell Innerrhoden', 'canton'),
        ('St. Gallen', 'St. Gallen', 'canton'),
        ('Basel-Stadt', 'Basel-Stadt', 'canton'),
        ('Aargau', 'Aargau', 'canton'),
        ('Geneva', 'Genève', 'canton'),
        ('Graubünden', 'Graubünden', 'canton'),
        ('Fribourg', 'Fribourg', 'canton'),
        ('Luganese', 'Lugano', 'district'),
        ('Schaffhausen', 'Schaffhausen', 'canton'),
        ('Jura', 'Jura', 'canton'),
        ('Bellinzona e Alto Ticino', 'Bellinzona', 'district'),
        ('Valais', 'Valais', 'canton'),
        ('Lago Maggiore e Valli', 'Lago Maggiore', 'district'),
        ('Lucerne', 'Luzern', 'canton'),
        ('Glarus', 'Glarus', 'canton'),
        ('Appenzell Ausserrhoden', 'Appenzell Ausserrhoden', 'canton'),
        ('Zurich', 'Zürich', 'canton'),
        ('Obwalden', 'Obwalden', 'canton'),
        ('Schwyz', 'Schwyz', 'canton'),
        ('Mendrisiotto', 'Mendrisio', 'district'),

        -- Major tourist destinations
        ('San Maurizio', 'St. Moritz', 'tourism_region'),
        ('Davos', 'Davos', 'tourism_region'),
        ('Zermatt', 'Zermatt', 'tourism_region'),
        ('Locarno', 'Locarno', 'tourism_region'),
        ('Ascona', 'Ascona', 'tourism_region'),
        ('Interlaken', 'Interlaken', 'tourism_region'),
        ('Montreux', 'Montreux', 'tourism_region'),
        
        -- Districts and regions
        ('Oberland Bernese', 'Bernese Oberland', 'district'),
        ('Engadina', 'Engadin', 'district'),
        ('Prättigau-Davos', 'Prättigau-Davos', 'district'),
        ('Maloja', 'Maloja', 'district'),
        ('Bernina', 'Bernina', 'district'),
        ('Interlaken-Oberhasli', 'Interlaken-Oberhasli', 'district'),
        ('Visp', 'Visp', 'district'),
        ('Verbano', 'Lago Maggiore', 'district'),
        
        -- Common spelling variants and alternate names
        ('Lago di Lugano', 'Lugano', 'district'),
        ('Lago Maggiore', 'Lago Maggiore', 'district'),
        ('Lago di Como', 'Como', 'district'),
        ('Regione del Lemano', 'Lake Geneva', 'district'),
        ('Leventina', 'Leventina', 'district'),
        ('Riviera', 'Riviera', 'district');
        
    -- Get all unique regions from the source data and add them if they don't have mappings
    FOR v_region IN 
        SELECT DISTINCT region_name 
        FROM inervista.dim_region
        WHERE region_name NOT IN (
            SELECT italian_name FROM temp_italian_mappings
        )
    LOOP
        -- Add each unmapped source region to the table with itself as the canonical name
        -- Use 'tourism_region' as a safe default region_type that's allowed by the constraint
        INSERT INTO temp_italian_mappings (italian_name, canonical_name, region_type)
        VALUES (v_region.region_name, v_region.region_name, 'tourism_region');
        
        RAISE NOTICE 'Added direct mapping for unmapped region: %', v_region.region_name;
    END LOOP;
    
    -- First ensure we have the core regions in the dim_region table
    FOR v_region IN 
        SELECT DISTINCT canonical_name, region_type 
        FROM temp_italian_mappings 
        WHERE NOT EXISTS (
            SELECT 1 FROM dw.dim_region 
            WHERE LOWER(region_name) = LOWER(temp_italian_mappings.canonical_name)
        )
    LOOP
        -- Use the safe insert function to add the region
        SELECT dw.safe_insert_region(
            v_region.canonical_name,
            v_region.region_type
        ) INTO v_region_id;
        
        RAISE NOTICE 'Added missing region: % (type: %) with ID: %', 
            v_region.canonical_name, v_region.region_type, v_region_id;
    END LOOP;
    
    -- Add these mappings to the region_mapping table if they don't exist already
    INSERT INTO dw.dim_region_mapping (
        region_id,
        variant_name,
        variant_type,
        source_system,
        is_primary
    )
    SELECT 
        dr.region_id,
        tim.italian_name,
        'canonical',
        'intervista',
        CASE WHEN tim.italian_name = tim.canonical_name THEN TRUE ELSE FALSE END
    FROM temp_italian_mappings tim
    JOIN dw.dim_region dr ON LOWER(dr.region_name) = LOWER(tim.canonical_name)
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_region_mapping rm
        WHERE rm.region_id = dr.region_id
        AND LOWER(rm.variant_name) = LOWER(tim.italian_name)
        AND rm.source_system = 'intervista'
    )
    ON CONFLICT (variant_name, source_system) DO NOTHING;
    
    GET DIAGNOSTICS v_mappings_count = ROW_COUNT;
    RAISE NOTICE 'Added % explicit Italian region name mappings', v_mappings_count;
    
    -- Directly map any unmapped regions from previous runs
    FOR v_region IN 
        SELECT DISTINCT region_name
        FROM dw.unmapped_regions
        WHERE source_system = 'intervista' 
        AND NOT EXISTS (
            SELECT 1 FROM dw.dim_region_mapping rm
            WHERE LOWER(rm.variant_name) = LOWER(dw.unmapped_regions.region_name)
            AND rm.source_system = 'intervista'
        )
    LOOP
        -- Use the safe insert function to add the region with tourism_region type
        SELECT dw.safe_insert_region(
            v_region.region_name,
            'tourism_region'
        ) INTO v_region_id;
        
        -- Add a mapping for the region
        IF v_region_id IS NOT NULL THEN
            INSERT INTO dw.dim_region_mapping (
                region_id,
                variant_name,
                variant_type,
                source_system,
                is_primary
            ) VALUES (
                v_region_id,
                v_region.region_name,
                'canonical',
                'intervista',
                TRUE
            )
            ON CONFLICT (variant_name, source_system) DO NOTHING;
            
            RAISE NOTICE 'Mapped previously unmapped region: % with ID: %', 
                v_region.region_name, v_region_id;
        END IF;
    END LOOP;
    
    -- Clean up
    DROP TABLE temp_italian_mappings;
END $$;

-- Create a simpler function for region matching that doesn't rely on similarity
CREATE OR REPLACE FUNCTION dw.fuzzy_match_region(
    p_source_name TEXT, 
    p_threshold FLOAT DEFAULT 0.7
) 
RETURNS INTEGER AS $$
DECLARE
    v_region_id INTEGER;
    v_source_normalized TEXT;
BEGIN
    -- Normalize the source name
    v_source_normalized := UPPER(TRIM(REGEXP_REPLACE(p_source_name, '[^A-Za-zÀ-ÿ0-9]', '', 'g')));
    
    -- First try through region_mapping with exact match
    SELECT rm.region_id INTO v_region_id
    FROM dw.dim_region_mapping rm
    WHERE UPPER(TRIM(REGEXP_REPLACE(rm.variant_name, '[^A-Za-zÀ-ÿ0-9]', '', 'g'))) = v_source_normalized
    AND rm.source_system = 'intervista'
    LIMIT 1;
    
    -- If not found, try direct region match
    IF v_region_id IS NULL THEN
        SELECT dr.region_id INTO v_region_id
        FROM dw.dim_region dr
        WHERE UPPER(TRIM(REGEXP_REPLACE(dr.region_name, '[^A-Za-zÀ-ÿ0-9]', '', 'g'))) = v_source_normalized
        LIMIT 1;
    END IF;
    
    -- If still not found, try with LIKE pattern
    IF v_region_id IS NULL THEN
        -- Try mapping table with LIKE pattern
        SELECT rm.region_id INTO v_region_id
        FROM dw.dim_region_mapping rm
        WHERE 
            rm.source_system = 'intervista' AND
            (
                -- Try exact substring match
                UPPER(TRIM(REGEXP_REPLACE(rm.variant_name, '[^A-Za-zÀ-ÿ0-9]', '', 'g'))) LIKE '%' || v_source_normalized || '%'
                OR 
                v_source_normalized LIKE '%' || UPPER(TRIM(REGEXP_REPLACE(rm.variant_name, '[^A-Za-zÀ-ÿ0-9]', '', 'g'))) || '%'
            )
        LIMIT 1;
        
        -- If still not found, try direct regions with LIKE pattern
        IF v_region_id IS NULL THEN
            SELECT dr.region_id INTO v_region_id
            FROM dw.dim_region dr
            WHERE 
                (
                    -- Try exact substring match
                    UPPER(TRIM(REGEXP_REPLACE(dr.region_name, '[^A-Za-zÀ-ÿ0-9]', '', 'g'))) LIKE '%' || v_source_normalized || '%'
                    OR 
                    v_source_normalized LIKE '%' || UPPER(TRIM(REGEXP_REPLACE(dr.region_name, '[^A-Za-zÀ-ÿ0-9]', '', 'g'))) || '%'
                )
            LIMIT 1;
        END IF;
    END IF;
    
    RETURN v_region_id;
END;
$$ LANGUAGE plpgsql;

-- Step 3: Load the data from Intervista to DW for monthly data
DO $$
DECLARE
    v_year INTEGER := 2023;
    v_month_start INTEGER := 1;
    v_month_end INTEGER := 12;
    v_start_date_id INTEGER;
    v_end_date_id INTEGER;
    v_region_count INTEGER := 0;
    v_visitor_type_count INTEGER := 0;
    v_fact_count INTEGER := 0;
    v_region_name TEXT;
    v_region_id INTEGER;
    v_visit_type_name TEXT;
    v_visitor_type_id INTEGER;
    v_fuzzy_matched INTEGER := 0;
    v_count INTEGER := 0;
    v_data RECORD;
    v_i INTEGER;
    v_total_source_records INTEGER;
    v_matched_direct_name INTEGER := 0;
    v_matched_variant_name INTEGER := 0;
    v_matched_fuzzy INTEGER := 0;
    v_unmatched INTEGER := 0;
    v_region_record RECORD;
BEGIN
    -- Calculate date IDs for year range using first day of month
    v_start_date_id := (v_year * 10000 + v_month_start * 100 + 1)::INTEGER; -- YYYYMM01
    v_end_date_id := (v_year * 10000 + v_month_end * 100 + 1)::INTEGER; -- YYYYMM01
    
    RAISE NOTICE 'Starting Intervista data load for period % to % (YYYYMM01 format)', 
                v_start_date_id, v_end_date_id;
                
    -- Count total records in source to track progress
    SELECT COUNT(*) INTO v_total_source_records
    FROM inervista.fact_tourism
    WHERE date_id BETWEEN v_month_start AND v_month_end;
    
    RAISE NOTICE 'Total source records to process: %', v_total_source_records;
    
    -- Clean existing data for the period (using the first day of each month)
    DELETE FROM dw.fact_visitors fv
    WHERE fv.source_system = 'intervista'
    AND EXISTS (
        SELECT 1 FROM dw.dim_date dd 
        WHERE dd.date_id = fv.date_id 
        AND dd.year = v_year 
        AND dd.month BETWEEN v_month_start AND v_month_end
    );
    
    RAISE NOTICE 'Cleaned existing data for the period';
    
    -- 1. Map visitor types
    RAISE NOTICE 'Adding visitor type mappings...';
    
    INSERT INTO dw.dim_visitor_type (
        visitor_code,
        visitor_name,
        visitor_category,
        visitor_subcategory,
        is_domestic,
        is_overnight,
        is_business,
        intervista_category_name,
        description,
        is_active,
        valid_from,
        valid_to,
        created_at,
        updated_at
    )
    SELECT 
        'INT_' || ivt.visit_type_id,
        ivt.visit_type_name,
        'general',
        'standard',
        TRUE, -- Default is_domestic
        CASE WHEN ivt.visit_type_name = 'overnight' THEN TRUE ELSE FALSE END,
        FALSE, -- Default is_business
        ivt.visit_type_name,
        COALESCE(ivt.visit_type_description, 'Imported from Intervista'),
        TRUE,
        CURRENT_DATE,
        '9999-12-31'::DATE,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM inervista.dim_visit_type ivt
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_visitor_type dvt
        WHERE dvt.intervista_category_name = ivt.visit_type_name
    );
    
    GET DIAGNOSTICS v_visitor_type_count = ROW_COUNT;
    RAISE NOTICE 'Mapped % visitor types', v_visitor_type_count;

    -- 2. Load the fact data with proper date mapping
    RAISE NOTICE 'Loading fact data with monthly date mapping...';
    
    FOR v_month IN v_month_start..v_month_end LOOP
        v_start_date_id := (v_year * 10000 + v_month * 100 + 1)::INTEGER; -- Use YYYYMM01
        
        -- Insert data for this month using direct name matching first
        INSERT INTO dw.fact_visitors (
            date_id,
            region_id,
            visitor_type_id,
            visitor_count,
            source_system,
            created_at
        )
        SELECT 
            v_start_date_id, -- Use YYYYMM01 format for target data warehouse
            dr.region_id,
            dvt.visitor_type_id,
            ift.total,
            'intervista',
            CURRENT_TIMESTAMP
        FROM inervista.fact_tourism ift
        JOIN inervista.dim_region ir ON ir.region_id = ift.region_id
        JOIN dw.dim_region dr ON LOWER(dr.region_name) = LOWER(ir.region_name)
        JOIN inervista.dim_visit_type ivt ON ivt.visit_type_id = ift.visit_type_id
        JOIN dw.dim_visitor_type dvt ON dvt.intervista_category_name = ivt.visit_type_name
        WHERE ift.date_id = v_month -- Source uses month number (1-12)
        ON CONFLICT DO NOTHING;
        
        GET DIAGNOSTICS v_count = ROW_COUNT;
        v_matched_direct_name := v_matched_direct_name + v_count;
        v_fact_count := v_fact_count + v_count;
        
        -- Try region mapping for records not matched by direct name
        INSERT INTO dw.fact_visitors (
            date_id,
            region_id,
            visitor_type_id,
            visitor_count,
            source_system,
            created_at
        )
        SELECT 
            v_start_date_id,
            rm.region_id,
            dvt.visitor_type_id,
            ift.total,
            'intervista',
            CURRENT_TIMESTAMP
        FROM inervista.fact_tourism ift
        JOIN inervista.dim_region ir ON ir.region_id = ift.region_id
        JOIN dw.dim_region_mapping rm ON LOWER(rm.variant_name) = LOWER(ir.region_name) AND rm.source_system = 'intervista'
        JOIN inervista.dim_visit_type ivt ON ivt.visit_type_id = ift.visit_type_id
        JOIN dw.dim_visitor_type dvt ON dvt.intervista_category_name = ivt.visit_type_name
        WHERE ift.date_id = v_month
        AND NOT EXISTS (
            SELECT 1 FROM dw.fact_visitors fv
            WHERE fv.date_id = v_start_date_id
            AND fv.visitor_type_id = dvt.visitor_type_id
            AND fv.region_id = rm.region_id
            AND fv.source_system = 'intervista'
        )
        ON CONFLICT DO NOTHING;
        
        GET DIAGNOSTICS v_count = ROW_COUNT;
        v_matched_variant_name := v_matched_variant_name + v_count;
        v_fact_count := v_fact_count + v_count;
    END LOOP;
    
    -- 3. Use pattern matching for any remaining unmatched regions
    RAISE NOTICE 'Applying pattern matching for remaining unmatched regions...';
    
    -- Create a temporary table to track unmatched regions
    CREATE TEMP TABLE temp_unmatched_regions (
        region_name TEXT,
        visitor_type_id INTEGER,
        month_id INTEGER,
        total INTEGER,
        processed BOOLEAN DEFAULT FALSE
    );
    
    -- Populate it with unmatched records
    FOR v_month IN v_month_start..v_month_end LOOP
        v_start_date_id := (v_year * 10000 + v_month * 100 + 1)::INTEGER; -- YYYYMM01
        
        INSERT INTO temp_unmatched_regions (region_name, visitor_type_id, month_id, total)
        SELECT DISTINCT 
            ir.region_name,
            dvt.visitor_type_id,
            v_month,
            ift.total
        FROM inervista.fact_tourism ift
        JOIN inervista.dim_region ir ON ir.region_id = ift.region_id
        JOIN inervista.dim_visit_type ivt ON ivt.visit_type_id = ift.visit_type_id
        JOIN dw.dim_visitor_type dvt ON dvt.intervista_category_name = ivt.visit_type_name
        WHERE ift.date_id = v_month
        AND NOT EXISTS (
            -- Check if this record is already in fact_visitors
            SELECT 1 FROM dw.fact_visitors fv
            WHERE fv.date_id = v_start_date_id
            AND fv.visitor_type_id = dvt.visitor_type_id
            AND fv.source_system = 'intervista'
            AND EXISTS (
                -- Check if any region matches this source region
                SELECT 1 FROM dw.dim_region_mapping rm 
                WHERE rm.region_id = fv.region_id AND LOWER(rm.variant_name) = LOWER(ir.region_name)
            )
        )
        AND NOT EXISTS (
            -- Check if there's a direct match with region name
            SELECT 1 FROM dw.dim_region dr
            WHERE LOWER(dr.region_name) = LOWER(ir.region_name)
        );
    END LOOP;
    
    -- Process each unmatched region
    FOR v_region_record IN SELECT DISTINCT region_name FROM temp_unmatched_regions WHERE processed = FALSE LOOP
        -- Try fuzzy matching
        SELECT dw.fuzzy_match_region(v_region_record.region_name, 0.8) INTO v_region_id;
        
        IF v_region_id IS NOT NULL THEN
            -- Add mapping for future use
            INSERT INTO dw.dim_region_mapping (
                region_id,
                variant_name,
                variant_type,
                source_system,
                is_primary
            ) VALUES (
                v_region_id,
                v_region_record.region_name,
                'canonical',
                'intervista',
                FALSE
            )
            ON CONFLICT (variant_name, source_system) DO NOTHING;
            
            -- Insert records for this region for all months
            INSERT INTO dw.fact_visitors (
                date_id,
                region_id,
                visitor_type_id,
                visitor_count,
                source_system,
                created_at
            )
            SELECT 
                (v_year * 10000 + tur.month_id * 100 + 1)::INTEGER,
                v_region_id,
                tur.visitor_type_id,
                tur.total,
                'intervista',
                CURRENT_TIMESTAMP
            FROM temp_unmatched_regions tur
            WHERE tur.region_name = v_region_record.region_name
            AND tur.processed = FALSE
            ON CONFLICT DO NOTHING;
            
            GET DIAGNOSTICS v_count = ROW_COUNT;
            v_matched_fuzzy := v_matched_fuzzy + v_count;
            v_fact_count := v_fact_count + v_count;
            
            -- Mark as processed
            UPDATE temp_unmatched_regions SET processed = TRUE
            WHERE region_name = v_region_record.region_name;
            
            RAISE NOTICE 'Fuzzy matched region: % to ID: %, inserted % records', 
                v_region_record.region_name, v_region_id, v_count;
        ELSE
            -- If can't match, try to create the region directly
            v_region_id := dw.safe_insert_region(v_region_record.region_name, 'tourism_region');
            
            IF v_region_id IS NOT NULL THEN
                -- Add mapping for this region
                INSERT INTO dw.dim_region_mapping (
                    region_id,
                    variant_name,
                    variant_type,
                    source_system,
                    is_primary
                ) VALUES (
                    v_region_id,
                    v_region_record.region_name,
                    'canonical',
                    'intervista',
                    TRUE
                )
                ON CONFLICT (variant_name, source_system) DO NOTHING;
                
                -- Insert records for this region for all months
                INSERT INTO dw.fact_visitors (
                    date_id,
                    region_id,
                    visitor_type_id,
                    visitor_count,
                    source_system,
                    created_at
                )
                SELECT 
                    (v_year * 10000 + tur.month_id * 100 + 1)::INTEGER,
                    v_region_id,
                    tur.visitor_type_id,
                    tur.total,
                    'intervista',
                    CURRENT_TIMESTAMP
                FROM temp_unmatched_regions tur
                WHERE tur.region_name = v_region_record.region_name
                AND tur.processed = FALSE
                ON CONFLICT DO NOTHING;
                
                GET DIAGNOSTICS v_count = ROW_COUNT;
                v_matched_fuzzy := v_matched_fuzzy + v_count;
                v_fact_count := v_fact_count + v_count;
                
                -- Mark as processed
                UPDATE temp_unmatched_regions SET processed = TRUE
                WHERE region_name = v_region_record.region_name;
                
                RAISE NOTICE 'Created and mapped new region: % with ID: %, inserted % records', 
                    v_region_record.region_name, v_region_id, v_count;
            ELSE
                v_unmatched := v_unmatched + 1;
                
                -- Log the unmatched regions for future handling
                INSERT INTO dw.unmapped_regions (
                    source_system,
                    region_name,
                    normalized_name,
                    mapping_status,
                    mapping_notes
                ) VALUES (
                    'intervista',
                    v_region_record.region_name,
                    LOWER(REGEXP_REPLACE(v_region_record.region_name, '[^a-zA-Z0-9]', '', 'g')),
                    'PENDING',
                    'Failed to match in run on ' || CURRENT_TIMESTAMP
                )
                ON CONFLICT DO NOTHING;
                
                -- Mark as processed
                UPDATE temp_unmatched_regions SET processed = TRUE
                WHERE region_name = v_region_record.region_name;
                
                RAISE NOTICE 'Could not match region: %, marked as unmapped', v_region_record.region_name;
            END IF;
        END IF;
    END LOOP;
    
    -- Get remaining unmatched count
    SELECT COUNT(*) INTO v_unmatched FROM temp_unmatched_regions WHERE processed = FALSE;
    DROP TABLE temp_unmatched_regions;
    
    -- 5. Summary
    RAISE NOTICE 'Data load complete. Summary:';
    RAISE NOTICE '  - Period: %01 to %01 (YYYYMM01 format)', (v_year * 100 + v_month_start), (v_year * 100 + v_month_end);
    RAISE NOTICE '  - Total source records: %', v_total_source_records;
    RAISE NOTICE '  - Mapped visitor types: %', v_visitor_type_count;
    RAISE NOTICE '  - Records matched by direct region name: %', v_matched_direct_name;
    RAISE NOTICE '  - Records matched by region variant: %', v_matched_variant_name;
    RAISE NOTICE '  - Records matched by fuzzy matching: %', v_matched_fuzzy;
    RAISE NOTICE '  - Total loaded fact records: %', v_fact_count;
    RAISE NOTICE '  - Unmatched records: %', v_unmatched;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in ETL process: %', SQLERRM;
    RAISE;
END $$;

-- Verify data was loaded correctly - grouped by month and region
SELECT 
    EXTRACT(YEAR FROM dd.full_date)::INTEGER AS year, 
    EXTRACT(MONTH FROM dd.full_date)::INTEGER AS month,
    dr.region_name,
    COUNT(*) as record_count
FROM dw.fact_visitors fv
JOIN dw.dim_date dd ON fv.date_id = dd.date_id
JOIN dw.dim_region dr ON dr.region_id = fv.region_id
WHERE fv.source_system = 'intervista'
AND dd.year = 2023 -- Filter by year in dim_date
GROUP BY EXTRACT(YEAR FROM dd.full_date), EXTRACT(MONTH FROM dd.full_date), dr.region_name -- Group by expressions
ORDER BY EXTRACT(YEAR FROM dd.full_date), EXTRACT(MONTH FROM dd.full_date), dr.region_name; -- Order by expressions

-- Show the summary of mapped regions
SELECT
    dr.region_type,
    COUNT(DISTINCT dr.region_id) as region_count,
    STRING_AGG(DISTINCT dr.region_name, ', ' ORDER BY dr.region_name) as region_names
FROM dw.dim_region dr
JOIN dw.dim_region_mapping rm ON rm.region_id = dr.region_id
WHERE rm.source_system = 'intervista'
GROUP BY dr.region_type
ORDER BY region_count DESC;