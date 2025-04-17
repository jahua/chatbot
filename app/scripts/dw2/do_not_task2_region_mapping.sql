-- Drop existing functions to ensure clean creation
DROP FUNCTION IF EXISTS dw.get_region_id(character varying, character varying);
DROP FUNCTION IF EXISTS dw.get_region_id(character varying);
DROP FUNCTION IF EXISTS dw.get_region_id(character varying, character varying, character varying);

-- IMPLEMENTATION OF UNIFIED REGION MAPPING SYSTEM
-- This script demonstrates how to use the mapping system with sample data

-- First fix the ETL metadata table to ensure it has the correct columns
DO $$
BEGIN
    -- Check if we need to modify the etl_metadata table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'etl_metadata') THEN
        -- Check if it has the old structure
        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'dw' AND table_name = 'etl_metadata' AND column_name = 'task_name') THEN
            -- Old structure exists, need to convert or recreate
            DROP TABLE dw.etl_metadata;
        END IF;
    END IF;
    
    -- Create the new structure if needed
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'etl_metadata') THEN
        CREATE TABLE dw.etl_metadata (
            etl_id SERIAL PRIMARY KEY,
            process_name VARCHAR(100) NOT NULL,
            status_message TEXT,
            status_code VARCHAR(50) NOT NULL,
            records_processed INTEGER,
            records_rejected INTEGER,
            progress_percentage INTEGER,
            start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP WITH TIME ZONE,
            duration INTERVAL GENERATED ALWAYS AS (end_time - start_time) STORED,
            created_by VARCHAR(100) DEFAULT CURRENT_USER
        );
    END IF;
END $$;

-- Function to report ETL status
CREATE OR REPLACE FUNCTION dw.report_status(
    p_process_name VARCHAR,
    p_message TEXT,
    p_status VARCHAR,
    p_records_processed INTEGER DEFAULT NULL,
    p_records_rejected INTEGER DEFAULT NULL,
    p_progress_percentage INTEGER DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO dw.etl_metadata (
        process_name,
        status_message,
        status_code,
        records_processed,
        records_rejected,
        progress_percentage,
        start_time,
        end_time
    ) VALUES (
        p_process_name,
        p_message,
        p_status,
        p_records_processed,
        p_records_rejected,
        p_progress_percentage,
        CASE WHEN p_status = 'STARTED' THEN CURRENT_TIMESTAMP ELSE NULL END,
        CASE WHEN p_status = 'COMPLETE' OR p_status = 'COMPLETED' THEN CURRENT_TIMESTAMP ELSE NULL END
    );
    
    RAISE NOTICE '%: %', p_process_name, p_message;
END;
$$ LANGUAGE plpgsql;

-- Function to add region variants - added before it's called
CREATE OR REPLACE FUNCTION dw.add_region_variants(
    p_region_name VARCHAR,
    p_variants_json TEXT
)
RETURNS VOID AS $$
DECLARE
    v_region_id INTEGER;
    v_variants JSONB;
    v_variant JSONB;
    v_source VARCHAR;
    v_type VARCHAR;
BEGIN
    -- Get region ID
    SELECT region_id INTO v_region_id
    FROM dw.dim_region
    WHERE region_name = p_region_name;
    
    IF v_region_id IS NULL THEN
        RAISE EXCEPTION 'Region not found: %', p_region_name;
    END IF;
    
    -- Parse JSON
    v_variants := p_variants_json::JSONB;
    
    -- Add variants
    FOR v_variant IN SELECT value FROM jsonb_array_elements(v_variants)
    LOOP
        -- Map source names to allowed values
        v_source := CASE 
            WHEN v_variant->>'source' = 'aoi_days' THEN 'aoi'
            ELSE v_variant->>'source'
        END;
        
        -- Map type names to allowed values
        v_type := CASE 
            WHEN v_variant->>'type' IN ('english', 'german', 'canonical') THEN v_variant->>'type'
            ELSE 'canonical'
        END;
        
        INSERT INTO dw.dim_region_mapping (
            region_id,
            variant_name,
            variant_type,
            source_system,
            is_primary
        ) VALUES (
            v_region_id,
            v_variant->>'name',
            v_type,
            v_source,
            COALESCE((v_variant->>'is_primary')::BOOLEAN, FALSE)
        )
        ON CONFLICT (variant_name, source_system) DO NOTHING;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to populate country dimension
CREATE OR REPLACE PROCEDURE dw.populate_country_dimension()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Check if table exists, if not create it
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'dim_country') THEN
        CREATE TABLE dw.dim_country (
            country_id SERIAL PRIMARY KEY,
            country_code CHAR(2) NOT NULL UNIQUE,
            country_name VARCHAR(100) NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    END IF;

    -- Insert core countries with proper error handling
    INSERT INTO dw.dim_country (
        country_code,
        country_name,
        is_active
    ) VALUES 
        ('CH', 'Switzerland', TRUE),
        ('DE', 'Germany', TRUE),
        ('FR', 'France', TRUE),
        ('IT', 'Italy', TRUE),
        ('AT', 'Austria', TRUE),
        ('US', 'United States', TRUE),
        ('GB', 'United Kingdom', TRUE),
        ('CN', 'China', TRUE),
        ('JP', 'Japan', TRUE),
        ('IN', 'India', TRUE)
    ON CONFLICT (country_code) DO UPDATE SET
        country_name = EXCLUDED.country_name,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Country dimension populated with % records', v_count;
END;
$$;

-- Function to populate canton dimension
CREATE OR REPLACE PROCEDURE dw.populate_canton_dimension()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Check if table exists, if not create it
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'dim_canton') THEN
        CREATE TABLE dw.dim_canton (
            canton_id SERIAL PRIMARY KEY,
            canton_code CHAR(2) NOT NULL UNIQUE,
            canton_name VARCHAR(100) NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    END IF;

    -- Insert Swiss cantons with proper error handling
    INSERT INTO dw.dim_canton (
        canton_code,
        canton_name,
        is_active
    ) VALUES 
        ('ZH', 'Zürich', TRUE),
        ('BE', 'Bern', TRUE),
        ('LU', 'Luzern', TRUE),
        ('UR', 'Uri', TRUE),
        ('SZ', 'Schwyz', TRUE),
        ('OW', 'Obwalden', TRUE),
        ('NW', 'Nidwalden', TRUE),
        ('GL', 'Glarus', TRUE),
        ('ZG', 'Zug', TRUE),
        ('FR', 'Fribourg', TRUE),
        ('SO', 'Solothurn', TRUE),
        ('BS', 'Basel-Stadt', TRUE),
        ('BL', 'Basel-Landschaft', TRUE),
        ('SH', 'Schaffhausen', TRUE),
        ('AR', 'Appenzell Ausserrhoden', TRUE),
        ('AI', 'Appenzell Innerrhoden', TRUE),
        ('SG', 'St. Gallen', TRUE),
        ('GR', 'Graubünden', TRUE),
        ('AG', 'Aargau', TRUE),
        ('TG', 'Thurgau', TRUE),
        ('TI', 'Ticino', TRUE),
        ('VD', 'Vaud', TRUE),
        ('VS', 'Valais', TRUE),
        ('NE', 'Neuchâtel', TRUE),
        ('GE', 'Genève', TRUE),
        ('JU', 'Jura', TRUE)
    ON CONFLICT (canton_code) DO UPDATE SET
        canton_name = EXCLUDED.canton_name,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Canton dimension populated with % records', v_count;
END;
$$;

-- Function to link cantons to regions
CREATE OR REPLACE PROCEDURE dw.link_cantons_to_regions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Update region records with canton codes
    UPDATE dw.dim_region r
    SET canton_code = c.canton_code
    FROM dw.dim_canton c
    WHERE LOWER(r.region_name) = LOWER(c.canton_name)
    AND r.region_type = 'canton';
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Linked % canton codes to regions', v_count;
END;
$$;

-- Function to validate region data
CREATE OR REPLACE PROCEDURE dw.validate_region_data()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Check for regions without canton codes
    SELECT COUNT(*) INTO v_count
    FROM dw.dim_region
    WHERE region_type = 'canton'
    AND canton_code IS NULL;
    
    IF v_count > 0 THEN
        RAISE WARNING 'Found % cantons without canton codes', v_count;
    END IF;
    
    -- Check for districts without parent regions
    SELECT COUNT(*) INTO v_count
    FROM dw.dim_region
    WHERE region_type = 'district'
    AND parent_region_id IS NULL;
    
    IF v_count > 0 THEN
        RAISE WARNING 'Found % districts without parent regions', v_count;
    END IF;
    
    -- Check for cities without parent regions
    SELECT COUNT(*) INTO v_count
    FROM dw.dim_region
    WHERE region_type = 'city'
    AND parent_region_id IS NULL;
    
    IF v_count > 0 THEN
        RAISE WARNING 'Found % cities without parent regions', v_count;
    END IF;
    
    -- Check for gemeinde without parent regions
    SELECT COUNT(*) INTO v_count
    FROM dw.dim_region
    WHERE region_type = 'gemeinde'
    AND parent_region_id IS NULL;
    
    IF v_count > 0 THEN
        RAISE WARNING 'Found % gemeinde without parent regions', v_count;
    END IF;
    
    -- Check for regions without variants
    SELECT COUNT(*) INTO v_count
    FROM dw.dim_region r
    LEFT JOIN dw.dim_region_mapping m ON r.region_id = m.region_id
    WHERE m.region_id IS NULL;
    
    IF v_count > 0 THEN
        RAISE WARNING 'Found % regions without variants', v_count;
    END IF;
    
    RAISE NOTICE 'Region data validation completed';
END;
$$;

-- Function to suggest region matches
CREATE OR REPLACE FUNCTION dw.suggest_region_matches(
    p_search_term VARCHAR,
    p_similarity_threshold NUMERIC DEFAULT 0.6
)
RETURNS TABLE (
    region_name VARCHAR,
    similarity NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        r.region_name,
        similarity(r.region_name, p_search_term) AS similarity
    FROM dw.dim_region r
    WHERE similarity(r.region_name, p_search_term) >= p_similarity_threshold
    ORDER BY similarity DESC
    LIMIT 5;
END;
$$ LANGUAGE plpgsql;

-- Function to export region mappings
CREATE OR REPLACE FUNCTION dw.export_region_mappings()
RETURNS JSONB AS $$
BEGIN
    RETURN (
        SELECT jsonb_agg(
            jsonb_build_object(
                'region_id', r.region_id,
                'region_name', r.region_name,
                'region_type', r.region_type,
                'variants', (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'name', m.variant_name,
                            'type', m.variant_type,
                            'source', m.source_system
                        )
                    )
                    FROM dw.dim_region_mapping m
                    WHERE m.region_id = r.region_id
                )
            )
        )
        FROM dw.dim_region r
    );
END;
$$ LANGUAGE plpgsql;

-- Function to import region mappings - fixed loop variable issue
CREATE OR REPLACE PROCEDURE dw.import_region_mappings(p_mappings JSONB)
LANGUAGE plpgsql
AS $$
DECLARE
    v_mapping JSONB;
    v_variant JSONB;
BEGIN
    FOR v_mapping IN SELECT value FROM jsonb_array_elements(p_mappings)
    LOOP
        -- Insert or update region
        INSERT INTO dw.dim_region (
            region_id,
            region_name,
            region_type
        ) VALUES (
            (v_mapping->>'region_id')::INTEGER,
            v_mapping->>'region_name',
            v_mapping->>'region_type'
        )
        ON CONFLICT (region_id) DO UPDATE SET
            region_name = EXCLUDED.region_name,
            region_type = EXCLUDED.region_type;
        
        -- Insert or update variants
        FOR v_variant IN SELECT value FROM jsonb_array_elements(v_mapping->'variants')
        LOOP
            INSERT INTO dw.dim_region_mapping (
                region_id,
                variant_name,
                variant_type,
                source_system
            ) VALUES (
                (v_mapping->>'region_id')::INTEGER,
                v_variant->>'name',
                v_variant->>'type',
                v_variant->>'source'
            )
            ON CONFLICT (variant_name, source_system) DO UPDATE SET
                variant_type = EXCLUDED.variant_type;
        END LOOP;
    END LOOP;
    
    RAISE NOTICE 'Imported % region mappings', jsonb_array_length(p_mappings);
END;
$$;

-- Add get_region_id function needed for later scripts
CREATE OR REPLACE FUNCTION dw.get_region_id(
    p_region_name VARCHAR,
    p_source_system VARCHAR DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_region_id INTEGER;
BEGIN
    -- Try to find region by name directly
    IF p_source_system IS NULL THEN
        SELECT r.region_id INTO v_region_id
        FROM dw.dim_region r
        WHERE r.region_name = p_region_name
        AND r.is_active = TRUE;
    ELSE
        -- Try to find region by variant name for the given source system
        SELECT r.region_id INTO v_region_id
        FROM dw.dim_region r
        JOIN dw.dim_region_mapping m ON r.region_id = m.region_id
        WHERE m.variant_name = p_region_name
        AND m.source_system = p_source_system
        AND r.is_active = TRUE;
        
        -- If not found, try canonical name
        IF v_region_id IS NULL THEN
            SELECT r.region_id INTO v_region_id
            FROM dw.dim_region r
            WHERE r.region_name = p_region_name
            AND r.is_active = TRUE;
        END IF;
    END IF;
    
    RETURN v_region_id;
END;
$$ LANGUAGE plpgsql;

-- Make sure is_primary column exists in dim_region_mapping
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'dw' AND table_name = 'dim_region_mapping' AND column_name = 'is_primary'
    ) THEN
        ALTER TABLE dw.dim_region_mapping ADD COLUMN is_primary BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

-- Create a helper procedure to run the import steps in correct sequence
CREATE OR REPLACE PROCEDURE dw.run_region_import()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER;
BEGIN
    -- Start the import process
    PERFORM dw.report_status('region_import', 'Starting region data import', 'STARTED');

    -- Insert country level entries
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        country_code,
        is_active
    ) VALUES 
        ('Switzerland', 'country', 'CH', TRUE),
        ('Germany', 'country', 'DE', TRUE),
        ('Italy', 'country', 'IT', TRUE)
    ON CONFLICT (region_name, region_type) DO UPDATE SET
        country_code = EXCLUDED.country_code,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;
        
    -- Insert cantons with proper error handling
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        parent_region_id,
        is_active
    ) VALUES 
        ('Zürich', 'canton', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Switzerland' AND region_type = 'country'),
            TRUE),
        ('Bern', 'canton', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Switzerland' AND region_type = 'country'),
            TRUE),
        ('Luzern', 'canton', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Switzerland' AND region_type = 'country'),
            TRUE),
        ('Uri', 'canton', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Switzerland' AND region_type = 'country'),
            TRUE),
        ('Schwyz', 'canton', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Switzerland' AND region_type = 'country'),
            TRUE),
        ('Obwalden', 'canton', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Switzerland' AND region_type = 'country'),
            TRUE),
        ('Nidwalden', 'canton', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Switzerland' AND region_type = 'country'),
            TRUE)
    ON CONFLICT (region_name, region_type) DO UPDATE SET
        parent_region_id = EXCLUDED.parent_region_id,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;

    -- Insert districts with proper parent-child relationships
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        parent_region_id,
        is_active
    ) VALUES 
        ('Zürich Stadt', 'district', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Zürich' AND region_type = 'canton'),
            TRUE),
        ('Zürich Land', 'district',
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Zürich' AND region_type = 'canton'),
            TRUE),
        ('Bern Stadt', 'district',
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Bern' AND region_type = 'canton'),
            TRUE)
    ON CONFLICT (region_name, region_type) DO UPDATE SET
        parent_region_id = EXCLUDED.parent_region_id,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;
        
    -- Insert cities with proper parent-child relationships
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        parent_region_id,
        is_active
    ) VALUES 
        ('Zürich', 'city', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Zürich Stadt' AND region_type = 'district'),
            TRUE),
        ('Winterthur', 'city',
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Zürich Land' AND region_type = 'district'),
            TRUE),
        ('Bern', 'city',
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Bern Stadt' AND region_type = 'district'),
            TRUE)
    ON CONFLICT (region_name, region_type) DO UPDATE SET
        parent_region_id = EXCLUDED.parent_region_id,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;
        
    -- Insert gemeinde with proper parent-child relationships
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        parent_region_id,
        is_active
    ) VALUES 
        ('Adliswil', 'gemeinde', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Zürich Land' AND region_type = 'district'),
            TRUE),
        ('Kilchberg', 'gemeinde',
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Zürich Land' AND region_type = 'district'),
            TRUE),
        ('Köniz', 'gemeinde',
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Bern Stadt' AND region_type = 'district'),
            TRUE)
    ON CONFLICT (region_name, region_type) DO UPDATE SET
        parent_region_id = EXCLUDED.parent_region_id,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;

    -- Insert tourism regions with proper parent-child relationships
    INSERT INTO dw.dim_region (
        region_name,
        region_type,
        parent_region_id,
        is_active
    ) VALUES 
        ('Zürichsee', 'tourism_region', 
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Zürich' AND region_type = 'canton'),
            TRUE),
        ('Zürcher Oberland', 'tourism_region',
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Zürich' AND region_type = 'canton'),
            TRUE),
        ('Berner Oberland', 'tourism_region',
            (SELECT region_id FROM dw.dim_region WHERE region_name = 'Bern' AND region_type = 'canton'),
            TRUE)
    ON CONFLICT (region_name, region_type) DO UPDATE SET
        parent_region_id = EXCLUDED.parent_region_id,
        is_active = EXCLUDED.is_active,
        updated_at = CURRENT_TIMESTAMP;

    -- Report progress
    PERFORM dw.report_status('region_import', 'Core regions imported', 'IN_PROGRESS', NULL, NULL, 50);

    -- Add region variants for multilingual and source-specific names
    -- For Switzerland (country)
    PERFORM dw.add_region_variants('Switzerland', '[
        {"name": "Schweiz", "type": "german", "source": "mastercard", "is_primary": true},
        {"name": "Suisse", "type": "english", "source": "mastercard", "is_primary": true},
        {"name": "Svizzera", "type": "english", "source": "intervista", "is_primary": true}
    ]');

    -- For Zürich
    PERFORM dw.add_region_variants('Zürich', '[
        {"name": "Zurich", "type": "english", "source": "mastercard", "is_primary": true},
        {"name": "Zuerich", "type": "english", "source": "aoi", "is_primary": true},
        {"name": "Zurigo", "type": "english", "source": "intervista", "is_primary": true}
    ]');

    -- For Bern
    PERFORM dw.add_region_variants('Bern', '[
        {"name": "Berne", "type": "english", "source": "mastercard", "is_primary": true},
        {"name": "Bern", "type": "german", "source": "aoi", "is_primary": true},
        {"name": "Berna", "type": "english", "source": "intervista", "is_primary": true}
    ]');

    -- For Luzern
    PERFORM dw.add_region_variants('Luzern', '[
        {"name": "Lucerne", "type": "english", "source": "mastercard", "is_primary": true},
        {"name": "Luzern", "type": "german", "source": "aoi", "is_primary": true},
        {"name": "Lucerna", "type": "english", "source": "intervista", "is_primary": true}
    ]');
    
    -- For Zürich city
    PERFORM dw.add_region_variants('Zürich', '[
        {"name": "Zurich City", "type": "english", "source": "mastercard", "is_primary": true},
        {"name": "Zurich-City", "type": "english", "source": "aoi", "is_primary": true}
    ]');

    -- Report completion
    PERFORM dw.report_status('region_import', 'All region variants imported successfully', 'COMPLETE', NULL, NULL, 100);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in region import: %', SQLERRM;
    PERFORM dw.report_status('region_import', 'Error: ' || SQLERRM, 'FAILED', NULL, NULL, NULL);
    RAISE;
END;
$$;

BEGIN;

-- Populate reference dimensions
CALL dw.populate_country_dimension();
CALL dw.populate_canton_dimension();

-- Run the import process
CALL dw.run_region_import();

-- After adding all regions, link cantons to regions
CALL dw.link_cantons_to_regions();

-- Validate the data
CALL dw.validate_region_data();

-- Test the mapping system with some examples
DO $$
DECLARE
    v_region_id INTEGER;
    v_region_name VARCHAR;
    v_region_type VARCHAR;
BEGIN
    -- Test 1: Lookup by direct name
    SELECT dw.get_region_id('Zürich') INTO v_region_id;
    
    IF v_region_id IS NULL THEN
        RAISE NOTICE 'Test 1: "Zürich" not found';
    ELSE
        -- Get the canonical name and type
        SELECT region_name, region_type INTO v_region_name, v_region_type FROM dw.dim_region WHERE region_id = v_region_id;
        RAISE NOTICE 'Test 1: "Zürich" maps to region_id % (canonical name: %, type: %)', v_region_id, v_region_name, v_region_type;
    END IF;
    
    -- Test 2: Lookup with source system
    SELECT dw.get_region_id('Zurich', 'mastercard') INTO v_region_id;
    
    IF v_region_id IS NULL THEN
        RAISE NOTICE 'Test 2: "Zurich" (mastercard) not found';
    ELSE
        -- Get the canonical name and type
        SELECT region_name, region_type INTO v_region_name, v_region_type FROM dw.dim_region WHERE region_id = v_region_id;
        RAISE NOTICE 'Test 2: "Zurich" (mastercard) maps to region_id % (canonical name: %, type: %)', v_region_id, v_region_name, v_region_type;
    END IF;
    
    -- Test 3: Lookup by country name
    SELECT dw.get_region_id('Switzerland') INTO v_region_id;
    
    IF v_region_id IS NULL THEN
        RAISE NOTICE 'Test 3: "Switzerland" not found';
    ELSE
        -- Get the canonical name and type
        SELECT region_name, region_type INTO v_region_name, v_region_type FROM dw.dim_region WHERE region_id = v_region_id;
        RAISE NOTICE 'Test 3: "Switzerland" maps to region_id % (canonical name: %, type: %)', v_region_id, v_region_name, v_region_type;
    END IF;
    
    -- Test 4: Lookup by gemeinde name
    SELECT dw.get_region_id('Adliswil') INTO v_region_id;
    
    IF v_region_id IS NULL THEN
        RAISE NOTICE 'Test 4: "Adliswil" not found';
    ELSE
        -- Get the canonical name and type
        SELECT region_name, region_type INTO v_region_name, v_region_type FROM dw.dim_region WHERE region_id = v_region_id;
        RAISE NOTICE 'Test 4: "Adliswil" maps to region_id % (canonical name: %, type: %)', v_region_id, v_region_name, v_region_type;
    END IF;
END;
$$;

COMMIT;