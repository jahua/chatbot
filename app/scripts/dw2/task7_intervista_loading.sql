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

-- Step 1: Create region mappings for the four Ticino tourism regions and Switzerland
DO $$
DECLARE
    v_ticino_id INTEGER;
BEGIN
    -- First ensure Canton Ticino exists
    INSERT INTO dw.dim_region (region_name, region_type)
    SELECT 'Ticino', 'canton'
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_region WHERE region_name = 'Ticino' AND region_type = 'canton'
    )
    RETURNING region_id INTO v_ticino_id;

    -- If we didn't insert it, get its existing ID
    IF v_ticino_id IS NULL THEN
        SELECT region_id INTO v_ticino_id
        FROM dw.dim_region
        WHERE region_name = 'Ticino' AND region_type = 'canton';
    END IF;

    -- Ensure Switzerland exists as a country
    INSERT INTO dw.dim_region (region_name, region_type)
    SELECT 'Switzerland', 'country'
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_region WHERE region_name = 'Switzerland' AND region_type = 'country'
    );

    -- Ensure the four tourism regions exist
    INSERT INTO dw.dim_region (region_name, region_type, parent_region_id)
    VALUES 
        ('Bellinzonese', 'tourism_region', v_ticino_id),
        ('Ascona-Locarno', 'tourism_region', v_ticino_id),
        ('Luganese', 'tourism_region', v_ticino_id),
        ('Mendrisiotto', 'tourism_region', v_ticino_id)
    ON CONFLICT (region_name, region_type) 
    DO UPDATE SET parent_region_id = EXCLUDED.parent_region_id;
END $$;

-- Step 2: Set up region mappings
    CREATE TEMP TABLE temp_italian_mappings (
        italian_name TEXT,
        canonical_name TEXT,
    region_type TEXT
);

-- Insert mappings for Ticino tourism regions and Switzerland
    INSERT INTO temp_italian_mappings (italian_name, canonical_name, region_type) VALUES
    ('Bellinzona e Alto Ticino', 'Bellinzonese', 'tourism_region'),
    ('Lago Maggiore e Valli', 'Ascona-Locarno', 'tourism_region'),
    ('Lago di Lugano', 'Luganese', 'tourism_region'),
    ('Mendrisiotto', 'Mendrisiotto', 'tourism_region'),
    ('Switzerland', 'Switzerland', 'country');

-- Create mappings in dim_region_mapping
INSERT INTO dw.dim_region_mapping (region_id, variant_name, variant_type, source_system)
    SELECT 
        dr.region_id,
        tim.italian_name,
        'canonical',
    'intervista'
    FROM temp_italian_mappings tim
JOIN dw.dim_region dr ON dr.region_name = tim.canonical_name AND dr.region_type = tim.region_type
    ON CONFLICT (variant_name, source_system) DO NOTHING;
    
-- Step 3: Set up visitor types
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
        valid_from,
        valid_to,
        created_at,
        updated_at
    )
    SELECT 
    'INT_' || visit_type_id,
    visit_type_name,
        'general',
        'standard',
    CASE 
        WHEN visit_type_name = 'total' THEN FALSE
        WHEN visit_type_name = 'overnight' THEN TRUE
        WHEN visit_type_name = 'daytrip' THEN FALSE
    END,
    CASE 
        WHEN visit_type_name = 'total' THEN FALSE
        WHEN visit_type_name = 'overnight' THEN TRUE
        WHEN visit_type_name = 'daytrip' THEN FALSE
    END,
    FALSE,
    visit_type_name,
    visit_type_description,
    '2023-01-01'::DATE,
        '9999-12-31'::DATE,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM inervista.dim_visit_type ivt
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_visitor_type dvt
        WHERE dvt.intervista_category_name = ivt.visit_type_name
    );
    
-- Step 4: Load Intervista data into fact_visitor
TRUNCATE TABLE dw.fact_visitor;

INSERT INTO dw.fact_visitor (
            date_id,
            region_id,
    total_visitors,
    swiss_tourists,
    foreign_tourists,
    swiss_locals,
    foreign_workers,
    swiss_commuters,
    demographics,
    top_swiss_cantons,
    transaction_metrics,
    source_system
        )
        SELECT 
    -- Date handling: Use first day of month for date_id
    (2023 * 10000 + ft.date_id * 100 + 1) as date_id,
            dr.region_id,
    
    -- Visitor metrics using dim_visitor_type with data_category
    SUM(ft.total) as total_visitors,
    SUM(CASE 
        WHEN dvt.is_domestic = TRUE 
         AND dvt.intervista_category_name = 'tourist' 
        THEN ft.total 
        ELSE 0 
    END) as swiss_tourists,
    
    SUM(CASE 
        WHEN dvt.is_domestic = FALSE 
         AND dvt.intervista_category_name = 'tourist'
        THEN ft.total 
        ELSE 0 
    END) as foreign_tourists,
    
    SUM(CASE 
        WHEN dvt.is_domestic = TRUE 
         AND dvt.intervista_category_name = 'local'
        THEN ft.total 
        ELSE 0 
    END) as swiss_locals,
    
    SUM(CASE 
        WHEN dvt.is_domestic = FALSE 
         AND dvt.intervista_category_name = 'worker'
        THEN ft.total 
        ELSE 0 
    END) as foreign_workers,
    
    SUM(CASE 
        WHEN dvt.is_domestic = TRUE 
         AND dvt.intervista_category_name = 'commuter'
        THEN ft.total 
        ELSE 0 
    END) as swiss_commuters,
    
    -- Demographics JSONB
    jsonb_build_object(
        'age', jsonb_build_object(
            '15_29', SUM(ft.age_15_29),
            '30_44', SUM(ft.age_30_44),
            '45_59', SUM(ft.age_45_59),
            '60_plus', SUM(ft.age_60_plus)
        ),
        'gender', jsonb_build_object(
            'male', SUM(ft.sex_male),
            'female', SUM(ft.sex_female)
        ),
        'education', jsonb_build_object(
            'high', SUM(ft.educ_high),
            'medium', SUM(ft.educ_medium),
            'low', SUM(ft.educ_low)
        ),
        'income', jsonb_build_object(
            '0_6000', SUM(ft.income_0_6000),
            '6001_12000', SUM(ft.income_6001_12000),
            '12001_plus', SUM(ft.income_12001_plus)
        ),
        'household_size', jsonb_build_object(
            'hh_1_2', SUM(ft.size_hh_1_2),
            'hh_3_plus', SUM(ft.size_hh_3_plus)
        )
    ) as demographics,
    
    -- Top Swiss Cantons JSONB - using empty object for now as it's not in source data
    jsonb_build_object() as top_swiss_cantons,
    
    -- Transaction Metrics JSONB
    jsonb_build_object(
        'transport', jsonb_build_object(
            'public', SUM(ft.transport_public),
            'vehicle', SUM(ft.transport_invehicle),
            'bicycle', SUM(ft.transport_onbicycle),
            'foot', SUM(ft.transport_onfoot),
            'abroad', SUM(ft.transport_abroad),
            'other', SUM(ft.transport_other)
        ),
        'distance', jsonb_build_object(
            '0_10', SUM(ft.distance_0_10),
            '10_25', SUM(ft.distance_10_25),
            '25_50', SUM(ft.distance_25_50),
            '50_100', SUM(ft.distance_50_100),
            '100_150', SUM(ft.distance_100_150),
            '150_plus', SUM(ft.distance_150_plus)
        )
    ) as transaction_metrics,
    'intervista' as source_system

FROM inervista.fact_tourism ft
JOIN inervista.dim_region ir ON ir.region_id = ft.region_id
JOIN dw.dim_region_mapping rm ON rm.variant_name = ir.region_name AND rm.source_system = 'intervista'
JOIN dw.dim_region dr ON dr.region_id = rm.region_id
JOIN inervista.dim_visit_type ivt ON ft.visit_type_id = ivt.visit_type_id
JOIN dw.dim_visitor_type dvt ON dvt.intervista_category_name = ivt.visit_type_name
GROUP BY 
    ft.date_id,
    dr.region_id;

-- Step 5: Verify data was loaded correctly - grouped by month and region
SELECT 
    EXTRACT(YEAR FROM dd.full_date)::INTEGER AS year, 
    EXTRACT(MONTH FROM dd.full_date)::INTEGER AS month,
    dr.region_name,
    dr.region_type,
    COUNT(*) as record_count,
    SUM(total_visitors) as total_visitors
FROM dw.fact_visitor fv
JOIN dw.dim_date dd ON fv.date_id = dd.date_id
JOIN dw.dim_region dr ON dr.region_id = fv.region_id
WHERE dd.year = 2023
GROUP BY 
    EXTRACT(YEAR FROM dd.full_date), 
    EXTRACT(MONTH FROM dd.full_date), 
    dr.region_name,
    dr.region_type
ORDER BY 
    year, month, dr.region_name;