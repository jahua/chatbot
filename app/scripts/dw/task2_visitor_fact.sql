-- Unified Visitor Fact Table Design
-- This design accommodates both AOI and Intervista data sources

CREATE TABLE dw.fact_visitors (
    -- Primary key and dimensions
    fact_id BIGSERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL,              -- Reference to dim_date
    region_id INTEGER NOT NULL,            -- Reference to dim_region
    visitor_type_id INTEGER NOT NULL,      -- Reference to dim_visitor_type
    object_type_id INTEGER,                -- From Intervista/AOI
    data_type_id INTEGER,                  -- From Intervista/AOI
    
    -- Core metrics (common across sources)
    total_visitor_count NUMERIC NOT NULL,  -- 'total' in Intervista, computed from AOI categories
    
    -- AOI specific visitor categories
    swiss_tourists_count NUMERIC,          -- From AOI 'visitors' JSONB
    foreign_tourists_count NUMERIC,        -- From AOI 'visitors' JSONB
    swiss_locals_count NUMERIC,            -- From AOI 'visitors' JSONB
    foreign_workers_count NUMERIC,         -- From AOI 'visitors' JSONB
    swiss_commuters_count NUMERIC,         -- From AOI 'visitors' JSONB
    
    -- Common demographic breakdowns
    age_15_29_count NUMERIC,               -- Both sources
    age_30_44_count NUMERIC,               -- Both sources
    age_45_59_count NUMERIC,               -- Both sources
    age_60_plus_count NUMERIC,             -- Both sources
    male_count NUMERIC,                    -- Both sources
    female_count NUMERIC,                  -- Both sources
    
    -- Dwell time metrics
    avg_stay_duration_mins NUMERIC(8,2),   -- Average stay in minutes
    stay_days NUMERIC(5,2),                -- From Intervista 'staydays'
    
    -- Intervista specific metrics
    basis NUMERIC,                         -- From Intervista
    
    -- Transportation metrics (from Intervista)
    transport_abroad NUMERIC,
    transport_invehicle NUMERIC,
    transport_onbicycle NUMERIC,
    transport_onfoot NUMERIC,
    transport_other NUMERIC,
    transport_public NUMERIC,
    
    -- Household size (from Intervista)
    size_hh_1_2 NUMERIC,
    size_hh_3_plus NUMERIC,
    
    -- Education level (from Intervista)
    educ_low NUMERIC,
    educ_medium NUMERIC,
    educ_high NUMERIC,
    
    -- Origin data
    origin_breakdown JSONB,                -- Structured origin data from both sources
    
    -- ETL metadata
    source_system VARCHAR(20) NOT NULL,    -- 'aoi', 'intervista'
    source_keys JSONB,                     -- Reference back to source system
    is_overnight BOOLEAN DEFAULT FALSE,    -- Flag for overnight visitors
    batch_id INTEGER NOT NULL,             -- ETL batch ID
    quality_score NUMERIC(5,2),            -- Data quality indicator (0-100)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT fact_visitors_date_fk FOREIGN KEY (date_id) REFERENCES dw.dim_date(date_id),
    CONSTRAINT fact_visitors_region_fk FOREIGN KEY (region_id) REFERENCES dw.dim_region(region_id),
    CONSTRAINT fact_visitors_visitor_type_fk FOREIGN KEY (visitor_type_id) REFERENCES dw.dim_visitor_type(visitor_type_id),
    CONSTRAINT fact_visitors_unique UNIQUE (date_id, region_id, visitor_type_id, object_type_id, data_type_id, source_system)
)
PARTITION BY RANGE (date_id);

-- Create partitions
CREATE TABLE dw.fact_visitors_y2023 PARTITION OF dw.fact_visitors
    FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE dw.fact_visitors_y2024 PARTITION OF dw.fact_visitors
    FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE dw.fact_visitors_y2025 PARTITION OF dw.fact_visitors
    FOR VALUES FROM (20250101) TO (20260101);

-- Create indexes for performance
CREATE INDEX idx_fact_visitors_date ON dw.fact_visitors(date_id);
CREATE INDEX idx_fact_visitors_region ON dw.fact_visitors(region_id);
CREATE INDEX idx_fact_visitors_visitor_type ON dw.fact_visitors(visitor_type_id);
CREATE INDEX idx_fact_visitors_source ON dw.fact_visitors(source_system);
CREATE INDEX idx_fact_visitors_batch ON dw.fact_visitors(batch_id);
CREATE INDEX idx_fact_visitors_date_region ON dw.fact_visitors(date_id, region_id);

-- Function to load AOI data into unified fact table
CREATE OR REPLACE FUNCTION dw.load_aoi_visitor_data(p_batch_id INTEGER)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Insert data from AOI staging into fact table
    INSERT INTO dw.fact_visitors (
        date_id,
        region_id,
        visitor_type_id,
        object_type_id,
        data_type_id,
        total_visitor_count,
        swiss_tourists_count,
        foreign_tourists_count,
        swiss_locals_count,
        foreign_workers_count,
        swiss_commuters_count,
        age_15_29_count,
        age_30_44_count,
        age_45_59_count,
        age_60_plus_count,
        male_count,
        female_count,
        avg_stay_duration_mins,
        origin_breakdown,
        source_system,
        source_keys,
        is_overnight,
        batch_id
    )
    SELECT
        a.date_id,
        a.region_id,
        1, -- Default visitor type - adjust based on your visitor type dimension
        a.object_type_id,
        a.data_type_id,
        a.total_visitors_structured,
        a.swiss_tourists_raw,
        a.foreign_tourists_raw,
        a.swiss_locals_raw,
        a.foreign_workers_raw,
        a.swiss_commuters_raw,
        a.age_15_29,
        a.age_30_44,
        a.age_45_59,
        a.age_60_plus,
        a.sex_male,
        a.sex_female,
        a.avg_dwell_time_mins,
        jsonb_build_object(
            'top_foreign_countries', a.top_foreign_countries,
            'top_swiss_cantons', a.top_swiss_cantons
        ),
        'aoi',
        a.source_keys,
        -- Check if overnights exist in source data
        (a.source_keys->>'overnights')::boolean,
        p_batch_id
    FROM
        edw.stg_aoi_visitors_daily a
    WHERE
        NOT EXISTS (
            SELECT 1 FROM dw.fact_visitors f
            WHERE f.date_id = a.date_id
            AND f.region_id = a.region_id
            AND f.source_system = 'aoi'
            AND COALESCE(f.object_type_id, 0) = COALESCE(a.object_type_id, 0)
            AND COALESCE(f.data_type_id, 0) = COALESCE(a.data_type_id, 0)
        );
    
    -- Get count of inserted records
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Function to load Intervista data into unified fact table
CREATE OR REPLACE FUNCTION dw.load_intervista_visitor_data(p_batch_id INTEGER)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    -- Insert data from Intervista staging into fact table
    INSERT INTO dw.fact_visitors (
        date_id,
        region_id,
        visitor_type_id,
        object_type_id,
        data_type_id,
        total_visitor_count,
        age_15_29_count,
        age_30_44_count,
        age_45_59_count,
        age_60_plus_count,
        male_count,
        female_count,
        stay_days,
        basis,
        transport_abroad,
        transport_invehicle,
        transport_onbicycle,
        transport_onfoot,
        transport_other,
        transport_public,
        size_hh_1_2,
        size_hh_3_plus,
        educ_low,
        educ_medium,
        educ_high,
        origin_breakdown,
        source_system,
        batch_id
    )
    SELECT
        i.date_id,
        i.region_id,
        i.visit_type_id, -- Using visit_type_id from Intervista
        i.object_type_id,
        i.data_type_id,
        i.total, -- Total visitors from Intervista
        i.age_15_29,
        i.age_30_44,
        i.age_45_59,
        i.age_60_plus,
        i.sex_male,
        i.sex_female,
        i.staydays,
        i.basis,
        i.transport_abroad,
        i.transport_invehicle,
        i.transport_onbicycle,
        i.transport_onfoot,
        i.transport_other,
        i.transport_public,
        i.size_hh_1_2,
        i.size_hh_3_plus,
        i.educ_low,
        i.educ_medium,
        i.educ_high,
        -- Create a structured JSONB object for origin data
        jsonb_build_object(
            'origin_D', i.origin_D,
            'origin_F', i.origin_F,
            'origin_I', i.origin_I,
            'cantons', jsonb_build_object(
                'AG', i.canton_AG,
                'AR', i.canton_AR,
                'AI', i.canton_AI,
                'BL', i.canton_BL,
                'BS', i.canton_BS,
                'BE', i.canton_BE,
                'FR', i.canton_FR,
                'GE', i.canton_GE,
                'GL', i.canton_GL,
                'GR', i.canton_GR,
                'JU', i.canton_JU,
                'LU', i.canton_LU,
                'NE', i.canton_NE,
                'NW', i.canton_NW,
                'OW', i.canton_OW,
                'SH', i.canton_SH,
                'SZ', i.canton_SZ,
                'SO', i.canton_SO,
                'SG', i.canton_SG,
                'TG', i.canton_TG,
                'UR', i.canton_UR,
                'VD', i.canton_VD,
                'VS', i.canton_VS,
                'ZG', i.canton_ZG,
                'ZH', i.canton_ZH
            )
        ),
        'intervista',
        p_batch_id
    FROM
        inervista.fact_tourism i
    WHERE
        NOT EXISTS (
            SELECT 1 FROM dw.fact_visitors f
            WHERE f.date_id = i.date_id
            AND f.region_id = i.region_id
            AND f.visitor_type_id = i.visit_type_id
            AND f.source_system = 'intervista'
            AND COALESCE(f.object_type_id, 0) = COALESCE(i.object_type_id, 0)
            AND COALESCE(f.data_type_id, 0) = COALESCE(i.data_type_id, 0)
        );
    
    -- Get count of inserted records
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Create a view to show combined visitor data
CREATE OR REPLACE VIEW dw.v_visitor_summary AS
SELECT 
    r.region_name,
    COALESCE(SUM(CASE WHEN f.source_system = 'aoi' THEN f.total_visitor_count ELSE 0 END), 0) AS aoi_visitors,
    COALESCE(SUM(CASE WHEN f.source_system = 'intervista' THEN f.total_visitor_count ELSE 0 END), 0) AS intervista_visitors,
    COALESCE(SUM(f.swiss_tourists_count), 0) AS swiss_tourists,
    COALESCE(SUM(f.foreign_tourists_count), 0) AS foreign_tourists,
    COALESCE(AVG(f.avg_stay_duration_mins), 0) AS avg_stay_duration_mins,
    COALESCE(AVG(f.stay_days), 0) AS avg_stay_days
FROM 
    dw.dim_region r
LEFT JOIN 
    dw.fact_visitors f ON r.region_id = f.region_id
WHERE 
    r.region_type = 'canton'
    AND f.date_id = 20240315
GROUP BY 
    r.region_name;

-- Test the view
SELECT * FROM dw.v_visitor_summary ORDER BY aoi_visitors DESC; 