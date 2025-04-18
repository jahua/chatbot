-- AOI Data Integration Script for trip_dw - Daily Granularity Version
-- This script integrates data from data_lake.aoi_days_raw into the EDW schema
-- Created: April 13, 2025

-- Step 1: Create a mapping table for AOI IDs to region_ids if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.aoi_region_mapping (
    aoi_id VARCHAR(100) PRIMARY KEY,
    region_id INTEGER NOT NULL REFERENCES edw.dim_region(region_id),
    mapping_source TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Step 2: Add mapping for the AOI ID we found
INSERT INTO edw.aoi_region_mapping (aoi_id, region_id, mapping_source)
VALUES ('f7883818-99e1-4d20-b09a-5171bf16133a', 1, 'manual_mapping')
ON CONFLICT (aoi_id) DO NOTHING;

-- Step 3: Create a daily time dimension if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.dim_time_daily (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekday BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    season VARCHAR(10) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Populate the daily time dimension for 2023 if needed
INSERT INTO edw.dim_time_daily (
    full_date,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    month_number,
    month,
    quarter,
    year,
    is_weekday,
    season
)
SELECT
    date_series::DATE AS full_date,
    EXTRACT(ISODOW FROM date_series) AS day_of_week,
    TO_CHAR(date_series, 'Day') AS day_name,
    EXTRACT(DAY FROM date_series) AS day_of_month,
    EXTRACT(DOY FROM date_series) AS day_of_year,
    EXTRACT(MONTH FROM date_series) AS month_number,
    TO_CHAR(date_series, 'Month') AS month,
    EXTRACT(QUARTER FROM date_series) AS quarter,
    EXTRACT(YEAR FROM date_series) AS year,
    CASE WHEN EXTRACT(ISODOW FROM date_series) < 6 THEN TRUE ELSE FALSE END AS is_weekday,
    CASE
        WHEN EXTRACT(MONTH FROM date_series) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date_series) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date_series) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season
FROM
    GENERATE_SERIES('2023-01-01'::DATE, '2023-12-31'::DATE, '1 day'::INTERVAL) AS date_series
ON CONFLICT (full_date) DO NOTHING;

-- Step 4: Create a staging table for daily AOI data
DROP TABLE IF EXISTS edw.stg_aoi_visitors_daily;
CREATE TABLE edw.stg_aoi_visitors_daily (
    date_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    visit_type_id INTEGER NOT NULL,
    object_type_id INTEGER NOT NULL,
    data_type_id INTEGER NOT NULL,
    swiss_tourists_raw NUMERIC,
    foreign_tourists_raw NUMERIC,
    swiss_locals_raw NUMERIC,
    foreign_workers_raw NUMERIC,
    swiss_commuters_raw NUMERIC,
    total_visitors_structured NUMERIC,
    age_15_29 NUMERIC,
    age_30_44 NUMERIC,
    age_45_59 NUMERIC,
    age_60_plus NUMERIC,
    sex_male NUMERIC,
    sex_female NUMERIC,
    avg_dwell_time_mins NUMERIC,
    demographics JSONB,
    top_foreign_countries JSONB,
    top_swiss_cantons JSONB,
    source_keys JSONB,
    aoi_date DATE NOT NULL,
    PRIMARY KEY (date_id, region_id, visit_type_id, object_type_id, data_type_id)
);

-- Step 5: Insert daily AOI data into staging
INSERT INTO edw.stg_aoi_visitors_daily
SELECT 
    dt.date_id,
    rm.region_id,
    1 AS visit_type_id, -- total visits
    2 AS object_type_id, -- visitors (not visits)
    2 AS data_type_id,   -- predicted values
    (aoi.visitors->>'swissTourist')::NUMERIC AS swiss_tourists_raw,
    (aoi.visitors->>'foreignTourist')::NUMERIC AS foreign_tourists_raw,
    (aoi.visitors->>'swissLocal')::NUMERIC AS swiss_locals_raw,
    (aoi.visitors->>'foreignWorker')::NUMERIC AS foreign_workers_raw,
    (aoi.visitors->>'swissCommuter')::NUMERIC AS swiss_commuters_raw,
    COALESCE((aoi.visitors->>'swissTourist')::NUMERIC, 0) +
    COALESCE((aoi.visitors->>'foreignTourist')::NUMERIC, 0) +
    COALESCE((aoi.visitors->>'swissLocal')::NUMERIC, 0) +
    COALESCE((aoi.visitors->>'foreignWorker')::NUMERIC, 0) +
    COALESCE((aoi.visitors->>'swissCommuter')::NUMERIC, 0) AS total_visitors_structured,
    (COALESCE((aoi.visitors->>'swissTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissLocal')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignWorker')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissCommuter')::NUMERIC, 0)) *
    COALESCE((aoi.demographics->'ageDistribution'->0)::NUMERIC, 0) AS age_15_29,
    (COALESCE((aoi.visitors->>'swissTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissLocal')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignWorker')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissCommuter')::NUMERIC, 0)) *
    COALESCE((aoi.demographics->'ageDistribution'->1)::NUMERIC, 0) AS age_30_44,
    (COALESCE((aoi.visitors->>'swissTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissLocal')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignWorker')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissCommuter')::NUMERIC, 0)) *
    COALESCE((aoi.demographics->'ageDistribution'->2)::NUMERIC, 0) AS age_45_59,
    (COALESCE((aoi.visitors->>'swissTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissLocal')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignWorker')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissCommuter')::NUMERIC, 0)) *
    COALESCE((aoi.demographics->'ageDistribution'->3)::NUMERIC, 0) AS age_60_plus,
    (COALESCE((aoi.visitors->>'swissTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissLocal')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignWorker')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissCommuter')::NUMERIC, 0)) *
    COALESCE((aoi.demographics->>'maleProportion')::NUMERIC, 0.5) AS sex_male,
    (COALESCE((aoi.visitors->>'swissTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignTourist')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissLocal')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'foreignWorker')::NUMERIC, 0) +
     COALESCE((aoi.visitors->>'swissCommuter')::NUMERIC, 0)) *
    (1 - COALESCE((aoi.demographics->>'maleProportion')::NUMERIC, 0.5)) AS sex_female,
    -- Calculate average dwell time from the buckets
    (COALESCE((aoi.dwelltimes->0)::NUMERIC, 0) * 15 + 
     COALESCE((aoi.dwelltimes->1)::NUMERIC, 0) * 45 + 
     COALESCE((aoi.dwelltimes->2)::NUMERIC, 0) * 75 +
     COALESCE((aoi.dwelltimes->3)::NUMERIC, 0) * 105 +
     COALESCE((aoi.dwelltimes->4)::NUMERIC, 0) * 135 +
     COALESCE((aoi.dwelltimes->5)::NUMERIC, 0) * 165 +
     COALESCE((aoi.dwelltimes->6)::NUMERIC, 0) * 195 +
     COALESCE((aoi.dwelltimes->7)::NUMERIC, 0) * 225 +
     COALESCE((aoi.dwelltimes->8)::NUMERIC, 0) * 255) / 
    NULLIF(
     COALESCE((aoi.dwelltimes->0)::NUMERIC, 0) + 
     COALESCE((aoi.dwelltimes->1)::NUMERIC, 0) + 
     COALESCE((aoi.dwelltimes->2)::NUMERIC, 0) +
     COALESCE((aoi.dwelltimes->3)::NUMERIC, 0) +
     COALESCE((aoi.dwelltimes->4)::NUMERIC, 0) +
     COALESCE((aoi.dwelltimes->5)::NUMERIC, 0) +
     COALESCE((aoi.dwelltimes->6)::NUMERIC, 0) +
     COALESCE((aoi.dwelltimes->7)::NUMERIC, 0) +
     COALESCE((aoi.dwelltimes->8)::NUMERIC, 0), 0) AS avg_dwell_time_mins,
    aoi.demographics AS demographics,
    aoi.top_foreign_countries AS top_foreign_countries,
    aoi.top_swiss_cantons AS top_swiss_cantons,
    jsonb_build_object(
        'aoi_id', aoi.aoi_id,
        'source_system', aoi.source_system
    ) AS source_keys,
    aoi.aoi_date
FROM 
    data_lake.aoi_days_raw aoi
JOIN 
    edw.aoi_region_mapping rm ON aoi.aoi_id = rm.aoi_id
JOIN 
    edw.dim_time_daily dt ON dt.full_date = aoi.aoi_date::DATE;

-- Create a new fact table for daily tourism data if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.fact_tourism_visitors_daily (
    fact_id SERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES edw.dim_time_daily(date_id),
    region_id INTEGER NOT NULL REFERENCES edw.dim_region(region_id),
    visit_type_id INTEGER NOT NULL REFERENCES edw.dim_visit_type(visit_type_id),
    object_type_id INTEGER NOT NULL REFERENCES edw.dim_object_type(object_type_id),
    data_type_id INTEGER NOT NULL REFERENCES edw.dim_data_type(data_type_id),
    total_visitors_structured NUMERIC,
    swiss_tourists_raw NUMERIC,
    foreign_tourists_raw NUMERIC,
    swiss_locals_raw NUMERIC,
    foreign_workers_raw NUMERIC,
    swiss_commuters_raw NUMERIC,
    age_15_29 NUMERIC,
    age_30_44 NUMERIC,
    age_45_59 NUMERIC,
    age_60_plus NUMERIC,
    sex_male NUMERIC,
    sex_female NUMERIC,
    avg_dwell_time_mins NUMERIC,
    demographics JSONB,
    top_foreign_countries JSONB,
    top_swiss_cantons JSONB,
    source_keys JSONB,
    has_raw_data_match BOOLEAN DEFAULT FALSE,
    data_completion_pct NUMERIC DEFAULT 0,
    data_sources TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (date_id, region_id, visit_type_id, object_type_id, data_type_id)
);

-- Step 6: Insert daily AOI data into fact table
INSERT INTO edw.fact_tourism_visitors_daily (
    date_id,
    region_id,
    visit_type_id,
    object_type_id,
    data_type_id,
    total_visitors_structured,
    swiss_tourists_raw,
    foreign_tourists_raw,
    swiss_locals_raw,
    foreign_workers_raw,
    swiss_commuters_raw,
    age_15_29,
    age_30_44,
    age_45_59,
    age_60_plus,
    sex_male,
    sex_female,
    avg_dwell_time_mins,
    demographics,
    top_foreign_countries,
    top_swiss_cantons,
    source_keys,
    has_raw_data_match,
    data_completion_pct,
    data_sources
)
SELECT 
    stg.date_id,
    stg.region_id,
    stg.visit_type_id,
    stg.object_type_id,
    stg.data_type_id,
    stg.total_visitors_structured,
    stg.swiss_tourists_raw,
    stg.foreign_tourists_raw,
    stg.swiss_locals_raw,
    stg.foreign_workers_raw,
    stg.swiss_commuters_raw,
    stg.age_15_29,
    stg.age_30_44,
    stg.age_45_59,
    stg.age_60_plus,
    stg.sex_male,
    stg.sex_female,
    stg.avg_dwell_time_mins,
    stg.demographics,
    stg.top_foreign_countries,
    stg.top_swiss_cantons,
    stg.source_keys,
    TRUE,
    100,
    'aoi_days_raw'
FROM 
    edw.stg_aoi_visitors_daily stg
ON CONFLICT (date_id, region_id, visit_type_id, object_type_id, data_type_id) 
DO UPDATE SET
    total_visitors_structured = EXCLUDED.total_visitors_structured,
    swiss_tourists_raw = EXCLUDED.swiss_tourists_raw,
    foreign_tourists_raw = EXCLUDED.foreign_tourists_raw, 
    swiss_locals_raw = EXCLUDED.swiss_locals_raw,
    foreign_workers_raw = EXCLUDED.foreign_workers_raw,
    swiss_commuters_raw = EXCLUDED.swiss_commuters_raw,
    age_15_29 = EXCLUDED.age_15_29,
    age_30_44 = EXCLUDED.age_30_44,
    age_45_59 = EXCLUDED.age_45_59,
    age_60_plus = EXCLUDED.age_60_plus,
    sex_male = EXCLUDED.sex_male,
    sex_female = EXCLUDED.sex_female,
    avg_dwell_time_mins = EXCLUDED.avg_dwell_time_mins,
    demographics = EXCLUDED.demographics,
    top_foreign_countries = EXCLUDED.top_foreign_countries,
    top_swiss_cantons = EXCLUDED.top_swiss_cantons,
    source_keys = EXCLUDED.source_keys,
    has_raw_data_match = TRUE,
    data_completion_pct = 100,
    data_sources = CASE 
        WHEN fact_tourism_visitors_daily.data_sources IS NULL THEN 'aoi_days_raw'
        WHEN fact_tourism_visitors_daily.data_sources NOT LIKE '%aoi_days_raw%' THEN fact_tourism_visitors_daily.data_sources || ', aoi_days_raw'
        ELSE fact_tourism_visitors_daily.data_sources
    END,
    updated_at = NOW();

-- Create a view to join the daily data with time and region information
CREATE OR REPLACE VIEW edw.vw_tourism_daily AS
SELECT
    f.fact_id,
    t.full_date,
    t.day_name,
    t.day_of_week,
    t.month,
    t.month_number,
    t.year,
    t.season,
    r.region_name,
    r.region_type,
    f.total_visitors_structured AS visitor_count,
    f.swiss_tourists_raw,
    f.foreign_tourists_raw,
    f.swiss_locals_raw,
    f.foreign_workers_raw,
    f.swiss_commuters_raw,
    f.avg_dwell_time_mins,
    f.age_15_29,
    f.age_30_44,
    f.age_45_59,
    f.age_60_plus,
    f.sex_male,
    f.sex_female,
    f.data_completion_pct,
    f.has_raw_data_match,
    f.data_sources
FROM
    edw.fact_tourism_visitors_daily f
JOIN
    edw.dim_time_daily t ON f.date_id = t.date_id
JOIN
    edw.dim_region r ON f.region_id = r.region_id;

-- Final success message with count
SELECT 'AOI Data Integration Complete - Successfully processed ' || 
       COUNT(*) || ' daily visitor records' AS integration_status
FROM edw.fact_tourism_visitors_daily
WHERE has_raw_data_match = TRUE;