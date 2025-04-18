-- Script to load data from staging table into fact_tourism_visitors
\echo 'Starting fact_tourism_visitors loading process...'

-- Begin transaction
BEGIN;

-- Add unique constraint if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_tourism_visitors_unique_key'
    ) THEN
        ALTER TABLE dw.fact_tourism_visitors
        ADD CONSTRAINT fact_tourism_visitors_unique_key 
        UNIQUE (date_id, region_id, object_type_id, visit_type_id, data_type_id);
    END IF;
END $$;

\echo 'Creating temporary table for invalid records...'
CREATE TEMP TABLE invalid_records (
    date_id INTEGER,
    region_id INTEGER,
    error_description TEXT
);

-- Insert records that fail validation into invalid_records
INSERT INTO invalid_records
SELECT 
    date_id,
    region_id,
    'Data consistency error: total visitors does not match component sum'
FROM dw.stg_aoi_visitors
WHERE ABS(total_visitors_structured - 
    (COALESCE(swiss_tourists_raw, 0) + 
     COALESCE(foreign_tourists_raw, 0) + 
     COALESCE(swiss_locals_raw, 0) + 
     COALESCE(foreign_workers_raw, 0) + 
     COALESCE(swiss_commuters_raw, 0))) > 0.01;

\echo 'Loading data into fact_tourism_visitors...'
INSERT INTO dw.fact_tourism_visitors (
    date_id,
    region_id,
    object_type_id,
    visit_type_id,
    data_type_id,
    total_visitors_structured,
    age_15_29,
    age_30_44,
    age_45_59,
    age_60_plus,
    basis,
    sex_female,
    sex_male,
    size_hh_1_2,
    size_hh_3_plus
)
SELECT 
    s.date_id,
    s.region_id,
    s.object_type_id,
    s.visit_type_id,
    s.data_type_id,
    s.total_visitors_structured,
    s.age_15_29,
    s.age_30_44,
    -- Estimate age_45_59 as a portion of remaining visitors
    (s.total_visitors_structured - COALESCE(s.age_15_29, 0) - COALESCE(s.age_30_44, 0)) * 0.4 as age_45_59,
    -- Estimate age_60_plus as the remaining portion
    (s.total_visitors_structured - COALESCE(s.age_15_29, 0) - COALESCE(s.age_30_44, 0)) * 0.6 as age_60_plus,
    s.total_visitors_structured as basis,
    s.total_visitors_structured - s.sex_male as sex_female,
    s.sex_male,
    -- Estimate household sizes based on typical distribution
    s.total_visitors_structured * 0.6 as size_hh_1_2,
    s.total_visitors_structured * 0.4 as size_hh_3_plus
FROM dw.stg_aoi_visitors s
LEFT JOIN invalid_records ir ON s.date_id = ir.date_id AND s.region_id = ir.region_id
WHERE ir.date_id IS NULL -- Exclude invalid records
ON CONFLICT (date_id, region_id, object_type_id, visit_type_id, data_type_id) 
DO UPDATE SET
    total_visitors_structured = EXCLUDED.total_visitors_structured,
    age_15_29 = EXCLUDED.age_15_29,
    age_30_44 = EXCLUDED.age_30_44,
    age_45_59 = EXCLUDED.age_45_59,
    age_60_plus = EXCLUDED.age_60_plus,
    basis = EXCLUDED.basis,
    sex_female = EXCLUDED.sex_female,
    sex_male = EXCLUDED.sex_male,
    size_hh_1_2 = EXCLUDED.size_hh_1_2,
    size_hh_3_plus = EXCLUDED.size_hh_3_plus;

\echo 'Checking for records that failed to load...'
SELECT COUNT(*) as invalid_record_count, error_description
FROM invalid_records
GROUP BY error_description;

-- If everything is successful, commit the transaction
COMMIT;

\echo 'Fact table loading process complete.' 