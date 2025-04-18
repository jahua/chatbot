-- Script to load data from staging tables into fact tables
\echo 'Starting fact table loading process...'

-- Begin transaction
BEGIN;

-- Add unique constraint if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_tourism_unique_key'
    ) THEN
        ALTER TABLE inervista.fact_tourism
        ADD CONSTRAINT fact_tourism_unique_key 
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
FROM edw.stg_aoi_visitors
WHERE ABS(total_visitors_structured - 
    (COALESCE(swiss_tourists_raw, 0) + 
     COALESCE(foreign_tourists_raw, 0) + 
     COALESCE(swiss_locals_raw, 0) + 
     COALESCE(foreign_workers_raw, 0) + 
     COALESCE(swiss_commuters_raw, 0))) > 0.01;

\echo 'Loading data into fact_tourism...'
INSERT INTO inervista.fact_tourism (
    date_id,
    region_id,
    object_type_id,
    visit_type_id,
    data_type_id,
    total,
    staydays,
    basis,
    sex_male,
    sex_female,
    age_15_29,
    age_30_44
)
SELECT 
    s.date_id,
    s.region_id,
    s.object_type_id,
    s.visit_type_id,
    s.data_type_id,
    s.total_visitors_structured as total,
    NULL as staydays, -- Placeholder as this might come from a different source
    s.total_visitors_structured as basis,
    s.sex_male,
    s.total_visitors_structured - s.sex_male as sex_female,
    s.age_15_29,
    s.age_30_44
FROM edw.stg_aoi_visitors s
LEFT JOIN invalid_records ir ON s.date_id = ir.date_id AND s.region_id = ir.region_id
WHERE ir.date_id IS NULL -- Exclude invalid records
ON CONFLICT (date_id, region_id, object_type_id, visit_type_id, data_type_id) 
DO UPDATE SET
    total = EXCLUDED.total,
    staydays = EXCLUDED.staydays,
    basis = EXCLUDED.basis,
    sex_male = EXCLUDED.sex_male,
    sex_female = EXCLUDED.sex_female,
    age_15_29 = EXCLUDED.age_15_29,
    age_30_44 = EXCLUDED.age_30_44;

\echo 'Checking for records that failed to load...'
SELECT COUNT(*) as invalid_record_count, error_description
FROM invalid_records
GROUP BY error_description;

-- If everything is successful, commit the transaction
COMMIT;

\echo 'Fact table loading process complete.' 