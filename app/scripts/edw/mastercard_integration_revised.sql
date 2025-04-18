-- MasterCard Data Integration Script - PART 1: Setup and Data Loading (FIXED)
-- This script handles the table creation and data loading parts
-- Created: April 14, 2025

-- Step 1: Create a mapping table for geo_names to region_ids if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.mastercard_region_mapping (
    geo_type VARCHAR(100) NOT NULL,
    geo_name VARCHAR(255) NOT NULL,
    region_id INTEGER NOT NULL,
    mapping_source TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (geo_type, geo_name)
);

-- Step 2: Create a transaction date dimension if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.dim_transaction_date (
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

-- Step 3: Create the staging table for MasterCard transaction data
DROP TABLE IF EXISTS edw.stg_mastercard_transactions;
CREATE TABLE edw.stg_mastercard_transactions (
    date_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    industry_id INTEGER NOT NULL,
    txn_date DATE NOT NULL,
    txn_amt NUMERIC,
    txn_cnt NUMERIC,
    acct_cnt NUMERIC,
    avg_ticket NUMERIC,
    avg_freq NUMERIC,
    avg_spend_amt NUMERIC,
    yoy_txn_amt NUMERIC,
    yoy_txn_cnt NUMERIC,
    quad_id VARCHAR(100),
    central_latitude NUMERIC,
    central_longitude NUMERIC,
    bounding_box TEXT,
    source_keys JSONB,
    PRIMARY KEY (date_id, region_id, industry_id)
);

-- Step 4: Create fact table for daily tourism spending if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.fact_tourism_spending_daily (
    fact_id SERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    industry_id INTEGER NOT NULL,
    visit_type_id INTEGER NOT NULL,
    data_type_id INTEGER NOT NULL,
    txn_date DATE NOT NULL,
    txn_amt_index NUMERIC,
    txn_cnt_index NUMERIC,
    acct_cnt_index NUMERIC,
    avg_ticket_index NUMERIC,
    avg_freq_index NUMERIC,
    avg_spend_amt_index NUMERIC,
    yoy_txn_amt_pct NUMERIC,
    yoy_txn_cnt_pct NUMERIC,
    quad_id VARCHAR(100),
    central_latitude NUMERIC,
    central_longitude NUMERIC,
    bounding_box TEXT,
    source_keys JSONB,
    has_transaction_data BOOLEAN DEFAULT TRUE,
    data_completion_pct NUMERIC DEFAULT 100,
    data_sources VARCHAR(255) DEFAULT 'mastercard',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (date_id, region_id, industry_id, visit_type_id, data_type_id)
);

-- Step 5: Create daily summary table if it doesn't exist
CREATE TABLE IF NOT EXISTS edw.daily_tourism_performance_summary (
    summary_date DATE PRIMARY KEY,
    total_regions INTEGER,
    total_industries INTEGER,
    avg_transaction_index NUMERIC,
    max_transaction_index NUMERIC,
    min_transaction_index NUMERIC,
    top_performing_region VARCHAR(255),
    top_performing_industry VARCHAR(255),
    bottom_performing_region VARCHAR(255),
    bottom_performing_industry VARCHAR(255),
    avg_yoy_change_pct NUMERIC,
    is_peak_day BOOLEAN,
    day_notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Step 6: Populate the region mapping table
-- Switzerland country mapping
INSERT INTO edw.mastercard_region_mapping (geo_type, geo_name, region_id, mapping_source)
VALUES 
    ('Country', 'Switzerland', 1, 'manual_mapping')
ON CONFLICT (geo_type, geo_name) DO NOTHING;

-- Canton level mappings
INSERT INTO edw.mastercard_region_mapping (geo_type, geo_name, region_id, mapping_source)
VALUES 
    ('State', 'Zürich', 26, 'manual_mapping'),
    ('State', 'Genève', 9, 'manual_mapping'),
    ('State', 'Graubünden', 11, 'manual_mapping'),
    ('State', 'Neuchâtel', 14, 'manual_mapping'),
    ('State', 'Luzern', 13, 'manual_mapping'),
    ('State', 'Basel-Land', 5, 'manual_mapping'),
    ('State', 'Basel-Stadt', 6, 'manual_mapping'),
    ('State', 'Sankt Gallen', 20, 'manual_mapping'),
    ('State', 'Fribourg', 8, 'manual_mapping'),
    ('State', 'Vaud', 23, 'manual_mapping'),
    ('State', 'Bern', 7, 'manual_mapping'),
    ('State', 'Aargau', 2, 'manual_mapping'),
    ('State', 'Valais', 24, 'manual_mapping'),
    ('State', 'Ticino', 27, 'manual_mapping'),
    ('State', 'Thurgau', 21, 'manual_mapping'),
    ('State', 'Solothurn', 19, 'manual_mapping'),
    ('State', 'Schwyz', 18, 'manual_mapping'),
    ('State', 'Zug', 25, 'manual_mapping'),
    ('State', 'Schaffhausen', 17, 'manual_mapping'),
    ('State', 'Jura', 12, 'manual_mapping'),
    ('State', 'Glarus', 10, 'manual_mapping'),
    ('State', 'Obwalden', 16, 'manual_mapping'),
    ('State', 'Nidwalden', 15, 'manual_mapping'),
    ('State', 'Uri', 22, 'manual_mapping'),
    ('State', 'Appenzell Ausserrhoden', 3, 'manual_mapping'),
    ('State', 'Appenzell Innerrhoden', 4, 'manual_mapping')
ON CONFLICT (geo_type, geo_name) DO NOTHING;

-- Handle encoded versions of names (if they appear in the source data)
INSERT INTO edw.mastercard_region_mapping (geo_type, geo_name, region_id, mapping_source)
VALUES 
    ('State', 'ZÃÂ¼rich', 26, 'manual_mapping'),
    ('State', 'GenÃÂ¨ve', 9, 'manual_mapping'),
    ('State', 'GraubÃÂ¼nden', 11, 'manual_mapping'),
    ('State', 'NeuchÃÂ¢tel', 14, 'manual_mapping'),
    ('State', 'BÃÂ¼lach', 26, 'canton_rollup')
ON CONFLICT (geo_type, geo_name) DO NOTHING;

-- Key tourism regions in Ticino
INSERT INTO edw.mastercard_region_mapping (geo_type, geo_name, region_id, mapping_source)
VALUES 
    ('Msa', 'Lugano', 34, 'manual_mapping'),
    ('Msa', 'Locarno', 33, 'manual_mapping'),
    ('Msa', 'Bellinzona', 32, 'manual_mapping'),
    ('Msa', 'Mendrisio', 31, 'manual_mapping')
ON CONFLICT (geo_type, geo_name) DO NOTHING;

-- Additional MSAs mapped to cantons
INSERT INTO edw.mastercard_region_mapping (geo_type, geo_name, region_id, mapping_source)
VALUES 
    -- Zurich Canton MSAs
    ('Msa', 'Zürich', 26, 'canton_rollup'),
    ('Msa', 'Winterthur', 26, 'canton_rollup'),
    ('Msa', 'Uster', 26, 'canton_rollup'),
    ('Msa', 'Dietikon', 26, 'canton_rollup'),
    ('Msa', 'Horgen', 26, 'canton_rollup'),
    ('Msa', 'Meilen', 26, 'canton_rollup'),
    ('Msa', 'Hinwil', 26, 'canton_rollup'),
    ('Msa', 'Dielsdorf', 26, 'canton_rollup'),
    ('Msa', 'Pfäffikon', 26, 'canton_rollup'),
    ('Msa', 'Affoltern', 26, 'canton_rollup'),
    ('Msa', 'Andelfingen', 26, 'canton_rollup'),
    
    -- Bern Canton MSAs
    ('Msa', 'Bern-Mittelland', 7, 'canton_rollup'),
    ('Msa', 'Interlaken-Oberhasli', 7, 'canton_rollup'),
    ('Msa', 'Thun', 7, 'canton_rollup'),
    ('Msa', 'Emmental', 7, 'canton_rollup'),
    ('Msa', 'Oberaargau', 7, 'canton_rollup'),
    ('Msa', 'Seeland', 7, 'canton_rollup'),
    ('Msa', 'Frutigen-Niedersimmental', 7, 'canton_rollup'),
    ('Msa', 'Jura bernois', 7, 'canton_rollup'),
    ('Msa', 'Obersimmental-Saanen', 7, 'canton_rollup'),
    
    -- Geneva Canton MSAs
    ('Msa', 'Genève', 9, 'canton_rollup')
ON CONFLICT (geo_type, geo_name) DO NOTHING;

-- Step 7: Populate the date dimension table with dates from MasterCard data
INSERT INTO edw.dim_transaction_date (
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
SELECT DISTINCT
    mc.txn_date AS full_date,
    EXTRACT(ISODOW FROM mc.txn_date) AS day_of_week,
    TO_CHAR(mc.txn_date, 'Day') AS day_name,
    EXTRACT(DAY FROM mc.txn_date) AS day_of_month,
    EXTRACT(DOY FROM mc.txn_date) AS day_of_year,
    EXTRACT(MONTH FROM mc.txn_date) AS month_number,
    TO_CHAR(mc.txn_date, 'Month') AS month,
    EXTRACT(QUARTER FROM mc.txn_date) AS quarter,
    EXTRACT(YEAR FROM mc.txn_date) AS year,
    CASE WHEN EXTRACT(ISODOW FROM mc.txn_date) < 6 THEN TRUE ELSE FALSE END AS is_weekday,
    CASE
        WHEN EXTRACT(MONTH FROM mc.txn_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM mc.txn_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM mc.txn_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season
FROM
    data_lake.master_card mc
ON CONFLICT (full_date) DO NOTHING;

-- Step 8: Use a temporary table to ensure no duplicates in staging data
-- Drop if it exists
DROP TABLE IF EXISTS edw.temp_mastercard_deduped;

-- Create the temp table
CREATE TEMP TABLE temp_mastercard_deduped AS
SELECT DISTINCT
    dtd.date_id,
    mrm.region_id,
    di.industry_id,
    mc.txn_date,
    -- Use aggregated values when there are duplicates
    AVG(mc.txn_amt) AS txn_amt,
    AVG(mc.txn_cnt) AS txn_cnt,
    AVG(mc.acct_cnt) AS acct_cnt,
    AVG(mc.avg_ticket) AS avg_ticket,
    AVG(mc.avg_freq) AS avg_freq,
    AVG(mc.avg_spend_amt) AS avg_spend_amt,
    AVG(mc.yoy_txn_amt) AS yoy_txn_amt,
    AVG(mc.yoy_txn_cnt) AS yoy_txn_cnt,
    -- Use first value for text/categorical fields
    FIRST_VALUE(mc.quad_id) OVER (PARTITION BY dtd.date_id, mrm.region_id, di.industry_id ORDER BY mc.txn_date) AS quad_id,
    AVG(mc.central_latitude) AS central_latitude,
    AVG(mc.central_longitude) AS central_longitude,
    FIRST_VALUE(mc.bounding_box) OVER (PARTITION BY dtd.date_id, mrm.region_id, di.industry_id ORDER BY mc.txn_date) AS bounding_box,
    FIRST_VALUE(
        jsonb_build_object(
            'source_table', 'data_lake.master_card',
            'geo_type', mc.geo_type,
            'geo_name', mc.geo_name,
            'industry', mc.industry,
            'segment', mc.segment,
            'quad_id', mc.quad_id,
            'year', mc.yr
        )
    ) OVER (PARTITION BY dtd.date_id, mrm.region_id, di.industry_id ORDER BY mc.txn_date) AS source_keys
FROM 
    data_lake.master_card mc
JOIN 
    edw.mastercard_region_mapping mrm ON mc.geo_type = mrm.geo_type AND mc.geo_name = mrm.geo_name
JOIN 
    edw.dim_industry di ON mc.industry = di.industry_name
JOIN 
    edw.dim_transaction_date dtd ON mc.txn_date = dtd.full_date
WHERE 
    mc.industry IN ('Accommodations', 'Eating Places', 'Bars/Taverns/Nightclubs', 'Art, Entertainment and Recreation')
GROUP BY
    dtd.date_id,
    mrm.region_id,
    di.industry_id,
    mc.txn_date,
    mc.quad_id,
    mc.bounding_box,
    mc.geo_type,
    mc.geo_name,
    mc.industry,
    mc.segment,
    mc.yr,
    mc.quad_id;

-- Truncate the staging table
TRUNCATE TABLE edw.stg_mastercard_transactions;

-- Now, insert deduplicated records into the staging table
INSERT INTO edw.stg_mastercard_transactions
SELECT 
    date_id,
    region_id,
    industry_id,
    txn_date,
    txn_amt,
    txn_cnt,
    acct_cnt,
    avg_ticket,
    avg_freq,
    avg_spend_amt,
    yoy_txn_amt,
    yoy_txn_cnt,
    quad_id,
    central_latitude,
    central_longitude,
    bounding_box,
    source_keys
FROM 
    temp_mastercard_deduped;

-- Drop the temporary table
DROP TABLE temp_mastercard_deduped;

-- Step 9: Insert transaction data into the fact table
INSERT INTO edw.fact_tourism_spending_daily (
    date_id,
    region_id,
    industry_id,
    visit_type_id,
    data_type_id,
    txn_date,
    txn_amt_index,
    txn_cnt_index,
    acct_cnt_index,
    avg_ticket_index,
    avg_freq_index,
    avg_spend_amt_index,
    yoy_txn_amt_pct,
    yoy_txn_cnt_pct,
    quad_id,
    central_latitude,
    central_longitude,
    bounding_box,
    source_keys
)
SELECT
    stg.date_id,
    stg.region_id,
    stg.industry_id,
    1 AS visit_type_id, -- Assuming 1 is for all visitors
    2 AS data_type_id,  -- Assuming 2 is for predicted values
    stg.txn_date,
    stg.txn_amt,
    stg.txn_cnt,
    stg.acct_cnt,
    stg.avg_ticket,
    stg.avg_freq,
    stg.avg_spend_amt,
    stg.yoy_txn_amt,
    stg.yoy_txn_cnt,
    stg.quad_id,
    stg.central_latitude,
    stg.central_longitude,
    stg.bounding_box,
    stg.source_keys
FROM
    edw.stg_mastercard_transactions stg
ON CONFLICT (date_id, region_id, industry_id, visit_type_id, data_type_id)
DO UPDATE SET
    txn_amt_index = EXCLUDED.txn_amt_index,
    txn_cnt_index = EXCLUDED.txn_cnt_index,
    acct_cnt_index = EXCLUDED.acct_cnt_index,
    avg_ticket_index = EXCLUDED.avg_ticket_index,
    avg_freq_index = EXCLUDED.avg_freq_index,
    avg_spend_amt_index = EXCLUDED.avg_spend_amt_index,
    yoy_txn_amt_pct = EXCLUDED.yoy_txn_amt_pct,
    yoy_txn_cnt_pct = EXCLUDED.yoy_txn_cnt_pct,
    quad_id = EXCLUDED.quad_id,
    central_latitude = EXCLUDED.central_latitude,
    central_longitude = EXCLUDED.central_longitude,
    bounding_box = EXCLUDED.bounding_box,
    source_keys = EXCLUDED.source_keys,
    has_transaction_data = TRUE,
    data_completion_pct = 100,
    data_sources = CASE 
        WHEN fact_tourism_spending_daily.data_sources IS NULL THEN 'mastercard'
        WHEN fact_tourism_spending_daily.data_sources NOT LIKE '%mastercard%' THEN fact_tourism_spending_daily.data_sources || ', mastercard'
        ELSE fact_tourism_spending_daily.data_sources
    END,
    updated_at = NOW();

-- Log completion of Part 1
DO $$
BEGIN
    RAISE NOTICE 'MasterCard Data Integration Part 1 completed successfully at %', NOW();
END $$;