-- Troubleshooting script to diagnose fact_spending insertion issues

-- 1. Check ETL metadata
SELECT * FROM dw.etl_metadata 
WHERE process_name = 'load_mastercard_spending' 
ORDER BY etl_id DESC LIMIT 3;

-- 2. Check the latest batch_id
\echo 'Latest ETL batch ID:'
SELECT MAX(etl_id) FROM dw.etl_metadata WHERE process_name = 'load_mastercard_spending';

-- 3. Check the date dimension, specifically the first date in the range we're loading
\echo 'Checking first date in range (2022-01-01):'
SELECT date_id, day_of_week, is_weekend 
FROM dw.dim_date WHERE date_id = 20220101;

-- 4. Check region mappings
\echo 'Geography-to-Region mapping count:'
SELECT COUNT(*) FROM dw.temp_geography_region_map;

\echo 'Sample geography-to-region mappings:'
SELECT g.geography_id, g.geo_name, g.geo_type, r.region_id, r.region_name, r.region_type
FROM dw.dim_geography g
JOIN dw.temp_geography_region_map m ON g.geography_id = m.geography_id
JOIN dw.dim_region r ON m.region_id = r.region_id
LIMIT 5;

-- 5. Check industry dimension
\echo 'Sample industries:'
SELECT * FROM dw.dim_industry LIMIT 5;

-- 6. Check fact table structure
\echo 'Fact table structure:'
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_schema = 'dw' AND table_name = 'fact_spending' 
ORDER BY ordinal_position;

-- 7. Run a simplified version of the fact insertion to debug
\echo 'Trying simplified fact insertion:'
EXPLAIN (ANALYZE FALSE) 
INSERT INTO dw.fact_spending (
    date_id,
    industry_id,
    region_id,
    transaction_count,
    total_amount,
    avg_transaction,
    source_system,
    batch_id
)
SELECT 
    TO_CHAR(mc.txn_date, 'YYYYMMDD')::INTEGER,
    di.industry_id,
    m.region_id,
    SUM(mc.txn_cnt),
    SUM(mc.txn_amt),
    CASE WHEN SUM(mc.txn_cnt) > 0 THEN SUM(mc.txn_amt) / SUM(mc.txn_cnt) ELSE 0 END,
    'mastercard',
    (SELECT MAX(etl_id) FROM dw.etl_metadata WHERE process_name = 'load_mastercard_spending')
FROM data_lake.master_card mc
JOIN dw.dim_industry di ON di.industry_name = mc.industry
JOIN dw.dim_geography dg ON TRIM(dg.geo_name) = TRIM(mc.geo_name) AND dg.geo_type = mc.geo_type
JOIN dw.temp_geography_region_map m ON m.geography_id = dg.geography_id
WHERE mc.txn_date = '2022-01-01'
GROUP BY 
    TO_CHAR(mc.txn_date, 'YYYYMMDD')::INTEGER,
    di.industry_id,
    m.region_id
LIMIT 1;

-- 8. Check for missing data in the join
\echo 'Checking for data in each step of the join chain (2022-01-01):'
SELECT 
    'Source data' as step,
    COUNT(*) as count
FROM data_lake.master_card
WHERE txn_date = '2022-01-01'

UNION ALL

SELECT 
    'After industry join' as step,
    COUNT(*) as count
FROM data_lake.master_card mc
JOIN dw.dim_industry di ON di.industry_name = mc.industry
WHERE mc.txn_date = '2022-01-01'

UNION ALL

SELECT 
    'After geography join' as step,
    COUNT(*) as count
FROM data_lake.master_card mc
JOIN dw.dim_industry di ON di.industry_name = mc.industry
JOIN dw.dim_geography dg ON TRIM(dg.geo_name) = TRIM(mc.geo_name) AND dg.geo_type = mc.geo_type
WHERE mc.txn_date = '2022-01-01'

UNION ALL

SELECT 
    'After region mapping join' as step,
    COUNT(*) as count
FROM data_lake.master_card mc
JOIN dw.dim_industry di ON di.industry_name = mc.industry
JOIN dw.dim_geography dg ON TRIM(dg.geo_name) = TRIM(mc.geo_name) AND dg.geo_type = mc.geo_type
JOIN dw.temp_geography_region_map m ON m.geography_id = dg.geography_id
WHERE mc.txn_date = '2022-01-01'; 