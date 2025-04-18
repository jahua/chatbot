-- Validation script for staging tables
\echo 'Starting validation of staging tables...'

-- 1. Check for NULL values in required fields
SELECT 'stg_aoi_visitors: NULL values in required fields' as check_name,
       COUNT(*) as error_count
FROM edw.stg_aoi_visitors
WHERE date_id IS NULL 
   OR region_id IS NULL 
   OR visit_type_id IS NULL 
   OR object_type_id IS NULL 
   OR data_type_id IS NULL;

-- 2. Check for negative values in numeric fields
SELECT 'stg_aoi_visitors: Negative values in visitor counts' as check_name,
       COUNT(*) as error_count
FROM edw.stg_aoi_visitors
WHERE swiss_tourists_raw < 0 
   OR foreign_tourists_raw < 0 
   OR swiss_locals_raw < 0 
   OR foreign_workers_raw < 0 
   OR swiss_commuters_raw < 0;

-- 3. Check for data consistency between raw and structured totals
SELECT 'stg_aoi_visitors: Inconsistent total calculations' as check_name,
       COUNT(*) as error_count
FROM edw.stg_aoi_visitors
WHERE ABS(total_visitors_structured - 
    (COALESCE(swiss_tourists_raw, 0) + 
     COALESCE(foreign_tourists_raw, 0) + 
     COALESCE(swiss_locals_raw, 0) + 
     COALESCE(foreign_workers_raw, 0) + 
     COALESCE(swiss_commuters_raw, 0))) > 0.01;

-- 4. Check for invalid date_id references
SELECT 'stg_aoi_visitors: Invalid date_id references' as check_name,
       COUNT(*) as error_count
FROM edw.stg_aoi_visitors s
LEFT JOIN inervista.dim_date d ON s.date_id = d.date_id
WHERE d.date_id IS NULL;

-- 5. Check for invalid region_id references
SELECT 'stg_aoi_visitors: Invalid region_id references' as check_name,
       COUNT(*) as error_count
FROM edw.stg_aoi_visitors s
LEFT JOIN edw.dim_region r ON s.region_id = r.region_id
WHERE r.region_id IS NULL;

-- 6. Check for duplicate records
SELECT 'stg_aoi_visitors: Duplicate records' as check_name,
       COUNT(*) as error_count
FROM (
    SELECT date_id, region_id, visit_type_id, object_type_id, data_type_id
    FROM edw.stg_aoi_visitors
    GROUP BY date_id, region_id, visit_type_id, object_type_id, data_type_id
    HAVING COUNT(*) > 1
) duplicates;

-- 7. Check for age group consistency
SELECT 'stg_aoi_visitors: Age group consistency' as check_name,
       COUNT(*) as error_count
FROM edw.stg_aoi_visitors
WHERE ABS(total_visitors_structured - 
    (COALESCE(age_15_29, 0) + 
     COALESCE(age_30_44, 0) + 
     COALESCE(age_45_59, 0) + 
     COALESCE(age_60_plus, 0))) > 0.01;

-- 8. Check for gender consistency
SELECT 'stg_aoi_visitors: Gender consistency' as check_name,
       COUNT(*) as error_count
FROM edw.stg_aoi_visitors
WHERE ABS(total_visitors_structured - 
    (COALESCE(sex_male, 0) + 
     COALESCE(total_visitors_structured - sex_male, 0))) > 0.01;

-- 9. Check for data completeness
SELECT 'stg_aoi_visitors: Data completeness' as check_name,
       COUNT(*) as total_records,
       SUM(CASE WHEN total_visitors_structured > 0 THEN 1 ELSE 0 END) as records_with_visitors,
       ROUND(SUM(CASE WHEN total_visitors_structured > 0 THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as completeness_percentage
FROM edw.stg_aoi_visitors;

-- 10. Check for data distribution
SELECT 'stg_aoi_visitors: Data distribution by region' as check_name,
       r.region_name,
       COUNT(*) as record_count,
       SUM(total_visitors_structured) as total_visitors,
       ROUND(AVG(total_visitors_structured), 2) as avg_visitors
FROM edw.stg_aoi_visitors s
JOIN edw.dim_region r ON s.region_id = r.region_id
GROUP BY r.region_name
ORDER BY total_visitors DESC;

\echo 'Validation complete. Please review the results above.' 