-- Check age group consistency in detail
SELECT 
    d.date_id,
    d.month,
    d.year,
    r.region_id,
    r.region_name,
    a.total,
    a.age_15_29,
    a.age_30_44,
    (a.age_15_29 + a.age_30_44) as sum_age_groups,
    a.total - (a.age_15_29 + a.age_30_44) as difference
FROM 
    inervista.fact_tourism a
JOIN 
    inervista.dim_date d ON a.date_id = d.date_id
JOIN 
    inervista.dim_region r ON a.region_id = r.region_id
WHERE 
    a.total != (a.age_15_29 + a.age_30_44)
    AND a.total IS NOT NULL 
    AND a.age_15_29 IS NOT NULL 
    AND a.age_30_44 IS NOT NULL
ORDER BY 
    d.year, d.month_number; 