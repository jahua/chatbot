-- Check visitors column structure
SELECT aoi_id, jsonb_pretty(visitors) FROM data_lake.aoi_days_raw WHERE visitors IS NOT NULL LIMIT 2;

-- Check dwelltimes column structure
SELECT aoi_id, jsonb_pretty(dwelltimes) FROM data_lake.aoi_days_raw WHERE dwelltimes IS NOT NULL LIMIT 2;

-- Check demographics column structure
SELECT aoi_id, jsonb_pretty(demographics) FROM data_lake.aoi_days_raw WHERE demographics IS NOT NULL LIMIT 2;

-- Check overnights_from_yesterday column structure
SELECT aoi_id, jsonb_pretty(overnights_from_yesterday) FROM data_lake.aoi_days_raw WHERE overnights_from_yesterday IS NOT NULL LIMIT 2;

-- Check top_foreign_countries column structure
SELECT aoi_id, jsonb_pretty(top_foreign_countries) FROM data_lake.aoi_days_raw WHERE top_foreign_countries IS NOT NULL LIMIT 2;

-- Check top_swiss_cantons column structure
SELECT aoi_id, jsonb_pretty(top_swiss_cantons) FROM data_lake.aoi_days_raw WHERE top_swiss_cantons IS NOT NULL LIMIT 2;

-- Check raw_content column structure
SELECT aoi_id, jsonb_pretty(raw_content) FROM data_lake.aoi_days_raw WHERE raw_content IS NOT NULL LIMIT 2;

-- Get all top-level keys from each jsonb column
SELECT 'visitors' AS column_name, jsonb_object_keys(visitors) AS keys
FROM data_lake.aoi_days_raw 
WHERE visitors IS NOT NULL
GROUP BY keys
UNION ALL
SELECT 'dwelltimes' AS column_name, jsonb_object_keys(dwelltimes) AS keys
FROM data_lake.aoi_days_raw 
WHERE dwelltimes IS NOT NULL
GROUP BY keys
UNION ALL
SELECT 'demographics' AS column_name, jsonb_object_keys(demographics) AS keys
FROM data_lake.aoi_days_raw 
WHERE demographics IS NOT NULL
GROUP BY keys
UNION ALL
SELECT 'overnights_from_yesterday' AS column_name, jsonb_object_keys(overnights_from_yesterday) AS keys
FROM data_lake.aoi_days_raw 
WHERE overnights_from_yesterday IS NOT NULL
GROUP BY keys;

-- Extract unique countries from top_foreign_countries array
WITH country_arrays AS (
    SELECT jsonb_array_elements(top_foreign_countries) AS country_element
    FROM data_lake.aoi_days_raw
    WHERE top_foreign_countries IS NOT NULL AND jsonb_typeof(top_foreign_countries) = 'array'
)
SELECT 
    country_element->>'country' AS country_name,
    COUNT(*) AS frequency
FROM country_arrays
GROUP BY country_name
ORDER BY frequency DESC
LIMIT 10;

-- Extract unique cantons from top_swiss_cantons array
WITH canton_arrays AS (
    SELECT jsonb_array_elements(top_swiss_cantons) AS canton_element
    FROM data_lake.aoi_days_raw
    WHERE top_swiss_cantons IS NOT NULL AND jsonb_typeof(top_swiss_cantons) = 'array'
)
SELECT 
    canton_element->>'canton' AS canton_name,
    COUNT(*) AS frequency
FROM canton_arrays
GROUP BY canton_name
ORDER BY frequency DESC
LIMIT 10;

-- Check if all visitor records have the same structure (keys)
SELECT 
    array_agg(DISTINCT key) AS visitor_keys,
    COUNT(*) AS records_with_this_schema
FROM (
    SELECT 
        aoi_id,
        jsonb_object_keys(visitors) AS key
    FROM data_lake.aoi_days_raw
    WHERE visitors IS NOT NULL
) subq
GROUP BY aoi_id
ORDER BY records_with_this_schema DESC
LIMIT 10;

-- Check for null or empty JSON objects in each column
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE visitors IS NULL) AS null_visitors,
    COUNT(*) FILTER (WHERE visitors = '{}'::jsonb) AS empty_visitors,
    COUNT(*) FILTER (WHERE demographics IS NULL) AS null_demographics,
    COUNT(*) FILTER (WHERE demographics = '{}'::jsonb) AS empty_demographics,
    COUNT(*) FILTER (WHERE dwelltimes IS NULL) AS null_dwelltimes,
    COUNT(*) FILTER (WHERE dwelltimes = '{}'::jsonb) AS empty_dwelltimes,
    COUNT(*) FILTER (WHERE overnights_from_yesterday IS NULL) AS null_overnights,
    COUNT(*) FILTER (WHERE overnights_from_yesterday = '{}'::jsonb) AS empty_overnights
FROM data_lake.aoi_days_raw;

-- Count numeric values distribution for a specific JSON field (example)
SELECT 
    visitors->>'total' AS total_visitors,
    COUNT(*) AS frequency
FROM data_lake.aoi_days_raw
WHERE visitors IS NOT NULL AND visitors ? 'total'
GROUP BY total_visitors
ORDER BY (total_visitors::numeric) DESC
LIMIT 15;

-- Find records with the largest JSON in a column
SELECT 
    aoi_id, 
    aoi_date,
    octet_length(visitors::text) AS visitors_size_bytes
FROM data_lake.aoi_days_raw
ORDER BY visitors_size_bytes DESC
LIMIT 5;