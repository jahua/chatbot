SELECT column_name FROM information_schema.columns WHERE table_schema = 'data_lake' AND table_name = 'aoi_days_raw' AND data_type IN ('json', 'jsonb');
-- Check nested JSON keys in each column
SELECT jsonb_object_keys(visitors) AS visitors_keys FROM data_lake.aoi_days_raw WHERE visitors IS NOT NULL LIMIT 1;
SELECT jsonb_object_keys(demographics) AS demographics_keys FROM data_lake.aoi_days_raw WHERE demographics IS NOT NULL LIMIT 1;
