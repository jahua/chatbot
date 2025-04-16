-- Update Geo Dimension Script
-- This script updates the dim_geography table to ensure proper mapping
-- of Msa geo_type to cities

-- First check the current state
SELECT 
    geo_type, 
    COUNT(*) as record_count,
    COUNT(CASE WHEN city IS NOT NULL THEN 1 END) as city_populated,
    COUNT(CASE WHEN state IS NOT NULL THEN 1 END) as state_populated,
    COUNT(CASE WHEN country IS NOT NULL THEN 1 END) as country_populated
FROM dw.dim_geography
GROUP BY geo_type
ORDER BY geo_type;

-- Update the city field for Msa geo_type
DO $$
DECLARE
    v_count INTEGER := 0;
BEGIN
    -- Update city field for Msa records
    UPDATE dw.dim_geography
    SET 
        city = geo_name,
        updated_at = CURRENT_TIMESTAMP
    WHERE 
        geo_type = 'Msa'
        AND (city IS NULL OR city <> geo_name);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Updated % Msa records to set city field', v_count;
    
    -- Update state field for State records
    UPDATE dw.dim_geography
    SET 
        state = geo_name,
        updated_at = CURRENT_TIMESTAMP
    WHERE 
        geo_type = 'State'
        AND (state IS NULL OR state <> geo_name);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Updated % State records to set state field', v_count;
    
    -- Update country field for Country records
    UPDATE dw.dim_geography
    SET 
        country = geo_name,
        updated_at = CURRENT_TIMESTAMP
    WHERE 
        geo_type = 'Country'
        AND (country IS NULL OR country <> geo_name);
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Updated % Country records to set country field', v_count;
    
    -- Set default country for Switzerland locations
    UPDATE dw.dim_geography
    SET 
        country = 'Switzerland',
        updated_at = CURRENT_TIMESTAMP
    WHERE 
        geo_type IN ('Msa', 'State')
        AND (country IS NULL OR country = '');
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'Updated % records to set default country to Switzerland', v_count;
END $$;

-- Check the updated state
SELECT 
    geo_type, 
    COUNT(*) as record_count,
    COUNT(CASE WHEN city IS NOT NULL THEN 1 END) as city_populated,
    COUNT(CASE WHEN state IS NOT NULL THEN 1 END) as state_populated,
    COUNT(CASE WHEN country IS NOT NULL THEN 1 END) as country_populated
FROM dw.dim_geography
GROUP BY geo_type
ORDER BY geo_type;

-- Sample of the updated data
SELECT 
    geography_id,
    geo_name,
    geo_type,
    country,
    state,
    city,
    created_at,
    updated_at
FROM dw.dim_geography
ORDER BY geo_type, geo_name
LIMIT 20; 