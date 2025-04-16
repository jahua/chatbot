-- Improved Encoding fix script for region names
-- This script finds and fixes ALL encoding issues in region names including the complex cases

BEGIN;

-- Create a temporary table to store name fixes  
DROP TABLE IF EXISTS temp_geo_name_fixes;
CREATE TEMP TABLE temp_geo_name_fixes (
    region_id INTEGER,
    original_name TEXT,
    fixed_name TEXT
);

-- First, gather all problematic region names with their IDs 
-- The exact CASE matches are used for the problematic regions with unicode escape sequences
INSERT INTO temp_geo_name_fixes (region_id, original_name, fixed_name)
SELECT 
    region_id,
    region_name,
    CASE
        -- Original patterns
        WHEN region_name ILIKE '%GenÃ%ve%' AND region_name NOT LIKE '%\u0083%' THEN 'Genève'
        WHEN region_name ILIKE '%ZÃ%rich%' AND region_name NOT LIKE '%\u0083%' THEN 'Zürich'
        WHEN region_name ILIKE '%BÃ%lach%' AND region_name NOT LIKE '%\u0083%' THEN 'Bülach'
        WHEN region_name ILIKE '%DelÃ%mont%' AND region_name NOT LIKE '%\u0083%' THEN 'Delémont'
        WHEN region_name ILIKE '%GraubÃ%nden%' AND region_name NOT LIKE '%\u0083%' THEN 'Graubünden'
        WHEN region_name ILIKE '%GÃ%u%' AND region_name NOT ILIKE '%GÃ%sgen%' AND region_name NOT LIKE '%\u0083%' THEN 'Gäu'
        WHEN region_name ILIKE '%GÃ%sgen%' AND region_name NOT LIKE '%\u0083%' THEN 'Gösgen'
        WHEN region_name ILIKE '%HÃ%rens%' AND region_name NOT LIKE '%\u0083%' THEN 'Hérens'
        WHEN region_name ILIKE '%HÃ%fe%' AND region_name NOT LIKE '%\u0083%' THEN 'Höfe'
        WHEN region_name ILIKE '%KÃ%ssnacht%' AND region_name NOT LIKE '%\u0083%' THEN 'Küssnacht (SZ)'
        WHEN region_name ILIKE '%BÃ%le-Campagne%' AND region_name NOT LIKE '%\u0083%' THEN 'Bâle-Campagne'
        WHEN region_name ILIKE '%BÃ%le-Ville%' AND region_name NOT LIKE '%\u0083%' THEN 'Bâle-Ville'
        WHEN region_name ILIKE '%FribourgerÃ%gion%' AND region_name NOT LIKE '%\u0083%' THEN 'Fribourgérégion'
        WHEN region_name ILIKE '%ThÃ%ne%' AND region_name NOT LIKE '%\u0083%' THEN 'Thône'
        WHEN region_name ILIKE '%VallÃ%e de Joux%' AND region_name NOT LIKE '%\u0083%' THEN 'Vallée de Joux'
        
        -- Specific fixes for the exact problematic region names
        WHEN region_id = 400 THEN 'Bülach'
        WHEN region_id = 402 THEN 'Delémont'
        WHEN region_id = 413 THEN 'Genève'
        WHEN region_id = 414 THEN 'Genève'
        WHEN region_id = 417 THEN 'Graubünden'
        WHEN region_id = 419 THEN 'Gäu'
        WHEN region_id = 420 THEN 'Gösgen'
        WHEN region_id = 426 THEN 'Hérens'
        WHEN region_id = 427 THEN 'Höfe'
        WHEN region_id = 434 THEN 'Küssnacht (SZ)'
        WHEN region_id = 439 THEN 'La Glâne'
        WHEN region_id = 440 THEN 'La Gruyère'
        WHEN region_id = 463 THEN 'Münchwilen'
        WHEN region_id = 464 THEN 'Neuchâtel'
        WHEN region_id = 465 THEN 'Neuchâtel'
        WHEN region_id = 471 THEN 'Pfäffikon'
        WHEN region_id = 474 THEN 'Prättigau-Davos'
        WHEN region_id = 513 THEN 'Zürich'
        WHEN region_id = 514 THEN 'Zürich'
    END
FROM dw.dim_region
WHERE region_name LIKE '%Ã%' OR region_name LIKE '%Â%' OR region_name LIKE '%\u0083%';

-- Show what we're about to fix
SELECT region_id, original_name, fixed_name FROM temp_geo_name_fixes ORDER BY region_id;

-- Update all problematic region names at once
UPDATE dw.dim_region dr
SET region_name = f.fixed_name
FROM temp_geo_name_fixes f
WHERE dr.region_id = f.region_id;

-- Show remaining issues
SELECT COUNT(*) as regions_with_encoding_issues_remaining 
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%';

-- Show what was fixed
SELECT region_id, region_name 
FROM dw.dim_region 
WHERE region_id IN (SELECT region_id FROM temp_geo_name_fixes)
ORDER BY region_id;

COMMIT;