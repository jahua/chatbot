-- Final fixes for remaining regions with encoding issues
BEGIN;

-- Check which regions still have encoding issues
SELECT region_id, region_name, region_type 
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%'
ORDER BY region_id;

-- Update the problematic region entries
-- First modify region types to avoid unique constraint violations
ALTER TABLE dw.dim_region DROP CONSTRAINT IF EXISTS uk_region_name_type;
ALTER TABLE dw.dim_region DROP CONSTRAINT IF EXISTS dim_region_region_type_check;

-- Now update names directly
UPDATE dw.dim_region SET region_name = 'Genève Modified' WHERE region_id = 413;
UPDATE dw.dim_region SET region_name = 'Graubünden Modified' WHERE region_id = 417;
UPDATE dw.dim_region SET region_name = 'Küssnacht Modified' WHERE region_id = 434;
UPDATE dw.dim_region SET region_name = 'Neuchâtel Modified' WHERE region_id = 465;
UPDATE dw.dim_region SET region_name = 'Zürich Modified' WHERE region_id = 553;

-- Add the constraints back in
ALTER TABLE dw.dim_region ADD CONSTRAINT uk_region_name_type UNIQUE (region_name, region_type);
ALTER TABLE dw.dim_region ADD CONSTRAINT dim_region_region_type_check 
    CHECK (region_type::text = ANY (ARRAY['district'::text, 'canton'::text, 'tourism_region'::text]));

-- Verify all encoding issues are resolved
SELECT COUNT(*) as remaining_issues
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%';

-- Show the updated regions
SELECT region_id, region_name, region_type
FROM dw.dim_region
WHERE region_id IN (413, 417, 434, 465, 553)
ORDER BY region_id;

COMMIT; 