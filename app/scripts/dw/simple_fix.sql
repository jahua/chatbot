-- Simple direct region fixes by ID
BEGIN;

-- Show problematic regions before
SELECT region_id, region_name, region_type
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%'
ORDER BY region_id;

-- Fix one region to check if it works
UPDATE dw.dim_region SET region_name = 'TEST' WHERE region_id = 400;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 400;

-- If successful, proceed with all updates
UPDATE dw.dim_region SET region_name = 'Bülach' WHERE region_id = 400;
UPDATE dw.dim_region SET region_name = 'Delémont' WHERE region_id = 402;

-- Handle unique constraint for Geneva - first make them different temporarily
UPDATE dw.dim_region SET region_type = 'canton_temp' WHERE region_id = 414;
UPDATE dw.dim_region SET region_name = 'Genève' WHERE region_id = 413;
UPDATE dw.dim_region SET region_name = 'Genève Canton' WHERE region_id = 414;
UPDATE dw.dim_region SET region_type = 'canton' WHERE region_id = 414;

-- Continue with the rest
UPDATE dw.dim_region SET region_name = 'Graubünden' WHERE region_id = 417;
UPDATE dw.dim_region SET region_name = 'Gäu' WHERE region_id = 419;
UPDATE dw.dim_region SET region_name = 'Gösgen' WHERE region_id = 420;
UPDATE dw.dim_region SET region_name = 'Hérens' WHERE region_id = 426;
UPDATE dw.dim_region SET region_name = 'Höfe' WHERE region_id = 427;
UPDATE dw.dim_region SET region_name = 'Küssnacht (SZ)' WHERE region_id = 436;
UPDATE dw.dim_region SET region_name = 'La Glâne' WHERE region_id = 439;
UPDATE dw.dim_region SET region_name = 'La Gruyère' WHERE region_id = 440;
UPDATE dw.dim_region SET region_name = 'Münchwilen' WHERE region_id = 463;

-- Handle unique constraint for Neuchâtel
UPDATE dw.dim_region SET region_type = 'canton_temp' WHERE region_id = 465;
UPDATE dw.dim_region SET region_name = 'Neuchâtel' WHERE region_id = 464;
UPDATE dw.dim_region SET region_name = 'Neuchâtel Canton' WHERE region_id = 465;
UPDATE dw.dim_region SET region_type = 'canton' WHERE region_id = 465;

-- Continue with the rest
UPDATE dw.dim_region SET region_name = 'Pfäffikon' WHERE region_id = 471;
UPDATE dw.dim_region SET region_name = 'Prättigau-Davos' WHERE region_id = 474;

-- Handle Zürich - careful with constraints
UPDATE dw.dim_region SET region_type = 'canton_temp' WHERE region_id = 553;
UPDATE dw.dim_region SET region_type = 'district_temp' WHERE region_id IN (551, 552);
UPDATE dw.dim_region SET region_name = 'Zürich' WHERE region_id = 550;
UPDATE dw.dim_region SET region_name = 'Zürich Stadt' WHERE region_id = 551;
UPDATE dw.dim_region SET region_name = 'Zürich Land' WHERE region_id = 552;
UPDATE dw.dim_region SET region_name = 'Zürich Kanton' WHERE region_id = 553;
UPDATE dw.dim_region SET region_type = 'district' WHERE region_id = 551;
UPDATE dw.dim_region SET region_type = 'district' WHERE region_id = 552;
UPDATE dw.dim_region SET region_type = 'canton' WHERE region_id = 553;

-- Check if any encoding issues remain
SELECT COUNT(*) as remaining_encoding_issues 
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%';

-- Show sample of fixed regions
SELECT region_id, region_name, region_type
FROM dw.dim_region
WHERE region_id IN (400, 402, 413, 414, 417)
ORDER BY region_id;

COMMIT; 