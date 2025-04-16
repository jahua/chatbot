-- Direct region ID encoding fix script
-- Uses direct updates by region_id which is more reliable than pattern matching

BEGIN;

-- Show problematic regions before fixing
SELECT region_id, region_name, region_type
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%'
ORDER BY region_id;

-- Direct updates by region_id - no pattern matching needed
-- Region 400: Bülach
UPDATE dw.dim_region SET region_name = 'Bülach' WHERE region_id = 400;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 400;

-- Region 402: Delémont
UPDATE dw.dim_region SET region_name = 'Delémont' WHERE region_id = 402;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 402;

-- Regions 413-414: Genève (need to handle unique constraint)
-- First update region_type to avoid constraint violation
UPDATE dw.dim_region SET region_type = 'canton_temp' WHERE region_id = 414;

-- Now update names
UPDATE dw.dim_region SET region_name = 'Genève' WHERE region_id = 413;
SELECT region_id, region_name, region_type FROM dw.dim_region WHERE region_id = 413;

UPDATE dw.dim_region SET region_name = 'Genève Canton' WHERE region_id = 414;
UPDATE dw.dim_region SET region_type = 'canton' WHERE region_id = 414;
SELECT region_id, region_name, region_type FROM dw.dim_region WHERE region_id = 414;

-- Region 417: Graubünden
UPDATE dw.dim_region SET region_name = 'Graubünden' WHERE region_id = 417;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 417;

-- Region 419: Gäu
UPDATE dw.dim_region SET region_name = 'Gäu' WHERE region_id = 419;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 419;

-- Region 420: Gösgen
UPDATE dw.dim_region SET region_name = 'Gösgen' WHERE region_id = 420;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 420;

-- Region 426: Hérens
UPDATE dw.dim_region SET region_name = 'Hérens' WHERE region_id = 426;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 426;

-- Region 427: Höfe
UPDATE dw.dim_region SET region_name = 'Höfe' WHERE region_id = 427;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 427;

-- Region 436: Küssnacht
UPDATE dw.dim_region SET region_name = 'Küssnacht (SZ)' WHERE region_id = 436;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 436;

-- Region 439: La Glâne
UPDATE dw.dim_region SET region_name = 'La Glâne' WHERE region_id = 439;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 439;

-- Region 440: La Gruyère
UPDATE dw.dim_region SET region_name = 'La Gruyère' WHERE region_id = 440;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 440;

-- Region 463: Münchwilen
UPDATE dw.dim_region SET region_name = 'Münchwilen' WHERE region_id = 463;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 463;

-- Region 464-465: Neuchâtel (handle unique constraint)
UPDATE dw.dim_region SET region_type = 'canton_temp' WHERE region_id = 465;
UPDATE dw.dim_region SET region_name = 'Neuchâtel' WHERE region_id = 464;
UPDATE dw.dim_region SET region_name = 'Neuchâtel Canton' WHERE region_id = 465;
UPDATE dw.dim_region SET region_type = 'canton' WHERE region_id = 465;
SELECT region_id, region_name, region_type FROM dw.dim_region WHERE region_id IN (464, 465);

-- Region 471: Pfäffikon
UPDATE dw.dim_region SET region_name = 'Pfäffikon' WHERE region_id = 471;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 471;

-- Region 474: Prättigau-Davos
UPDATE dw.dim_region SET region_name = 'Prättigau-Davos' WHERE region_id = 474;
SELECT region_id, region_name FROM dw.dim_region WHERE region_id = 474;

-- Regions 550-553: Zürich (handle unique constraint)
UPDATE dw.dim_region SET region_type = 'canton_temp' WHERE region_id = 553;
UPDATE dw.dim_region SET region_type = 'district_temp' WHERE region_id IN (551, 552);

UPDATE dw.dim_region SET region_name = 'Zürich' WHERE region_id = 550;
UPDATE dw.dim_region SET region_name = 'Zürich Stadt' WHERE region_id = 551;
UPDATE dw.dim_region SET region_name = 'Zürich Land' WHERE region_id = 552;
UPDATE dw.dim_region SET region_name = 'Zürich Kanton' WHERE region_id = 553;

-- Restore region types
UPDATE dw.dim_region SET region_type = 'district' WHERE region_id = 551;
UPDATE dw.dim_region SET region_type = 'district' WHERE region_id = 552;
UPDATE dw.dim_region SET region_type = 'canton' WHERE region_id = 553;

SELECT region_id, region_name, region_type FROM dw.dim_region WHERE region_id IN (550, 551, 552, 553);

-- Verify no encoding issues remain
SELECT COUNT(*) as regions_with_encoding_issues_remaining 
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%';

-- Show all fixed regions
SELECT region_id, region_name, region_type
FROM dw.dim_region
WHERE region_id IN (400, 402, 413, 414, 417, 419, 420, 426, 427, 436, 439, 440, 463, 464, 465, 471, 474, 550, 551, 552, 553)
ORDER BY region_id;

COMMIT;