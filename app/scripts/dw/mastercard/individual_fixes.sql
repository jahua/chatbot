-- Individual updates for each region
-- Each region is updated separately with commits

-- Region 400: Bülach
UPDATE dw.dim_region SET region_name = 'Bülach' WHERE region_id = 400;
COMMIT;

-- Region 402: Delémont
UPDATE dw.dim_region SET region_name = 'Delémont' WHERE region_id = 402;
COMMIT;

-- Handle Genève (region_id 413, 414)
UPDATE dw.dim_region SET region_type = 'canton_temp' WHERE region_id = 414;
COMMIT;

UPDATE dw.dim_region SET region_name = 'Genève' WHERE region_id = 413;
COMMIT;

UPDATE dw.dim_region SET region_name = 'Genève Region' WHERE region_id = 414;
COMMIT;

UPDATE dw.dim_region SET region_type = 'tourism_region' WHERE region_id = 414;
COMMIT;

-- Region 417: Graubünden
UPDATE dw.dim_region SET region_name = 'Graubünden' WHERE region_id = 417;
COMMIT;

-- Region 419: Gäu
UPDATE dw.dim_region SET region_name = 'Gäu' WHERE region_id = 419;
COMMIT;

-- Region 420: Gösgen
UPDATE dw.dim_region SET region_name = 'Gösgen' WHERE region_id = 420;
COMMIT;

-- Region 426: Hérens
UPDATE dw.dim_region SET region_name = 'Hérens' WHERE region_id = 426;
COMMIT;

-- Region 427: Höfe
UPDATE dw.dim_region SET region_name = 'Höfe' WHERE region_id = 427;
COMMIT;

-- Region 436: Küssnacht
UPDATE dw.dim_region SET region_name = 'Küssnacht (SZ)' WHERE region_id = 436;
COMMIT;

-- Region 439: La Glâne
UPDATE dw.dim_region SET region_name = 'La Glâne' WHERE region_id = 439;
COMMIT;

-- Region 440: La Gruyère
UPDATE dw.dim_region SET region_name = 'La Gruyère' WHERE region_id = 440;
COMMIT;

-- Region 463: Münchwilen
UPDATE dw.dim_region SET region_name = 'Münchwilen' WHERE region_id = 463;
COMMIT;

-- Handle Neuchâtel (region_id 464, 465)
UPDATE dw.dim_region SET region_type = 'canton_temp' WHERE region_id = 465;
COMMIT;

UPDATE dw.dim_region SET region_name = 'Neuchâtel' WHERE region_id = 464;
COMMIT;

UPDATE dw.dim_region SET region_name = 'Neuchâtel Canton' WHERE region_id = 465;
COMMIT;

UPDATE dw.dim_region SET region_type = 'canton' WHERE region_id = 465;
COMMIT;

-- Region 471: Pfäffikon
UPDATE dw.dim_region SET region_name = 'Pfäffikon' WHERE region_id = 471;
COMMIT;

-- Region 474: Prättigau-Davos
UPDATE dw.dim_region SET region_name = 'Prättigau-Davos' WHERE region_id = 474;
COMMIT;

-- Handle Zürich regions (550-553)
UPDATE dw.dim_region SET region_type = 'canton_temp' WHERE region_id = 553;
COMMIT;

UPDATE dw.dim_region SET region_type = 'district_temp' WHERE region_id IN (551, 552);
COMMIT;

UPDATE dw.dim_region SET region_name = 'Zürich' WHERE region_id = 550;
COMMIT;

UPDATE dw.dim_region SET region_name = 'Zürich Stadt' WHERE region_id = 551;
COMMIT;

UPDATE dw.dim_region SET region_name = 'Zürich Land' WHERE region_id = 552;
COMMIT;

UPDATE dw.dim_region SET region_name = 'Zürich Kanton' WHERE region_id = 553;
COMMIT;

UPDATE dw.dim_region SET region_type = 'district' WHERE region_id = 551;
COMMIT;

UPDATE dw.dim_region SET region_type = 'district' WHERE region_id = 552;
COMMIT;

UPDATE dw.dim_region SET region_type = 'canton' WHERE region_id = 553;
COMMIT;

-- Check for remaining encoding issues
SELECT COUNT(*) as remaining_issues
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%'; 