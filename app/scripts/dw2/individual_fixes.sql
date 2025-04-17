-- Individual updates for each region
-- Each region is updated separately with commits

-- Fix individual region names while preserving IDs

-- Handle Genève duplicates
DO $$
BEGIN
    -- Delete the incorrectly encoded duplicate
    DELETE FROM dw.dim_region WHERE region_id = 185;
    
    -- Ensure region 186 has correct name
    UPDATE dw.dim_region 
    SET region_name = 'Genève',
        region_type = 'canton'
    WHERE region_id = 186;
    
    -- Update region 238 to tourism_region
    UPDATE dw.dim_region 
    SET region_name = 'Genève Region',
        region_type = 'tourism_region'
    WHERE region_id = 238;
    
    -- Update region 239 to city
    UPDATE dw.dim_region 
    SET region_name = 'Genève City',
        region_type = 'city'
    WHERE region_id = 239;
END $$;

-- Update Bülach
UPDATE dw.dim_region 
SET region_name = 'Bülach',
    region_type = 'district'
WHERE region_id = 225;

-- Update Delémont
UPDATE dw.dim_region 
SET region_name = 'Delémont',
    region_type = 'district'
WHERE region_id = 227;

-- Update La Chaux-de-Fonds
UPDATE dw.dim_region 
SET region_name = 'La Chaux-de-Fonds',
    region_type = 'district'
WHERE region_id = 240;

-- Update Lausanne
UPDATE dw.dim_region 
SET region_name = 'Lausanne',
    region_type = 'district'
WHERE region_id = 242;

-- Update Le Locle
UPDATE dw.dim_region 
SET region_name = 'Le Locle',
    region_type = 'district'
WHERE region_id = 243;

-- Update Montreux
UPDATE dw.dim_region 
SET region_name = 'Montreux',
    region_type = 'district'
WHERE region_id = 245;

-- Update Morges
UPDATE dw.dim_region 
SET region_name = 'Morges',
    region_type = 'district'
WHERE region_id = 246;

-- Handle Neuchâtel carefully
DO $$
BEGIN
    -- First check if Neuchâtel canton exists
    IF NOT EXISTS (
        SELECT 1 FROM dw.dim_region 
        WHERE region_name = 'Neuchâtel' 
        AND region_type = 'canton'
        AND region_id != 296
    ) THEN
        -- Update only if it doesn't exist
        UPDATE dw.dim_region 
        SET region_name = 'Neuchâtel',
            region_type = 'canton'
        WHERE region_id = 296;
    END IF;
END $$;

-- Update Nyon
UPDATE dw.dim_region 
SET region_name = 'Nyon',
    region_type = 'district'
WHERE region_id = 297;

-- Update Payerne
UPDATE dw.dim_region 
SET region_name = 'Payerne',
    region_type = 'district'
WHERE region_id = 298;

-- Update Porrentruy
UPDATE dw.dim_region 
SET region_name = 'Porrentruy',
    region_type = 'district'
WHERE region_id = 299;

-- Update Vevey
UPDATE dw.dim_region 
SET region_name = 'Vevey',
    region_type = 'district'
WHERE region_id = 300;

-- Update Yverdon-les-Bains
UPDATE dw.dim_region 
SET region_name = 'Yverdon-les-Bains',
    region_type = 'district'
WHERE region_id = 301;

-- Handle Zürich carefully
DO $$
BEGIN
    -- First check if Zürich canton exists
    IF NOT EXISTS (
        SELECT 1 FROM dw.dim_region 
        WHERE region_name = 'Zürich' 
        AND region_type = 'canton'
        AND region_id != 353
    ) THEN
        -- Update only if it doesn't exist
        UPDATE dw.dim_region 
        SET region_name = 'Zürich',
            region_type = 'canton'
        WHERE region_id = 353;
    END IF;
END $$;

-- Update Zürich City
UPDATE dw.dim_region 
SET region_name = 'Zürich City',
    region_type = 'district'
WHERE region_id = 354;

-- Verify the changes and show problematic encodings
SELECT region_id, region_name, region_type 
FROM dw.dim_region
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Ã©%'
   OR region_name LIKE '%Ã¢%';

COMMIT;

-- Region 188: Graubünden
UPDATE dw.dim_region SET region_name = 'Graubünden' WHERE region_id = 188;
COMMIT;

-- Region 244: Gäu
UPDATE dw.dim_region SET region_name = 'Gäu' WHERE region_id = 244;
COMMIT;

-- Region 251: Hérens
UPDATE dw.dim_region SET region_name = 'Hérens' WHERE region_id = 251;
COMMIT;

-- Region 252: Höfe
UPDATE dw.dim_region SET region_name = 'Höfe' WHERE region_id = 252;
COMMIT;

-- Region 260: Küssnacht
UPDATE dw.dim_region SET region_name = 'Küssnacht (SZ)' WHERE region_id = 260;
COMMIT;

-- Region 265: La Glâne
UPDATE dw.dim_region SET region_name = 'La Glâne' WHERE region_id = 265;
COMMIT;

-- Region 266: La Gruyère
UPDATE dw.dim_region SET region_name = 'La Gruyère' WHERE region_id = 266;
COMMIT;

-- Region 295: Münchwilen
UPDATE dw.dim_region SET region_name = 'Münchwilen' WHERE region_id = 295;
COMMIT;

-- Region 304: Pfäffikon
UPDATE dw.dim_region SET region_name = 'Pfäffikon' WHERE region_id = 304;
COMMIT;

-- Region 307: Prättigau-Davos
UPDATE dw.dim_region SET region_name = 'Prättigau-Davos' WHERE region_id = 307;
COMMIT;

-- Check for remaining encoding issues
SELECT COUNT(*) as remaining_issues
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%'; 