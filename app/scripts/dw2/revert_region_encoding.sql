-- Revert region names back to their original state
UPDATE dw.dim_region
SET region_name = 
    CASE region_id 
        WHEN 225 THEN 'BÃ¼lach'
        WHEN 227 THEN 'DelÃ©mont'
        WHEN 185 THEN 'GenÃ¨ve'
        WHEN 238 THEN 'GenÃ¨ve City'
        WHEN 188 THEN 'GraubÃ¼nden'
        WHEN 244 THEN 'GÃ¤u'
        WHEN 245 THEN 'GÃ¶sgen'
        WHEN 251 THEN 'HÃ©rens'
        WHEN 252 THEN 'HÃ¶fe'
        WHEN 260 THEN 'KÃ¼ssnacht (SZ)'
        WHEN 265 THEN 'La GlÃ¢ne'
        WHEN 266 THEN 'La GruyÃ¨re'
        WHEN 295 THEN 'MÃ¼nchwilen'
        WHEN 191 THEN 'NeuchÃ¢tel'
        WHEN 296 THEN 'NeuchÃ¢tel City'
        WHEN 304 THEN 'PfÃ¤ffikon'
        WHEN 307 THEN 'PrÃ¤ttigau-Davos'
        WHEN 204 THEN 'ZÃ¼rich'
        WHEN 353 THEN 'ZÃ¼rich City'
        ELSE region_name
    END
WHERE region_id IN (225, 227, 185, 238, 188, 244, 245, 251, 252, 260, 265, 266, 295, 191, 296, 304, 307, 204, 353);

-- Verify changes
SELECT region_id, region_name, region_type
FROM dw.dim_region 
WHERE region_id IN (225, 227, 185, 238, 188, 244, 245, 251, 252, 260, 265, 266, 295, 191, 296, 304, 307, 204, 353)
ORDER BY region_name; 