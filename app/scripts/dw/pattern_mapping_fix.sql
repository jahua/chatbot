-- Pattern-based encoding fix script
-- Maps corrupted text patterns to correct names without relying on region_id

BEGIN;

-- Create a mapping table for corrupted patterns to correct names
DROP TABLE IF EXISTS text_pattern_mapping;
CREATE TEMP TABLE text_pattern_mapping (
    corrupted_pattern TEXT PRIMARY KEY,
    correct_name TEXT NOT NULL
);

-- Insert all pattern mappings
INSERT INTO text_pattern_mapping (corrupted_pattern, correct_name) VALUES
    ('BÃÂ¼lach', 'Bülach'),
    ('DelÃÂ©mont', 'Delémont'),
    ('GenÃÂ¨ve', 'Genève'),
    ('GraubÃÂ¼nden', 'Graubünden'),
    ('GÃÂ¤u', 'Gäu'),
    ('GÃÂ¶sgen', 'Gösgen'),
    ('HÃÂ©rens', 'Hérens'),
    ('HÃÂ¶fe', 'Höfe'),
    ('KÃÂ¼ssnacht (SZ)', 'Küssnacht (SZ)'),
    ('La GlÃÂ¢ne', 'La Glâne'),
    ('La GruyÃÂ¨re', 'La Gruyère'),
    ('MÃÂ¼nchwilen', 'Münchwilen'),
    ('NeuchÃÂ¢tel', 'Neuchâtel'),
    ('PfÃÂ¤ffikon', 'Pfäffikon'),
    ('PrÃÂ¤ttigau-Davos', 'Prättigau-Davos'),
    ('ZÃÂ¼rich', 'Zürich');

-- Add any pattern with character codes if needed
INSERT INTO text_pattern_mapping (corrupted_pattern, correct_name) VALUES
    ('BÃ\\u0083Â¼lach', 'Bülach'),
    ('DelÃ\\u0083Â©mont', 'Delémont'),
    ('GenÃ\\u0083Â¨ve', 'Genève'),
    ('GraubÃ\\u0083Â¼nden', 'Graubünden'),
    ('GÃ\\u0083Â¤u', 'Gäu'),
    ('GÃ\\u0083Â¶sgen', 'Gösgen'),
    ('HÃ\\u0083Â©rens', 'Hérens'),
    ('HÃ\\u0083Â¶fe', 'Höfe'),
    ('KÃ\\u0083Â¼ssnacht (SZ)', 'Küssnacht (SZ)'),
    ('La GlÃ\\u0083Â¢ne', 'La Glâne'),
    ('La GruyÃ\\u0083Â¨re', 'La Gruyère'),
    ('MÃ\\u0083Â¼nchwilen', 'Münchwilen'),
    ('NeuchÃ\\u0083Â¢tel', 'Neuchâtel'),
    ('PfÃ\\u0083Â¤ffikon', 'Pfäffikon'),
    ('PrÃ\\u0083Â¤ttigau-Davos', 'Prättigau-Davos'),
    ('ZÃ\\u0083Â¼rich', 'Zürich')
ON CONFLICT (corrupted_pattern) DO NOTHING;

-- Show the mapping table
SELECT corrupted_pattern, correct_name FROM text_pattern_mapping ORDER BY correct_name;

-- Show which records will be affected
SELECT r.region_id, r.region_name AS "current_name", m.correct_name AS "will_be_updated_to"
FROM dw.dim_region r
JOIN text_pattern_mapping m ON r.region_name = m.corrupted_pattern
ORDER BY r.region_id;

-- Update using the mapping table
UPDATE dw.dim_region r
SET region_name = m.correct_name
FROM text_pattern_mapping m
WHERE r.region_name = m.corrupted_pattern;

-- Verify no encoding issues remain
SELECT COUNT(*) as regions_with_encoding_issues_remaining 
FROM dw.dim_region 
WHERE region_name LIKE '%Ã%' 
   OR region_name LIKE '%Â%';

-- Show fixed records
SELECT region_id, region_name, region_type
FROM dw.dim_region
WHERE region_name IN (SELECT correct_name FROM text_pattern_mapping)
ORDER BY region_id;

-- Drop the temporary mapping table
DROP TABLE text_pattern_mapping;

COMMIT;