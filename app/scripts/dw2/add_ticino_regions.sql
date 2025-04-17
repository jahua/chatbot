-- First, insert Canton Ticino if not exists
INSERT INTO dw.dim_region (region_name, region_type, parent_region_id)
SELECT 'Ticino', 'canton', NULL
WHERE NOT EXISTS (
    SELECT 1 FROM dw.dim_region WHERE region_name = 'Ticino' AND region_type = 'canton'
)
RETURNING region_id;

-- Store Ticino's region_id for reference
DO $$
DECLARE
    v_ticino_id integer;
BEGIN
    SELECT region_id INTO v_ticino_id
    FROM dw.dim_region
    WHERE region_name = 'Ticino' AND region_type = 'canton';

    -- Insert tourism regions with their standardized names
    INSERT INTO dw.dim_region (region_name, region_type, parent_region_id, source_system, source_system_id)
    VALUES 
    ('Bellinzonese', 'tourism_region', v_ticino_id, 'intervista', 'Bellinzona e Alto Ticino'),
    ('Ascona-Locarno', 'tourism_region', v_ticino_id, 'intervista', 'Lago Maggiore e Valli'),
    ('Luganese', 'tourism_region', v_ticino_id, 'intervista', 'Lago di Lugano'),
    ('Mendrisiotto', 'tourism_region', v_ticino_id, 'intervista', 'Mendrisiotto')
    ON CONFLICT (region_name, region_type) DO UPDATE
    SET 
        parent_region_id = EXCLUDED.parent_region_id,
        source_system = EXCLUDED.source_system,
        source_system_id = EXCLUDED.source_system_id;
END $$; 