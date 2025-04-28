-- Update fact_visitor table with sample data for Swiss and international tourists
-- Distribute total_visitors as 60% Swiss tourists and 40% international tourists

UPDATE dw.fact_visitor
SET 
    swiss_tourists = total_visitors * 0.6,
    foreign_tourists = total_visitors * 0.4
WHERE 
    swiss_tourists = 0 AND foreign_tourists = 0;

-- Verify the update
SELECT 
    date_id,
    region_id,
    total_visitors,
    swiss_tourists,
    foreign_tourists
FROM 
    dw.fact_visitor
ORDER BY 
    date_id, region_id
LIMIT 20; 