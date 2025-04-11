-- Create the choropleth analytics view
CREATE MATERIALIZED VIEW IF NOT EXISTS geo_insights.choropleth_analytics AS
WITH region_boundaries AS (
    -- Get base region information
    SELECT DISTINCT
        geo_name as region_name,
        bounding_box,
        central_latitude,
        central_longitude
    FROM 
        data_lake.master_card
    WHERE 
        geo_name IS NOT NULL
),
tourism_metrics AS (
    -- Calculate tourism metrics
    SELECT 
        geo_name as region_name,
        SUM(CASE WHEN segment = 'Domestic' THEN txn_cnt ELSE 0 END)::FLOAT as swiss_tourists,
        SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END)::FLOAT as foreign_tourists,
        SUM(txn_cnt)::FLOAT as total_visitors,
        CASE 
            WHEN SUM(txn_cnt) > 0 
            THEN (SUM(CASE WHEN segment = 'International' THEN txn_cnt ELSE 0 END)::FLOAT / SUM(txn_cnt)::FLOAT * 100)
            ELSE 0 
        END as foreign_tourist_percentage
    FROM 
        data_lake.master_card
    GROUP BY 
        geo_name
),
spending_metrics AS (
    -- Calculate spending metrics
    SELECT 
        geo_name as region_name,
        SUM(txn_amt)::FLOAT as total_spend,
        AVG(txn_amt)::FLOAT as avg_transaction_value,
        SUM(txn_amt)::FLOAT / NULLIF(COUNT(DISTINCT txn_date), 0) as daily_spend
    FROM 
        data_lake.master_card
    GROUP BY 
        geo_name
),
industry_counts AS (
    -- Get counts per industry
    SELECT 
        geo_name as region_name,
        industry,
        COUNT(*) as industry_count
    FROM 
        data_lake.master_card
    WHERE 
        industry IS NOT NULL
    GROUP BY 
        geo_name, industry
),
industry_metrics AS (
    -- Calculate industry metrics
    SELECT 
        region_name,
        COUNT(DISTINCT industry) as industry_count,
        array_agg(DISTINCT industry) as industries,
        array_agg(industry ORDER BY industry_count DESC) as top_industries
    FROM 
        industry_counts
    GROUP BY 
        region_name
)
SELECT 
    rb.region_name,
    rb.bounding_box,
    rb.central_latitude as latitude,
    rb.central_longitude as longitude,
    tm.swiss_tourists,
    tm.foreign_tourists,
    tm.total_visitors,
    tm.foreign_tourist_percentage,
    sm.total_spend,
    sm.avg_transaction_value,
    sm.daily_spend,
    CASE 
        WHEN tm.total_visitors > 0 THEN sm.total_spend / tm.total_visitors 
        ELSE NULL 
    END as spend_per_visitor,
    im.industry_count,
    im.industries,
    im.top_industries[1] as top_industry
FROM 
    region_boundaries rb
LEFT JOIN 
    tourism_metrics tm ON rb.region_name = tm.region_name
LEFT JOIN 
    spending_metrics sm ON rb.region_name = sm.region_name
LEFT JOIN 
    industry_metrics im ON rb.region_name = im.region_name;

-- Create indices
CREATE INDEX IF NOT EXISTS idx_choropleth_region_name ON geo_insights.choropleth_analytics(region_name);
CREATE INDEX IF NOT EXISTS idx_choropleth_top_industry ON geo_insights.choropleth_analytics(top_industry); 