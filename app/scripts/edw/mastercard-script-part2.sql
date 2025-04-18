-- MasterCard Data Integration Script - PART 2: Analytics and Reporting
-- This script creates views, indexes, and functions for analytics
-- Created: April 14, 2025

-- Step 10: Update the unified fact table if it exists
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables 
               WHERE table_schema = 'edw' AND table_name = 'fact_tourism_unified') THEN
        
        -- Update or insert into unified fact table
        EXECUTE '
        INSERT INTO edw.fact_tourism_unified (
            date_id,
            region_id,
            visit_type_id,
            data_type_id,
            industry_count,
            industry_metrics,
            spending_data_completion_pct,
            data_quality_score,
            data_completeness,
            visitor_data_completion_pct,
            metadata,
            created_at,
            updated_at
        )
        WITH daily_industry_metrics AS (
            SELECT 
                ftsd.date_id,
                ftsd.region_id,
                ftsd.visit_type_id,
                ftsd.data_type_id,
                COUNT(DISTINCT ftsd.industry_id) AS industry_count,
                jsonb_object_agg(
                    i.industry_name,
                    jsonb_build_object(
                        ''txn_amt_index'', ftsd.txn_amt_index,
                        ''txn_cnt_index'', ftsd.txn_cnt_index,
                        ''acct_cnt_index'', ftsd.acct_cnt_index,
                        ''avg_ticket_index'', ftsd.avg_ticket_index,
                        ''yoy_change_pct'', ftsd.yoy_txn_amt_pct
                    )
                ) AS industry_metrics
            FROM
                edw.fact_tourism_spending_daily ftsd
            JOIN
                edw.dim_industry i ON ftsd.industry_id = i.industry_id
            GROUP BY
                ftsd.date_id,
                ftsd.region_id,
                ftsd.visit_type_id,
                ftsd.data_type_id
        )
        SELECT
            dim.date_id,
            dim.region_id,
            dim.visit_type_id,
            dim.data_type_id,
            dim.industry_count,
            dim.industry_metrics,
            100 AS spending_data_completion_pct,
            CASE
                WHEN fu.visitor_data_completion_pct > 0 THEN 100
                ELSE 50
            END AS data_quality_score,
            CASE
                WHEN fu.visitor_data_completion_pct > 0 THEN ''Complete''
                ELSE ''Partial''
            END AS data_completeness,
            COALESCE(fu.visitor_data_completion_pct, 0) AS visitor_data_completion_pct,
            jsonb_build_object(
                ''last_updated'', NOW(),
                ''data_sources'', CASE 
                    WHEN fu.metadata->''data_sources'' IS NULL THEN jsonb_build_array(''mastercard'')
                    WHEN NOT (fu.metadata->''data_sources'' @> ''"mastercard"''::jsonb) THEN 
                        (fu.metadata->''data_sources'')::jsonb || ''"mastercard"''::jsonb
                    ELSE fu.metadata->''data_sources''
                END,
                ''visitor_detail_level'', COALESCE(fu.metadata->>''visitor_detail_level'', ''minimal''),
                ''spending_detail_level'', ''industry_daily''
            ) AS metadata,
            NOW() AS created_at,
            NOW() AS updated_at
        FROM
            daily_industry_metrics dim
        LEFT JOIN
            edw.fact_tourism_unified fu ON dim.date_id = fu.date_id 
                                    AND dim.region_id = fu.region_id 
                                    AND dim.visit_type_id = fu.visit_type_id 
                                    AND dim.data_type_id = fu.data_type_id
        ON CONFLICT (date_id, region_id, visit_type_id, data_type_id)
        DO UPDATE SET
            industry_count = EXCLUDED.industry_count,
            industry_metrics = EXCLUDED.industry_metrics,
            spending_data_completion_pct = 100,
            data_quality_score = CASE
                WHEN fact_tourism_unified.visitor_data_completion_pct > 0 THEN 100
                ELSE 50
            END,
            data_completeness = CASE
                WHEN fact_tourism_unified.visitor_data_completion_pct > 0 THEN ''Complete''
                ELSE ''Partial''
            END,
            metadata = jsonb_build_object(
                ''last_updated'', NOW(),
                ''data_sources'', CASE 
                    WHEN fact_tourism_unified.metadata->''data_sources'' IS NULL THEN jsonb_build_array(''mastercard'')
                    WHEN NOT (fact_tourism_unified.metadata->''data_sources'' @> ''"mastercard"''::jsonb) THEN 
                        (fact_tourism_unified.metadata->''data_sources'')::jsonb || ''"mastercard"''::jsonb
                    ELSE fact_tourism_unified.metadata->''data_sources''
                END,
                ''visitor_detail_level'', COALESCE(fact_tourism_unified.metadata->>''visitor_detail_level'', ''minimal''),
                ''spending_detail_level'', ''industry_daily''
            ),
            updated_at = NOW()';
    END IF;
END
$$;

-- Step 11: Calculate and populate daily performance summary
INSERT INTO edw.daily_tourism_performance_summary (
    summary_date,
    total_regions,
    total_industries,
    avg_transaction_index,
    max_transaction_index,
    min_transaction_index,
    top_performing_region,
    top_performing_industry,
    bottom_performing_region,
    bottom_performing_industry,
    avg_yoy_change_pct,
    is_peak_day
)
WITH daily_stats AS (
    SELECT
        dt.full_date,
        COUNT(DISTINCT f.region_id) AS total_regions,
        COUNT(DISTINCT f.industry_id) AS total_industries,
        AVG(f.txn_amt_index) AS avg_txn_index,
        MAX(f.txn_amt_index) AS max_txn_index,
        MIN(f.txn_amt_index) AS min_txn_index,
        AVG(f.yoy_txn_amt_pct) AS avg_yoy_change
    FROM
        edw.fact_tourism_spending_daily f
    JOIN
        edw.dim_transaction_date dt ON f.date_id = dt.date_id
    GROUP BY
        dt.full_date
),
top_performers AS (
    SELECT DISTINCT
        dt.full_date,
        FIRST_VALUE(r.region_name) OVER (PARTITION BY dt.full_date ORDER BY f.txn_amt_index DESC) AS top_region,
        FIRST_VALUE(i.industry_name) OVER (PARTITION BY dt.full_date ORDER BY f.txn_amt_index DESC) AS top_industry,
        FIRST_VALUE(r.region_name) OVER (PARTITION BY dt.full_date ORDER BY f.txn_amt_index ASC) AS bottom_region,
        FIRST_VALUE(i.industry_name) OVER (PARTITION BY dt.full_date ORDER BY f.txn_amt_index ASC) AS bottom_industry
    FROM
        edw.fact_tourism_spending_daily f
    JOIN
        edw.dim_transaction_date dt ON f.date_id = dt.date_id
    JOIN
        edw.dim_region r ON f.region_id = r.region_id
    JOIN
        edw.dim_industry i ON f.industry_id = i.industry_id
),
peak_days AS (
    SELECT
        dt.full_date,
        CASE
            WHEN AVG(f.txn_amt_index) > (
                SELECT AVG(f2.txn_amt_index) + STDDEV(f2.txn_amt_index)
                FROM edw.fact_tourism_spending_daily f2
            ) THEN TRUE
            ELSE FALSE
        END AS is_peak_day
    FROM
        edw.fact_tourism_spending_daily f
    JOIN
        edw.dim_transaction_date dt ON f.date_id = dt.date_id
    GROUP BY
        dt.full_date
)
SELECT
    ds.full_date,
    ds.total_regions,
    ds.total_industries,
    ds.avg_txn_index,
    ds.max_txn_index,
    ds.min_txn_index,
    tp.top_region,
    tp.top_industry,
    tp.bottom_region,
    tp.bottom_industry,
    ds.avg_yoy_change,
    pd.is_peak_day
FROM
    daily_stats ds
JOIN
    top_performers tp ON ds.full_date = tp.full_date
JOIN
    peak_days pd ON ds.full_date = pd.full_date
ON CONFLICT (summary_date) 
DO UPDATE SET
    total_regions = EXCLUDED.total_regions,
    total_industries = EXCLUDED.total_industries,
    avg_transaction_index = EXCLUDED.avg_transaction_index,
    max_transaction_index = EXCLUDED.max_transaction_index,
    min_transaction_index = EXCLUDED.min_transaction_index,
    top_performing_region = EXCLUDED.top_performing_region,
    top_performing_industry = EXCLUDED.top_performing_industry,
    bottom_performing_region = EXCLUDED.bottom_performing_region,
    bottom_performing_industry = EXCLUDED.bottom_performing_industry,
    avg_yoy_change_pct = EXCLUDED.avg_yoy_change_pct,
    is_peak_day = EXCLUDED.is_peak_day,
    updated_at = NOW();

-- Step 12: Create a materialized view for faster access to daily data
CREATE MATERIALIZED VIEW IF NOT EXISTS edw.mv_daily_tourism_spending AS
SELECT
    f.fact_id,
    dt.full_date AS transaction_date,
    dt.day_name,
    dt.month,
    dt.year,
    dt.season,
    r.region_name,
    r.region_type,
    i.industry_name,
    i.industry_category,
    f.txn_amt_index AS transaction_amount_index,
    f.txn_cnt_index AS transaction_count_index,
    f.acct_cnt_index AS account_count_index,
    f.avg_ticket_index,
    f.avg_freq_index AS average_frequency_index,
    f.avg_spend_amt_index AS average_spend_amount_index,
    f.yoy_txn_amt_pct AS yoy_spending_change_pct,
    f.yoy_txn_cnt_pct AS yoy_transaction_change_pct,
    f.quad_id,
    f.central_latitude,
    f.central_longitude,
    f.data_sources
FROM
    edw.fact_tourism_spending_daily f
JOIN
    edw.dim_transaction_date dt ON f.date_id = dt.date_id
JOIN
    edw.dim_region r ON f.region_id = r.region_id
JOIN
    edw.dim_industry i ON f.industry_id = i.industry_id;

-- Step 14: Create spatial analysis view
CREATE OR REPLACE VIEW edw.vw_tourism_spending_spatial AS
SELECT
    f.fact_id,
    dt.full_date AS transaction_date,
    dt.month,
    dt.year,
    r.region_name,
    r.region_type,
    i.industry_name,
    f.txn_amt_index AS transaction_amount_index,
    f.txn_cnt_index AS transaction_count_index,
    f.quad_id,
    f.central_latitude,
    f.central_longitude,
    f.bounding_box,
    CASE
        WHEN f.txn_amt_index > 150 THEN 'Very High'
        WHEN f.txn_amt_index > 120 THEN 'High'
        WHEN f.txn_amt_index > 80 THEN 'Average'
        WHEN f.txn_amt_index > 50 THEN 'Low'
        ELSE 'Very Low'
    END AS spending_category
FROM
    edw.fact_tourism_spending_daily f
JOIN
    edw.dim_transaction_date dt ON f.date_id = dt.date_id
JOIN
    edw.dim_region r ON f.region_id = r.region_id
JOIN
    edw.dim_industry i ON f.industry_id = i.industry_id
WHERE
    f.central_latitude IS NOT NULL
    AND     f.central_longitude IS NOT NULL;

-- Step 15: Create a daily trend analysis view
CREATE OR REPLACE VIEW edw.vw_tourism_daily_trends AS
SELECT
    dt.full_date AS transaction_date,
    dt.day_of_week,
    dt.is_weekday,
    dt.is_holiday,
    dt.season,
    r.region_name,
    r.region_type,
    i.industry_name,
    i.industry_category,
    f.txn_amt_index,
    f.txn_cnt_index,
    f.acct_cnt_index,
    f.avg_ticket_index,
    f.yoy_txn_amt_pct,
    LAG(f.txn_amt_index, 1) OVER (PARTITION BY r.region_id, i.industry_id ORDER BY dt.full_date) AS prev_day_txn_amt,
    LAG(f.txn_amt_index, 7) OVER (PARTITION BY r.region_id, i.industry_id ORDER BY dt.full_date) AS prev_week_txn_amt,
    LAG(f.txn_amt_index, 30) OVER (PARTITION BY r.region_id, i.industry_id ORDER BY dt.full_date) AS prev_month_txn_amt,
    LAG(f.txn_amt_index, 365) OVER (PARTITION BY r.region_id, i.industry_id ORDER BY dt.full_date) AS prev_year_txn_amt,
    f.txn_amt_index - LAG(f.txn_amt_index, 1) OVER (PARTITION BY r.region_id, i.industry_id ORDER BY dt.full_date) AS day_over_day_change,
    f.txn_amt_index - LAG(f.txn_amt_index, 7) OVER (PARTITION BY r.region_id, i.industry_id ORDER BY dt.full_date) AS week_over_week_change,
    CASE 
        WHEN LAG(f.txn_amt_index, 7) OVER (PARTITION BY r.region_id, i.industry_id ORDER BY dt.full_date) > 0 
        THEN (f.txn_amt_index - LAG(f.txn_amt_index, 7) OVER (PARTITION BY r.region_id, i.industry_id ORDER BY dt.full_date)) / 
             LAG(f.txn_amt_index, 7) OVER (PARTITION BY r.region_id, i.industry_id ORDER BY dt.full_date) * 100
        ELSE NULL
    END AS wow_pct_change
FROM
    edw.fact_tourism_spending_daily f
JOIN
    edw.dim_transaction_date dt ON f.date_id = dt.date_id
JOIN
    edw.dim_region r ON f.region_id = r.region_id
JOIN
    edw.dim_industry i ON f.industry_id = i.industry_id;

-- Step 16: Create a daily regional comparison view
CREATE OR REPLACE VIEW edw.vw_daily_regional_comparison AS
SELECT
    dt.full_date AS transaction_date,
    r.region_name,
    r.region_type,
    AVG(f.txn_amt_index) AS avg_transaction_index,
    AVG(f.txn_cnt_index) AS avg_transaction_count_index,
    AVG(f.acct_cnt_index) AS avg_account_count_index,
    AVG(f.avg_ticket_index) AS avg_ticket_size_index,
    AVG(f.yoy_txn_amt_pct) AS avg_yoy_spending_change,
    
    -- Regional rankings
    RANK() OVER (PARTITION BY dt.full_date ORDER BY AVG(f.txn_amt_index) DESC) AS region_rank_by_spending,
    RANK() OVER (PARTITION BY dt.full_date ORDER BY AVG(f.txn_cnt_index) DESC) AS region_rank_by_transaction_count,
    RANK() OVER (PARTITION BY dt.full_date ORDER BY AVG(f.yoy_txn_amt_pct) DESC) AS region_rank_by_yoy_growth,
    
    -- Compare against national average
    AVG(f.txn_amt_index) / (
        SELECT AVG(f2.txn_amt_index) 
        FROM edw.fact_tourism_spending_daily f2 
        JOIN edw.dim_transaction_date dt2 ON f2.date_id = dt2.date_id 
        WHERE dt2.full_date = dt.full_date
    ) * 100 AS pct_of_national_avg
    
    -- Note: Removed the problematic day-over-day change and top_industry_by_spending
    -- These can be added back with a different approach if needed
FROM
    edw.fact_tourism_spending_daily f
JOIN
    edw.dim_transaction_date dt ON f.date_id = dt.date_id
JOIN
    edw.dim_region r ON f.region_id = r.region_id
GROUP BY
    dt.full_date, r.region_id, r.region_name, r.region_type;

-- Step 17: Create a daily industry performance view
CREATE OR REPLACE VIEW edw.vw_daily_industry_performance AS
SELECT
    dt.full_date AS transaction_date,
    i.industry_name,
    i.industry_category,
    AVG(f.txn_amt_index) AS avg_transaction_index,
    AVG(f.txn_cnt_index) AS avg_transaction_count_index,
    AVG(f.avg_ticket_index) AS avg_ticket_size_index,
    AVG(f.yoy_txn_amt_pct) AS avg_yoy_spending_change,
    
    -- Industry rankings
    RANK() OVER (PARTITION BY dt.full_date ORDER BY AVG(f.txn_amt_index) DESC) AS industry_rank_by_spending,
    RANK() OVER (PARTITION BY dt.full_date ORDER BY AVG(f.yoy_txn_amt_pct) DESC) AS industry_rank_by_growth,
    
    -- Compare against overall average
    AVG(f.txn_amt_index) / (
        SELECT AVG(f2.txn_amt_index) 
        FROM edw.fact_tourism_spending_daily f2 
        JOIN edw.dim_transaction_date dt2 ON f2.date_id = dt2.date_id 
        WHERE dt2.full_date = dt.full_date
    ) * 100 AS pct_of_overall_avg
    
    -- Note: Removed the problematic day-over-day change and top_region_by_spending
    -- These can be added back with a different approach if needed
FROM
    edw.fact_tourism_spending_daily f
JOIN
    edw.dim_transaction_date dt ON f.date_id = dt.date_id
JOIN
    edw.dim_industry i ON f.industry_id = i.industry_id
GROUP BY
    dt.full_date, i.industry_id, i.industry_name, i.industry_category;

-- Step 18: Create a summary trends view for analysis
CREATE OR REPLACE VIEW edw.vw_daily_summary_trends AS
SELECT
    summary_date,
    total_regions,
    total_industries,
    avg_transaction_index,
    max_transaction_index,
    min_transaction_index,
    top_performing_region,
    top_performing_industry,
    avg_yoy_change_pct,
    is_peak_day,
    
    -- Calculate 7-day moving average for smoother trend analysis
    AVG(avg_transaction_index) OVER (
        ORDER BY summary_date
        ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    ) AS seven_day_moving_avg,
    
    -- Flag weekends vs weekdays
    CASE WHEN EXTRACT(ISODOW FROM summary_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    
    -- Calculate day-over-day change
    avg_transaction_index - LAG(avg_transaction_index, 1) OVER (ORDER BY summary_date) AS day_over_day_change,
    
    -- Calculate percent change
    CASE 
        WHEN LAG(avg_transaction_index, 1) OVER (ORDER BY summary_date) > 0 
        THEN (avg_transaction_index - LAG(avg_transaction_index, 1) OVER (ORDER BY summary_date)) / 
             LAG(avg_transaction_index, 1) OVER (ORDER BY summary_date) * 100
        ELSE NULL
    END AS day_over_day_pct_change,
    
    -- Flag days with significant changes (more than 10%)
    CASE 
        WHEN ABS((avg_transaction_index - LAG(avg_transaction_index, 1) OVER (ORDER BY summary_date)) / 
            NULLIF(LAG(avg_transaction_index, 1) OVER (ORDER BY summary_date), 0) * 100) > 10 
        THEN TRUE 
        ELSE FALSE 
    END AS significant_change_day
FROM
    edw.daily_tourism_performance_summary
ORDER BY
    summary_date;

-- Step 19: Create a daily seasonality analysis view
CREATE OR REPLACE VIEW edw.vw_daily_seasonality_analysis AS
WITH daily_avg AS (
    SELECT
        EXTRACT(DOY FROM dt.full_date) AS day_of_year,
        r.region_id,
        r.region_name,
        i.industry_id,
        i.industry_name,
        AVG(f.txn_amt_index) AS avg_txn_index
    FROM
        edw.fact_tourism_spending_daily f
    JOIN
        edw.dim_transaction_date dt ON f.date_id = dt.date_id
    JOIN
        edw.dim_region r ON f.region_id = r.region_id
    JOIN
        edw.dim_industry i ON f.industry_id = i.industry_id
    GROUP BY
        EXTRACT(DOY FROM dt.full_date),
        r.region_id,
        r.region_name,
        i.industry_id,
        i.industry_name
)
SELECT
    da.day_of_year,
    TO_DATE('2024-01-01', 'YYYY-MM-DD') + (da.day_of_year - 1) * INTERVAL '1 day' AS sample_date,
    TO_CHAR(TO_DATE('2024-01-01', 'YYYY-MM-DD') + (da.day_of_year - 1) * INTERVAL '1 day', 'Month DD') AS display_date,
    da.region_name,
    da.industry_name,
    da.avg_txn_index,
    
    -- Relationship to annual average
    da.avg_txn_index / (
        SELECT AVG(da2.avg_txn_index)
        FROM daily_avg da2
        WHERE da2.region_id = da.region_id AND da2.industry_id = da.industry_id
    ) * 100 AS pct_of_annual_avg,
    
    -- Seasonality score (standard deviation from annual mean)
    (da.avg_txn_index - (
        SELECT AVG(da3.avg_txn_index)
        FROM daily_avg da3
        WHERE da3.region_id = da.region_id AND da3.industry_id = da.industry_id
    )) / NULLIF((
        SELECT STDDEV(da4.avg_txn_index)
        FROM daily_avg da4
        WHERE da4.region_id = da.region_id AND da4.industry_id = da.industry_id
    ), 0) AS seasonality_z_score,
    
    -- Seasonality classification
    CASE
        WHEN da.avg_txn_index > (
            SELECT AVG(da5.avg_txn_index) + 1.5 * COALESCE(STDDEV(da5.avg_txn_index), 0)
            FROM daily_avg da5
            WHERE da5.region_id = da.region_id AND da5.industry_id = da.industry_id
        ) THEN 'Peak Season'
        WHEN da.avg_txn_index < (
            SELECT AVG(da6.avg_txn_index) - 1.0 * COALESCE(STDDEV(da6.avg_txn_index), 0)
            FROM daily_avg da6
            WHERE da6.region_id = da.region_id AND da6.industry_id = da.industry_id
        ) THEN 'Low Season'
        WHEN da.avg_txn_index > (
            SELECT AVG(da7.avg_txn_index) + 0.5 * COALESCE(STDDEV(da7.avg_txn_index), 0)
            FROM daily_avg da7
            WHERE da7.region_id = da.region_id AND da7.industry_id = da.industry_id
        ) THEN 'High Season'
        ELSE 'Shoulder Season'
    END AS season_classification
FROM
    daily_avg da;

-- Step 20: Create a function for flexible data retrieval
CREATE OR REPLACE FUNCTION edw.get_daily_tourism_data(
    start_date DATE,
    end_date DATE,
    region_ids INTEGER[] DEFAULT NULL,
    industry_ids INTEGER[] DEFAULT NULL
)
RETURNS TABLE (
    transaction_date DATE,
    region_name TEXT,
    industry_name TEXT,
    txn_amt_index NUMERIC,
    txn_cnt_index NUMERIC,
    avg_ticket_index NUMERIC,
    yoy_change_pct NUMERIC,
    day_name TEXT,
    is_weekday BOOLEAN,
    is_holiday BOOLEAN
) 
LANGUAGE plpgsql
AS $
BEGIN
    RETURN QUERY
    SELECT
        dt.full_date,
        r.region_name,
        i.industry_name,
        f.txn_amt_index,
        f.txn_cnt_index,
        f.avg_ticket_index,
        f.yoy_txn_amt_pct,
        dt.day_name,
        dt.is_weekday,
        dt.is_holiday
    FROM
        edw.fact_tourism_spending_daily f
    JOIN
        edw.dim_transaction_date dt ON f.date_id = dt.date_id
    JOIN
        edw.dim_region r ON f.region_id = r.region_id
    JOIN
        edw.dim_industry i ON f.industry_id = i.industry_id
    WHERE
        dt.full_date BETWEEN start_date AND end_date
        AND (region_ids IS NULL OR f.region_id = ANY(region_ids))
        AND (industry_ids IS NULL OR f.industry_id = ANY(industry_ids))
    ORDER BY
        dt.full_date, r.region_name, i.industry_name;
END;
$;

-- Step 21: Create indices for better query performance
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON edw.daily_tourism_performance_summary(summary_date);
CREATE INDEX IF NOT EXISTS idx_fact_tourism_spending_date ON edw.fact_tourism_spending_daily(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_tourism_spending_region ON edw.fact_tourism_spending_daily(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_tourism_spending_industry ON edw.fact_tourism_spending_daily(industry_id);

-- Log completion
DO $
BEGIN
    RAISE NOTICE 'MasterCard Data Integration Part 2 completed successfully at %', NOW();
END $;
    dt.day_of_week,
    dt.month,
    dt.year,
    dt.season,
    dt.is_weekday,
    dt.is_holiday,
    r.region_id,
    r.region_name,
    r.region_type,
    i.industry_id,
    i.industry_name,
    i.industry_category,
    f.txn_amt_index AS transaction_amount_index,
    f.txn_cnt_index AS transaction_count_index,
    f.acct_cnt_index AS account_count_index,
    f.avg_ticket_index,
    f.avg_freq_index AS average_frequency_index,
    f.avg_spend_amt_index AS average_spend_amount_index,
    f.yoy_txn_amt_pct AS yoy_spending_change_pct,
    f.yoy_txn_cnt_pct AS yoy_transaction_change_pct,
    f.quad_id,
    f.central_latitude,
    f.central_longitude,
    CASE 
        WHEN f.txn_amt_index > 150 THEN 'Very High'
        WHEN f.txn_amt_index > 120 THEN 'High'
        WHEN f.txn_amt_index > 80 THEN 'Average'
        WHEN f.txn_amt_index > 50 THEN 'Low'
        ELSE 'Very Low'
    END AS spending_category
FROM
    edw.fact_tourism_spending_daily f
JOIN
    edw.dim_transaction_date dt ON f.date_id = dt.date_id
JOIN
    edw.dim_region r ON f.region_id = r.region_id
JOIN
    edw.dim_industry i ON f.industry_id = i.industry_id;

-- Create indices for better materialized view performance
CREATE INDEX IF NOT EXISTS idx_mv_daily_spending_date ON edw.mv_daily_tourism_spending(transaction_date);
CREATE INDEX IF NOT EXISTS idx_mv_daily_spending_region ON edw.mv_daily_tourism_spending(region_id);
CREATE INDEX IF NOT EXISTS idx_mv_daily_spending_industry ON edw.mv_daily_tourism_spending(industry_id);
CREATE INDEX IF NOT EXISTS idx_mv_daily_spending_category ON edw.mv_daily_tourism_spending(spending_category);

-- Add comment on refresh schedule
COMMENT ON MATERIALIZED VIEW edw.mv_daily_tourism_spending IS 'Daily tourism spending indices with refresh frequency: Daily at 03:00';

-- Step 13: Create basic daily transaction view
CREATE OR REPLACE VIEW edw.vw_tourism_spending_daily AS
SELECT
    f.fact_id,
    dt.full_date AS transaction_date,
    dt.day_name,