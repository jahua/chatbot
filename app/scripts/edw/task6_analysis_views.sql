-- Task 6: Create Analysis Views
DROP VIEW IF EXISTS edw.analysis_industry_spending CASCADE;
DROP VIEW IF EXISTS edw.analysis_region_performance CASCADE;
DROP VIEW IF EXISTS edw.analysis_visitor_trends CASCADE;

SELECT edw.report_status('Task 6: Starting analysis views creation');

-- Tourism Visitor Trends Analysis View
CREATE OR REPLACE VIEW edw.analysis_visitor_trends AS
SELECT
    f.year,
    f.month_number,
    f.month_name,
    f.season,
    -- Total metrics
    SUM(f.visitor_count) AS total_visitors,
    SUM(f.swiss_tourists_raw) AS total_swiss_tourists,
    SUM(f.foreign_tourists_raw) AS total_foreign_tourists,
    SUM(f.staydays) AS total_staydays,
    -- Demographics
    SUM(f.sex_male) AS male_visitors,
    SUM(f.sex_female) AS female_visitors,
    SUM(f.age_15_29) AS visitors_15_29,
    SUM(f.age_30_44) AS visitors_30_44,
    SUM(f.age_45_59) AS visitors_45_59,
    SUM(f.age_60_plus) AS visitors_60_plus,
    -- Percentages
    (100.0 * SUM(f.age_15_29) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_age_15_29,
    (100.0 * SUM(f.age_30_44) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_age_30_44,
    (100.0 * SUM(f.age_45_59) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_age_45_59,
    (100.0 * SUM(f.age_60_plus) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_age_60_plus,
    (100.0 * SUM(f.sex_male) / NULLIF(SUM(f.sex_male + f.sex_female), 0))::numeric(5,1) AS pct_male,
    -- Data quality metrics
    COUNT(*) AS total_records,
    SUM(CASE WHEN f.has_raw_data_match THEN 1 ELSE 0 END) AS records_with_raw_data,
    (100.0 * SUM(CASE WHEN f.has_raw_data_match THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0))::numeric(5,1) AS pct_with_raw_data,
    AVG(f.visitor_data_quality_score)::numeric(5,1) AS avg_data_quality_score
FROM edw.fact_tourism_unified f
GROUP BY f.year, f.month_number, f.month_name, f.season
ORDER BY f.year, f.month_number;

SELECT edw.report_status('Visitor trends analysis view created');

-- Region Performance Analysis View
CREATE OR REPLACE VIEW edw.analysis_region_performance AS
SELECT
    f.year,
    f.region_name,
    f.region_type,
    -- Visitor metrics
    SUM(f.visitor_count) AS total_visitors,
    (AVG(f.visitor_count))::numeric(10,1) AS avg_monthly_visitors,
    SUM(f.staydays) AS total_staydays,
    (AVG(f.staydays))::numeric(10,1) AS avg_staydays,
    (SUM(f.staydays) / NULLIF(SUM(f.visitor_count), 0))::numeric(5,2) AS avg_stay_per_visitor,
    -- Spending metrics
    SUM(f.spending_amount) AS total_spending,
    SUM(f.transaction_count) AS total_transactions,
    (SUM(f.spending_amount) / NULLIF(SUM(f.visitor_count), 0))::numeric(10,2) AS spend_per_visitor,
    (SUM(f.spending_amount) / NULLIF(SUM(f.transaction_count), 0))::numeric(10,2) AS avg_transaction_value,
    -- Demographics
    (100.0 * SUM(f.sex_male) / NULLIF(SUM(f.sex_male + f.sex_female), 0))::numeric(5,1) AS pct_male_visitors,
    (100.0 * SUM(f.age_15_29) / NULLIF(SUM(f.age_15_29 + f.age_30_44 + f.age_45_59 + f.age_60_plus), 0))::numeric(5,1) AS pct_young_visitors,
    -- Visitor mix
    (100.0 * SUM(COALESCE(f.swiss_tourists_raw, 0)) / 
        NULLIF(SUM(COALESCE(f.swiss_tourists_raw, 0) + COALESCE(f.foreign_tourists_raw, 0)), 0))::numeric(5,1) AS pct_swiss_tourists,
    (100.0 * SUM(COALESCE(f.swiss_locals_raw, 0)) / 
        NULLIF(SUM(COALESCE(f.swiss_tourists_raw, 0) + COALESCE(f.foreign_tourists_raw, 0) + 
        COALESCE(f.swiss_locals_raw, 0) + COALESCE(f.foreign_workers_raw, 0) + 
        COALESCE(f.swiss_commuters_raw, 0)), 0))::numeric(5,1) AS pct_locals,
    -- Industry metrics
    AVG(COALESCE(f.industry_count, 0))::numeric(5,1) AS avg_industries_per_record,
    -- Dwell time
    AVG(COALESCE(f.avg_dwell_time_mins, 0))::numeric(5,1) AS avg_dwell_time_mins,
    -- Data quality metrics
    COUNT(*) AS total_records,
    SUM(CASE WHEN f.spending_amount IS NOT NULL THEN 1 ELSE 0 END) AS records_with_spending,
    (100.0 * SUM(CASE WHEN f.spending_amount IS NOT NULL THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0))::numeric(5,1) AS pct_with_spending,
    AVG(f.data_quality_score)::numeric(5,1) AS avg_data_quality_score,
    -- Region categorization
    CASE 
        WHEN SUM(f.visitor_count) > 100000 THEN 'High Volume'
        WHEN SUM(f.visitor_count) > 50000 THEN 'Medium Volume'
        ELSE 'Low Volume'
    END AS visitor_volume_category,
    -- Spending categorization
    CASE 
        WHEN SUM(COALESCE(f.spending_amount, 0)) > 1000000 THEN 'High Spend'
        WHEN SUM(COALESCE(f.spending_amount, 0)) > 500000 THEN 'Medium Spend'
        ELSE 'Low Spend'
    END AS spending_category
FROM edw.fact_tourism_unified f
GROUP BY f.year, f.region_name, f.region_type
ORDER BY total_visitors DESC;

SELECT edw.report_status('Region performance analysis view created');

-- Industry Spending Analysis View
CREATE OR REPLACE VIEW edw.analysis_industry_spending AS
SELECT
    f.year,
    f.season,
    f.industry,
    -- Spending metrics
    SUM(f.spending_amount) AS total_spending,
    SUM(f.transaction_count) AS transaction_count,
    (AVG(f.avg_transaction_size))::numeric(10,2) AS average_transaction,
    -- Region coverage
    COUNT(DISTINCT f.region_name) AS region_count,
    -- Associated visitor metrics
    SUM(f.visitor_count) AS associated_visitors,
    -- Computed metrics
    (SUM(f.spending_amount) / NULLIF(SUM(f.visitor_count), 0))::numeric(10,2) AS spend_per_visitor,
    -- Categorize industries
    CASE
        WHEN SUM(f.spending_amount) > 1000000 THEN 'Major Industry'
        WHEN SUM(f.spending_amount) > 500000 THEN 'Significant Industry'
        ELSE 'Minor Industry'
    END AS industry_category,
    -- Data quality metrics
    COUNT(*) AS total_records,
    SUM(CASE WHEN f.spending_amount IS NOT NULL THEN 1 ELSE 0 END) AS records_with_spending,
    (100.0 * SUM(CASE WHEN f.spending_amount IS NOT NULL THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0))::numeric(5,1) AS pct_with_spending,
    AVG(f.spending_data_quality_score)::numeric(5,1) AS avg_data_quality_score
FROM edw.fact_tourism_unified f
WHERE f.industry IS NOT NULL
GROUP BY f.year, f.season, f.industry
ORDER BY f.year, f.season, total_spending DESC;

SELECT edw.report_status('Industry spending analysis view created');
SELECT edw.report_status('Task 6: All analysis views created successfully'); 