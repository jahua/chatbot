-- Process Time Dimension
DO $$
BEGIN
    PERFORM edw.report_status('Starting dim_time processing');
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'edw' AND table_name = 'dim_time')
    THEN
        PERFORM edw.report_status('Creating dim_time table');
        
        CREATE TABLE edw.dim_time AS
        SELECT 
            date_id,
            month,
            month_short,
            month_number,
            year,
            CASE 
                WHEN month_number BETWEEN 3 AND 5 THEN 'Spring'
                WHEN month_number BETWEEN 6 AND 8 THEN 'Summer'
                WHEN month_number BETWEEN 9 AND 11 THEN 'Fall'
                ELSE 'Winter'
            END AS season,
            EXTRACT(WEEK FROM DATE(year || '-' || month_number || '-01')) AS week_of_year,
            DATE(year || '-' || month_number || '-01') AS period_start_date,
            (DATE(year || '-' || month_number || '-01') + 
                CASE
                    WHEN month_number IN (1,3,5,7,8,10,12) THEN INTERVAL '31 days'
                    WHEN month_number IN (4,6,9,11) THEN INTERVAL '30 days'
                    WHEN month_number = 2 AND year % 4 = 0 AND (year % 100 <> 0 OR year % 400 = 0) THEN INTERVAL '29 days'
                    ELSE INTERVAL '28 days'
                END - INTERVAL '1 day')::date AS period_end_date,
            'inervista.dim_date' AS data_source,
            TRUE AS is_complete_period,
            'monthly' AS granularity
        FROM inervista.dim_date;

        ALTER TABLE edw.dim_time ADD PRIMARY KEY (date_id);
        CREATE INDEX idx_dim_time_year_month ON edw.dim_time(year, month_number);
        
        PERFORM edw.report_status('dim_time created with ' || (SELECT COUNT(*) FROM edw.dim_time) || ' rows');
    ELSE
        PERFORM edw.report_status('dim_time already exists');
    END IF;
END;
$$;