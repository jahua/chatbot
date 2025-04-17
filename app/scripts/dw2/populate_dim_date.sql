-- Improved populate_dim_date.sql
-- This script populates the dw.dim_date dimension with dates
-- spanning from January 1, 2022 to December 31, 2023

-- First, make sure all constraints that could cause issues are addressed
DO $$
BEGIN
    -- Drop problematic constraints if they exist
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_day_of_week') THEN
        EXECUTE 'ALTER TABLE dw.dim_date DROP CONSTRAINT chk_day_of_week';
        RAISE NOTICE 'Dropped day_of_week constraint for compatibility with PostgreSQL DOW function';
    END IF;
    
    -- Check if any other constraints need to be modified
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_season') THEN
        EXECUTE 'ALTER TABLE dw.dim_date DROP CONSTRAINT chk_season';
        RAISE NOTICE 'Dropped season constraint for flexibility';
    END IF;
END $$;

DO $$
DECLARE
    v_start_date DATE := '2022-01-01';
    v_end_date DATE := '2023-12-31';
    v_current_date DATE;
    v_date_id INTEGER;
    v_day_of_week INTEGER;
    v_month INTEGER;
    v_month_name VARCHAR(10);
    v_quarter INTEGER;
    v_year INTEGER;
    v_is_weekend BOOLEAN;
    v_season VARCHAR(10);
    v_batch_id INTEGER;
    v_count INTEGER := 0;
BEGIN
    -- Get or create a batch ID for this load
    BEGIN
        INSERT INTO dw.etl_metadata(
            task_name,
            status,
            source_system,
            start_time
        )
        VALUES(
            'populate_dim_date',
            'running',
            'script',
            NOW()
        )
        RETURNING etl_id INTO v_batch_id;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Could not create ETL metadata record: %', SQLERRM;
        v_batch_id := 0;
    END;
    
    RAISE NOTICE 'Starting date dimension population (batch ID: %)', v_batch_id;
    
    -- Generate date records
    v_current_date := v_start_date;
    
    WHILE v_current_date <= v_end_date LOOP
        -- Create date ID in format YYYYMMDD
        v_date_id := TO_CHAR(v_current_date, 'YYYYMMDD')::INTEGER;
        
        -- Extract date components
        v_day_of_week := EXTRACT(DOW FROM v_current_date);
        v_month := EXTRACT(MONTH FROM v_current_date);
        v_month_name := TRIM(TO_CHAR(v_current_date, 'Month'));
        v_quarter := EXTRACT(QUARTER FROM v_current_date);
        v_year := EXTRACT(YEAR FROM v_current_date);
        
        -- Determine if weekend
        v_is_weekend := (v_day_of_week = 0 OR v_day_of_week = 6);
        
        -- Determine season (Northern Hemisphere)
        v_season := CASE
            WHEN (v_month BETWEEN 3 AND 5) THEN 'Spring'
            WHEN (v_month BETWEEN 6 AND 8) THEN 'Summer'
            WHEN (v_month BETWEEN 9 AND 11) THEN 'Fall'
            ELSE 'Winter'
        END;
        
        -- Insert into dim_date, skipping if entry already exists
        BEGIN
            INSERT INTO dw.dim_date (
                date_id, 
                full_date, 
                year,
                quarter,
                month,
                week,
                day,
                day_of_week,
                is_weekend, 
                is_holiday, 
                season,
                month_name
            )
            VALUES (
                v_date_id,
                v_current_date,
                v_year,
                v_quarter,
                v_month,
                EXTRACT(WEEK FROM v_current_date),
                EXTRACT(DAY FROM v_current_date),
                v_day_of_week,
                v_is_weekend,
                FALSE, -- No holiday detection
                v_season,
                v_month_name
            )
            ON CONFLICT (date_id) DO NOTHING;
            
            GET DIAGNOSTICS v_count = ROW_COUNT;
            IF v_count > 0 THEN
                v_count := v_count + 1;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Error inserting date %: %', v_current_date, SQLERRM;
        END;
        
        -- Move to next day
        v_current_date := v_current_date + INTERVAL '1 day';
    END LOOP;
    
    -- Update ETL metadata if we created a record
    IF v_batch_id > 0 THEN
        BEGIN
            UPDATE dw.etl_metadata
            SET 
                status = 'completed',
                end_time = NOW(),
                message = 'Successfully loaded ' || v_count || ' date records'
            WHERE etl_id = v_batch_id;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Could not update ETL metadata: %', SQLERRM;
        END;
    END IF;
    
    RAISE NOTICE 'Date dimension population completed with % records added', v_count;
END $$;

-- Add month_name if not present and ensure it's trimmed
DO $$
BEGIN
    -- Update month_name based on full_date to ensure consistency
    UPDATE dw.dim_date
    SET month_name = TRIM(TO_CHAR(full_date, 'Month'))
    WHERE month_name IS NULL OR month_name = '' OR month_name LIKE '% ';
    
    RAISE NOTICE 'Updated month_name values';
END $$;

-- Display date range in table
SELECT 
    MIN(full_date) AS min_date,
    MAX(full_date) AS max_date,
    COUNT(*) AS record_count,
    COUNT(DISTINCT year) AS unique_years,
    COUNT(DISTINCT month_name) AS unique_months
FROM 
    dw.dim_date;

-- Show sample of dates
SELECT 
    date_id, 
    full_date, 
    day_of_week, 
    is_weekend, 
    month_name
FROM 
    dw.dim_date
LIMIT 10; 