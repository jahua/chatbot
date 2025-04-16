-- Improved Spending Fact Tables Setup

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS dw;

-- Create the update_timestamp function first if it doesn't exist
CREATE OR REPLACE FUNCTION dw.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Make sure batch_id column exists in fact_spending
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'fact_spending') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'dw' AND table_name = 'fact_spending' AND column_name = 'batch_id') THEN
            ALTER TABLE dw.fact_spending ADD COLUMN batch_id INTEGER;
        END IF;
    END IF;
END $$;

-- Add month_name to dim_date if it doesn't exist
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'dim_date') THEN
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'dw' AND table_name = 'dim_date' AND column_name = 'month_name') THEN
            ALTER TABLE dw.dim_date ADD COLUMN month_name VARCHAR(20);
            UPDATE dw.dim_date SET month_name = TO_CHAR(full_date, 'Month');
        END IF;
    END IF;
END $$;

-- Create standard industry categories if they don't exist
DO $$
BEGIN
    -- Create sample industries if they don't exist
    IF NOT EXISTS (SELECT 1 FROM dw.dim_industry WHERE industry_name = 'Retail') THEN
        INSERT INTO dw.dim_industry (industry_code, industry_name, industry_category, is_active)
        VALUES 
            ('RET001', 'Retail', 'Retail', TRUE),
            ('HOS001', 'Hospitality', 'Hospitality', TRUE),
            ('TRN001', 'Transportation', 'Transportation', TRUE);
        RAISE NOTICE 'Created sample industry records';
    END IF;
END $$;

-- Function to load spending data with proper error handling
CREATE OR REPLACE FUNCTION dw.load_spending_data(
    p_date_id INTEGER,
    p_region_name VARCHAR,
    p_industry_name VARCHAR,
    p_transaction_count INTEGER,
    p_total_amount NUMERIC,
    p_batch_id INTEGER DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_region_id INTEGER;
    v_industry_id INTEGER;
    v_fact_id INTEGER;
BEGIN
    -- Get region ID with error handling
    SELECT region_id INTO v_region_id
    FROM dw.dim_region
    WHERE region_name = p_region_name
    AND is_active = TRUE;
    
    IF v_region_id IS NULL THEN
        RAISE EXCEPTION 'Region not found: %', p_region_name;
    END IF;

    -- Get industry ID with error handling
    SELECT industry_id INTO v_industry_id
    FROM dw.dim_industry
    WHERE industry_name = p_industry_name
    AND is_active = TRUE;
    
    IF v_industry_id IS NULL THEN
        -- Try to create the industry if it doesn't exist
        INSERT INTO dw.dim_industry (
            industry_code,
            industry_name,
            industry_category,
            is_active
        ) VALUES (
            UPPER(REGEXP_REPLACE(p_industry_name, '[^a-zA-Z0-9]', '')::VARCHAR),
            p_industry_name,
            p_industry_name, -- Default to same as name
            TRUE
        )
        RETURNING industry_id INTO v_industry_id;
        
        RAISE NOTICE 'Created new industry: %', p_industry_name;
    END IF;

    -- Insert spending data
    INSERT INTO dw.fact_spending (
        date_id,
        region_id,
        industry_id,
        transaction_count,
        total_amount,
        source_system,
        batch_id
    ) VALUES (
        p_date_id,
        v_region_id,
        v_industry_id,
        p_transaction_count,
        p_total_amount,
        'mastercard',
        p_batch_id
    )
    RETURNING fact_id INTO v_fact_id;

    RETURN v_fact_id;
END;
$$ LANGUAGE plpgsql;

-- View for daily spending by region
CREATE OR REPLACE VIEW dw.v_daily_spending AS
SELECT 
    d.full_date,
    r.region_name,
    i.industry_name,
    SUM(fs.transaction_count) AS total_transactions,
    SUM(fs.total_amount) AS total_spending,
    CASE 
        WHEN SUM(fs.transaction_count) > 0 
        THEN SUM(fs.total_amount) / SUM(fs.transaction_count)
        ELSE 0
    END AS avg_transaction_value
FROM dw.fact_spending fs
JOIN dw.dim_date d ON fs.date_id = d.date_id
JOIN dw.dim_region r ON fs.region_id = r.region_id
JOIN dw.dim_industry i ON fs.industry_id = i.industry_id
WHERE r.is_active = TRUE AND i.is_active = TRUE
GROUP BY d.full_date, r.region_name, i.industry_name;

-- View for monthly spending trends
CREATE OR REPLACE VIEW dw.v_monthly_spending_trends AS
SELECT 
    d.year,
    TO_CHAR(d.full_date, 'Month') as month_name,
    r.region_name,
    i.industry_name,
    SUM(fs.transaction_count) AS total_transactions,
    SUM(fs.total_amount) AS total_spending,
    CASE 
        WHEN SUM(fs.transaction_count) > 0 
        THEN SUM(fs.total_amount) / SUM(fs.transaction_count)
        ELSE 0
    END AS avg_transaction_value
FROM dw.fact_spending fs
JOIN dw.dim_date d ON fs.date_id = d.date_id
JOIN dw.dim_region r ON fs.region_id = r.region_id
JOIN dw.dim_industry i ON fs.industry_id = i.industry_id
WHERE r.is_active = TRUE AND i.is_active = TRUE
GROUP BY d.year, d.full_date, r.region_name, i.industry_name;

-- Load sample spending data
DO $$
DECLARE
    v_next_batch INTEGER;
    v_fact_id INTEGER;
BEGIN
    -- Get a batch ID for this load
    SELECT COALESCE(MAX(batch_id), 0) + 1 INTO v_next_batch FROM dw.fact_spending;

    BEGIN
        -- Load sample data for Zürich
        SELECT dw.load_spending_data(20240315, 'Zürich', 'Retail', 1000, 50000.00, v_next_batch) INTO v_fact_id;
        RAISE NOTICE 'Loaded Zürich Retail spending data with fact_id %', v_fact_id;
        
        SELECT dw.load_spending_data(20240315, 'Zürich', 'Hospitality', 500, 25000.00, v_next_batch) INTO v_fact_id;
        RAISE NOTICE 'Loaded Zürich Hospitality spending data with fact_id %', v_fact_id;
        
        SELECT dw.load_spending_data(20240315, 'Zürich', 'Transportation', 300, 15000.00, v_next_batch) INTO v_fact_id;
        RAISE NOTICE 'Loaded Zürich Transportation spending data with fact_id %', v_fact_id;

        -- Load sample data for Bern
        SELECT dw.load_spending_data(20240315, 'Bern', 'Retail', 800, 40000.00, v_next_batch) INTO v_fact_id;
        RAISE NOTICE 'Loaded Bern Retail spending data with fact_id %', v_fact_id;
        
        SELECT dw.load_spending_data(20240315, 'Bern', 'Hospitality', 400, 20000.00, v_next_batch) INTO v_fact_id;
        RAISE NOTICE 'Loaded Bern Hospitality spending data with fact_id %', v_fact_id;
        
        SELECT dw.load_spending_data(20240315, 'Bern', 'Transportation', 200, 10000.00, v_next_batch) INTO v_fact_id;
        RAISE NOTICE 'Loaded Bern Transportation spending data with fact_id %', v_fact_id;

        -- Load sample data for Luzern
        SELECT dw.load_spending_data(20240315, 'Luzern', 'Retail', 600, 30000.00, v_next_batch) INTO v_fact_id;
        RAISE NOTICE 'Loaded Luzern Retail spending data with fact_id %', v_fact_id;
        
        SELECT dw.load_spending_data(20240315, 'Luzern', 'Hospitality', 300, 15000.00, v_next_batch) INTO v_fact_id;
        RAISE NOTICE 'Loaded Luzern Hospitality spending data with fact_id %', v_fact_id;
        
        SELECT dw.load_spending_data(20240315, 'Luzern', 'Transportation', 150, 7500.00, v_next_batch) INTO v_fact_id;
        RAISE NOTICE 'Loaded Luzern Transportation spending data with fact_id %', v_fact_id;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Error loading sample data: %', SQLERRM;
    END;
END $$;

-- Check for missing region references
SELECT 
    'Missing region references' as check_name,
    COUNT(*) as count
FROM dw.fact_spending fs
LEFT JOIN dw.dim_region r ON fs.region_id = r.region_id
WHERE r.region_id IS NULL;

-- Check for missing industry references
SELECT 
    'Missing industry references' as check_name,
    COUNT(*) as count
FROM dw.fact_spending fs
LEFT JOIN dw.dim_industry i ON fs.industry_id = i.industry_id
WHERE i.industry_id IS NULL;

-- Check for negative values
SELECT 
    'Negative transaction counts' as check_name,
    COUNT(*) as count
FROM dw.fact_spending
WHERE transaction_count < 0;

SELECT 
    'Negative amounts' as check_name,
    COUNT(*) as count
FROM dw.fact_spending
WHERE total_amount < 0;

-- Function to load spending data from MasterCard
CREATE OR REPLACE FUNCTION dw.load_mastercard_spending(
    p_date_from DATE DEFAULT NULL,
    p_date_to DATE DEFAULT NULL,
    p_batch_id INTEGER DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
    v_batch_id INTEGER;
BEGIN
    -- Set default date range if not provided
    IF p_date_from IS NULL THEN
        p_date_from := CURRENT_DATE - INTERVAL '30 days';
    END IF;
    
    IF p_date_to IS NULL THEN
        p_date_to := CURRENT_DATE - INTERVAL '1 day';
    END IF;
    
    -- Generate a new batch ID if none provided
    IF p_batch_id IS NULL THEN
        SELECT COALESCE(MAX(batch_id), 0) + 1 INTO v_batch_id FROM dw.fact_spending;
    ELSE
        v_batch_id := p_batch_id;
    END IF;
    
    -- Insert into fact spending table
    -- Note: This is simplified and assumes the real table exists
    -- In a real system, you would join to actual master_card data
    INSERT INTO dw.fact_spending (
        date_id,
        region_id,
        industry_id,
        transaction_count,
        total_amount,
        source_system,
        batch_id
    )
    SELECT 
        20240401, -- Example date_id
        r.region_id,
        i.industry_id,
        100, -- Example transaction count
        5000.00, -- Example amount
        'mastercard',
        v_batch_id
    FROM 
        dw.dim_region r,
        dw.dim_industry i
    WHERE 
        r.region_name = 'Zürich'
        AND i.industry_name = 'Retail'
    LIMIT 1;

    -- Get count of inserted records
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Function to load spending data from Intervista
CREATE OR REPLACE FUNCTION dw.load_intervista_spending(
    p_date_from DATE DEFAULT NULL,
    p_date_to DATE DEFAULT NULL,
    p_batch_id INTEGER DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
    v_batch_id INTEGER;
BEGIN
    -- Set default date range if not provided
    IF p_date_from IS NULL THEN
        p_date_from := CURRENT_DATE - INTERVAL '30 days';
    END IF;
    
    IF p_date_to IS NULL THEN
        p_date_to := CURRENT_DATE - INTERVAL '1 day';
    END IF;
    
    -- Generate a new batch ID if none provided
    IF p_batch_id IS NULL THEN
        SELECT COALESCE(MAX(batch_id), 0) + 1 INTO v_batch_id FROM dw.fact_spending;
    ELSE
        v_batch_id := p_batch_id;
    END IF;
    
    -- Insert into fact spending table
    -- Note: This is simplified and assumes the real table exists
    -- In a real system, you would join to actual intervista data
    INSERT INTO dw.fact_spending (
        date_id,
        region_id,
        industry_id,
        transaction_count,
        total_amount,
        source_system,
        batch_id
    )
    SELECT 
        20240401, -- Example date_id
        r.region_id,
        i.industry_id,
        100, -- Example transaction count
        5000.00, -- Example amount
        'intervista',
        v_batch_id
    FROM 
        dw.dim_region r,
        dw.dim_industry i
    WHERE 
        r.region_name = 'Bern'
        AND i.industry_name = 'Hospitality'
    LIMIT 1;

    -- Get count of inserted records
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Create a view for spending analysis
CREATE OR REPLACE VIEW dw.vw_spending_analysis AS
SELECT 
    d.full_date,
    r.region_name,
    i.industry_name,
    SUM(f.transaction_count) AS total_transactions,
    SUM(f.total_amount) AS total_spending,
    COUNT(f.fact_id) AS record_count
FROM 
    dw.fact_spending f
JOIN 
    dw.dim_date d ON f.date_id = d.date_id
JOIN 
    dw.dim_region r ON f.region_id = r.region_id
JOIN 
    dw.dim_industry i ON f.industry_id = i.industry_id
GROUP BY 
    d.full_date, r.region_name, i.industry_name;

-- Create a procedure to handle regular loading
CREATE OR REPLACE PROCEDURE dw.load_spending_incremental(
    p_days_back INTEGER DEFAULT 7
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id INTEGER;
    v_start_date DATE;
    v_end_date DATE;
    v_mastercard_count INTEGER;
    v_intervista_count INTEGER;
BEGIN
    -- Calculate date range
    v_end_date := CURRENT_DATE - INTERVAL '1 day';
    v_start_date := v_end_date - (p_days_back || ' days')::INTERVAL;
    
    -- Generate a new batch ID
    SELECT COALESCE(MAX(batch_id), 0) + 1 INTO v_batch_id FROM dw.fact_spending;
    
    -- Load data from both sources
    SELECT dw.load_mastercard_spending(v_start_date, v_end_date, v_batch_id) INTO v_mastercard_count;
    SELECT dw.load_intervista_spending(v_start_date, v_end_date, v_batch_id) INTO v_intervista_count;
    
    -- Log the results
    RAISE NOTICE 'Loaded % MasterCard and % Intervista spending records for date range % to % (batch ID: %)', 
                 v_mastercard_count, v_intervista_count, v_start_date, v_end_date, v_batch_id;
END;
$$;