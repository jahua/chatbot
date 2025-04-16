-- Improved Spending Fact Tables Setup

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS dw;

-- Create the update_timestamp function first
CREATE OR REPLACE FUNCTION dw.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

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

-- Check if fact_spending exists and drop it to recreate with partitioning
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'dw' AND table_name = 'fact_spending') THEN
        -- Check if it's already partitioned - if not, we need to drop and recreate
        IF NOT EXISTS (
            SELECT 1 FROM pg_partitioned_table pt 
            JOIN pg_class c ON c.oid = pt.partrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'dw' AND c.relname = 'fact_spending'
        ) THEN
            DROP TABLE dw.fact_spending CASCADE;
        END IF;
    END IF;
END $$;

-- Create the spending fact table with proper constraints
CREATE TABLE IF NOT EXISTS dw.fact_spending (
    fact_id SERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES dw.dim_date(date_id),
    region_id INTEGER NOT NULL REFERENCES dw.dim_region(region_id),
    industry_id INTEGER NOT NULL REFERENCES dw.dim_industry(industry_id),
    transaction_count INTEGER NOT NULL CHECK (transaction_count >= 0),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    source_system VARCHAR(50) NOT NULL CHECK (source_system IN ('mastercard', 'intervista')),
    batch_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_spending_date ON dw.fact_spending(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_spending_region ON dw.fact_spending(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_spending_industry ON dw.fact_spending(industry_id);
CREATE INDEX IF NOT EXISTS idx_fact_spending_source ON dw.fact_spending(source_system);
CREATE INDEX IF NOT EXISTS idx_fact_spending_date_region ON dw.fact_spending(date_id, region_id);
CREATE INDEX IF NOT EXISTS idx_fact_spending_date_industry ON dw.fact_spending(date_id, industry_id);
CREATE INDEX IF NOT EXISTS idx_fact_spending_batch ON dw.fact_spending(batch_id);

-- Create trigger for updating timestamps
CREATE TRIGGER update_fact_spending_timestamp
    BEFORE UPDATE ON dw.fact_spending
    FOR EACH ROW
    EXECUTE FUNCTION dw.update_timestamp();

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
    SELECT dw.get_region_id(p_region_name) INTO v_region_id;
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

    -- Insert spending data with conflict handling
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
    COALESCE(d.month_name, TO_CHAR(d.full_date, 'Month')) as month_name,
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
GROUP BY d.year, d.month_name, d.full_date, r.region_name, i.industry_name;

-- Load sample spending data
DO $$
DECLARE
    v_next_batch INTEGER;
BEGIN
    -- Get a batch ID for this load
    SELECT COALESCE(MAX(batch_id), 0) + 1 INTO v_next_batch FROM dw.fact_spending;

    -- Load sample data for Zürich
    PERFORM dw.load_spending_data(20240315, 'Zürich', 'Retail', 1000, 50000.00, v_next_batch);
    PERFORM dw.load_spending_data(20240315, 'Zürich', 'Hospitality', 500, 25000.00, v_next_batch);
    PERFORM dw.load_spending_data(20240315, 'Zürich', 'Transportation', 300, 15000.00, v_next_batch);

    -- Load sample data for Bern
    PERFORM dw.load_spending_data(20240315, 'Bern', 'Retail', 800, 40000.00, v_next_batch);
    PERFORM dw.load_spending_data(20240315, 'Bern', 'Hospitality', 400, 20000.00, v_next_batch);
    PERFORM dw.load_spending_data(20240315, 'Bern', 'Transportation', 200, 10000.00, v_next_batch);

    -- Load sample data for Luzern
    PERFORM dw.load_spending_data(20240315, 'Luzern', 'Retail', 600, 30000.00, v_next_batch);
    PERFORM dw.load_spending_data(20240315, 'Luzern', 'Hospitality', 300, 15000.00, v_next_batch);
    PERFORM dw.load_spending_data(20240315, 'Luzern', 'Transportation', 150, 7500.00, v_next_batch);
    
    RAISE NOTICE 'Loaded sample spending data with batch ID %', v_next_batch;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error loading sample data: %', SQLERRM;
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

-- Analyze tables for better query planning
ANALYZE dw.fact_spending;

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
        d.date_id,
        r.region_id,
        i.industry_id,
        COUNT(*) AS transaction_count,
        SUM(mc.transaction_amount) AS total_amount,
        'mastercard' AS source_system,
        v_batch_id
    FROM 
        data_lake.master_card mc
    JOIN 
        dw.dim_date d ON mc.transaction_date = d.full_date
    JOIN 
        dw.dim_region r ON dw.get_region_id(mc.region_name, 'mastercard') = r.region_id
    JOIN 
        dw.dim_industry i ON mc.industry_category = i.industry_name
    WHERE 
        mc.transaction_date BETWEEN p_date_from AND p_date_to
    GROUP BY 
        d.date_id, r.region_id, i.industry_id;

    -- Get count of inserted/updated records
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
        d.date_id,
        r.region_id,
        i.industry_id,
        COUNT(*) AS transaction_count,
        SUM(iv.spending_amount) AS total_amount,
        'intervista' AS source_system,
        v_batch_id
    FROM 
        data_lake.intervista_raw iv
    JOIN 
        dw.dim_date d ON iv.visit_date = d.full_date
    JOIN 
        dw.dim_region r ON dw.get_region_id(iv.region_name, 'intervista') = r.region_id
    JOIN 
        dw.dim_industry i ON iv.industry_category = i.industry_name
    WHERE 
        iv.visit_date BETWEEN p_date_from AND p_date_to
    GROUP BY 
        d.date_id, r.region_id, i.industry_id;

    -- Get count of inserted/updated records
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
    COUNT(DISTINCT f.batch_id) AS batch_count
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

-- Example of how to execute the loading procedure
-- CALL dw.load_spending_incremental(7);  -- Load last 7 days