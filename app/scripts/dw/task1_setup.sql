-- Tourism Data Warehouse Core Setup
-- This script establishes the essential dimension and fact tables
-- with proper constraints and performance optimizations

-- Create the main schema
CREATE SCHEMA IF NOT EXISTS dw;

-- =============================================
-- 1. CORE DIMENSION TABLES
-- =============================================

-- Region dimension with proper constraints
CREATE TABLE IF NOT EXISTS dw.dim_region (
    region_id SERIAL PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL,
    region_type VARCHAR(50) NOT NULL CHECK (region_type IN ('canton', 'tourism_region', 'district')),
    parent_region_id INTEGER REFERENCES dw.dim_region(region_id),
    country_code CHAR(2),
    canton_code CHAR(2),
    population INTEGER,
    area_sqkm NUMERIC(10,2),
    is_active BOOLEAN DEFAULT TRUE,
    valid_from DATE DEFAULT CURRENT_DATE,
    valid_to DATE DEFAULT '9999-12-31',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_region_name_type UNIQUE (region_name, region_type)
);

-- Region name variants with proper indexing
CREATE TABLE IF NOT EXISTS dw.dim_region_mapping (
    mapping_id SERIAL PRIMARY KEY,
    region_id INTEGER NOT NULL REFERENCES dw.dim_region(region_id),
    variant_name VARCHAR(100) NOT NULL,
    variant_type VARCHAR(20) NOT NULL CHECK (variant_type IN ('canonical', 'english', 'german')),
    source_system VARCHAR(50) NOT NULL CHECK (source_system IN ('aoi', 'intervista', 'mastercard')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (variant_name, source_system)
);

-- Date dimension with proper date handling
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    week INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    season VARCHAR(10) NOT NULL,
    CONSTRAINT chk_date_id CHECK (date_id BETWEEN 19000101 AND 21000101),
    CONSTRAINT chk_quarter CHECK (quarter BETWEEN 1 AND 4),
    CONSTRAINT chk_month CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT chk_week CHECK (week BETWEEN 1 AND 53),
    CONSTRAINT chk_day CHECK (day BETWEEN 1 AND 31),
    CONSTRAINT chk_day_of_week CHECK (day_of_week BETWEEN 1 AND 7),
    CONSTRAINT chk_season CHECK (season IN ('Winter', 'Spring', 'Summer', 'Fall'))
);

-- Visitor type dimension with proper categorization
CREATE TABLE IF NOT EXISTS dw.dim_visitor_type (
    visitor_type_id SERIAL PRIMARY KEY,
    visitor_code VARCHAR(20) NOT NULL UNIQUE,
    visitor_name VARCHAR(100) NOT NULL,
    visitor_category VARCHAR(50) NOT NULL,
    visitor_subcategory VARCHAR(50),
    is_domestic BOOLEAN NOT NULL,
    is_overnight BOOLEAN NOT NULL,
    is_business BOOLEAN NOT NULL,
    intervista_category_name VARCHAR(100),
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    valid_from DATE DEFAULT CURRENT_DATE,
    valid_to DATE DEFAULT '9999-12-31',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Industry dimension with proper categorization
CREATE TABLE IF NOT EXISTS dw.dim_industry (
    industry_id SERIAL PRIMARY KEY,
    industry_code VARCHAR(20) NOT NULL UNIQUE,
    industry_name VARCHAR(100) NOT NULL,
    industry_category VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- 2. FACT TABLES
-- =============================================

-- Visitor fact table with proper constraints
CREATE TABLE IF NOT EXISTS dw.fact_visitors (
    fact_id SERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES dw.dim_date(date_id),
    region_id INTEGER NOT NULL REFERENCES dw.dim_region(region_id),
    visitor_type_id INTEGER NOT NULL REFERENCES dw.dim_visitor_type(visitor_type_id),
    visitor_count INTEGER NOT NULL CHECK (visitor_count >= 0),
    source_system VARCHAR(50) NOT NULL CHECK (source_system IN ('aoi', 'intervista')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date_id, region_id, visitor_type_id, source_system)
);

-- Spending fact table with proper constraints
CREATE TABLE IF NOT EXISTS dw.fact_spending (
    fact_id SERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES dw.dim_date(date_id),
    region_id INTEGER NOT NULL REFERENCES dw.dim_region(region_id),
    industry_id INTEGER NOT NULL REFERENCES dw.dim_industry(industry_id),
    transaction_count INTEGER NOT NULL CHECK (transaction_count >= 0),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    source_system VARCHAR(50) NOT NULL CHECK (source_system = 'mastercard'),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date_id, region_id, industry_id, source_system)
);

-- =============================================
-- 3. PERFORMANCE OPTIMIZATIONS
-- =============================================

-- Region indexes for common queries
CREATE INDEX IF NOT EXISTS idx_region_parent ON dw.dim_region(parent_region_id);
CREATE INDEX IF NOT EXISTS idx_region_type ON dw.dim_region(region_type);
CREATE INDEX IF NOT EXISTS idx_region_name ON dw.dim_region(region_name);
CREATE INDEX IF NOT EXISTS idx_region_active ON dw.dim_region(is_active);
CREATE INDEX IF NOT EXISTS idx_region_country ON dw.dim_region(country_code);
CREATE INDEX IF NOT EXISTS idx_region_canton ON dw.dim_region(canton_code);

-- Region mapping indexes for lookups
CREATE INDEX IF NOT EXISTS idx_region_mapping_region ON dw.dim_region_mapping(region_id);
CREATE INDEX IF NOT EXISTS idx_region_mapping_variant ON dw.dim_region_mapping(variant_name);
CREATE INDEX IF NOT EXISTS idx_region_mapping_source ON dw.dim_region_mapping(source_system);

-- Date indexes for temporal queries
CREATE INDEX IF NOT EXISTS idx_date_full ON dw.dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_date_year_month ON dw.dim_date(year, month);
CREATE INDEX IF NOT EXISTS idx_date_quarter ON dw.dim_date(quarter);

-- Visitor fact indexes for common aggregations
CREATE INDEX IF NOT EXISTS idx_fact_visitors_date ON dw.fact_visitors(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_visitors_region ON dw.fact_visitors(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_visitors_type ON dw.fact_visitors(visitor_type_id);
CREATE INDEX IF NOT EXISTS idx_fact_visitors_source ON dw.fact_visitors(source_system);

-- Spending fact indexes for common aggregations
CREATE INDEX IF NOT EXISTS idx_fact_spending_date ON dw.fact_spending(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_spending_region ON dw.fact_spending(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_spending_industry ON dw.fact_spending(industry_id);
CREATE INDEX IF NOT EXISTS idx_fact_spending_source ON dw.fact_spending(source_system);

-- =============================================
-- 4. ESSENTIAL FUNCTIONS
-- =============================================

-- Function to get region ID with proper error handling
CREATE OR REPLACE FUNCTION dw.get_region_id(
    p_region_name VARCHAR,
    p_region_type VARCHAR DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_region_id INTEGER;
BEGIN
    SELECT region_id INTO v_region_id
    FROM dw.dim_region
    WHERE region_name = p_region_name
    AND (p_region_type IS NULL OR region_type = p_region_type)
    AND is_active = TRUE;
    
    IF v_region_id IS NULL THEN
        RAISE EXCEPTION 'Region not found: % (type: %)', p_region_name, p_region_type;
    END IF;
    
    RETURN v_region_id;
END;
$$ LANGUAGE plpgsql;

-- Function to add region variants with proper error handling
CREATE OR REPLACE FUNCTION dw.add_region_variants(
    p_region_id INTEGER,
    p_variant_name VARCHAR,
    p_variant_type VARCHAR,
    p_source_system VARCHAR
) RETURNS VOID AS $$
BEGIN
    -- Check if region exists
    IF NOT EXISTS (SELECT 1 FROM dw.dim_region WHERE region_id = p_region_id AND is_active = TRUE) THEN
        RAISE EXCEPTION 'Invalid region_id: %', p_region_id;
    END IF;
    
    -- Validate variant type
    IF p_variant_type NOT IN ('canonical', 'english', 'german') THEN
        RAISE EXCEPTION 'Invalid variant_type: %', p_variant_type;
    END IF;
    
    -- Validate source system
    IF p_source_system NOT IN ('aoi', 'intervista', 'mastercard') THEN
        RAISE EXCEPTION 'Invalid source_system: %', p_source_system;
    END IF;
    
    INSERT INTO dw.dim_region_mapping (
        region_id,
        variant_name,
        variant_type,
        source_system
    ) VALUES (
        p_region_id,
        p_variant_name,
        p_variant_type,
        p_source_system
    )
    ON CONFLICT (variant_name, source_system) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Task 1: Initial Setup
-- This script sets up the basic infrastructure for the data warehouse

-- Create ETL metadata tracking table
CREATE TABLE IF NOT EXISTS dw.etl_metadata (
    etl_id SERIAL PRIMARY KEY,
    task_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    message TEXT,
    start_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP WITH TIME ZONE,
    duration INTERVAL GENERATED ALWAYS AS (end_time - start_time) STORED,
    created_by VARCHAR(100) DEFAULT CURRENT_USER,
    CONSTRAINT chk_status CHECK (status IN ('started', 'completed', 'failed', 'warning'))
);

-- Function to report ETL status
CREATE OR REPLACE FUNCTION dw.report_status(message TEXT)
RETURNS TEXT AS $$
BEGIN
    INSERT INTO dw.etl_metadata (
        task_name,
        status,
        message,
        start_time
    ) VALUES (
        current_setting('app.current_task', true),
        'completed',
        message,
        CURRENT_TIMESTAMP
    );
    
    RAISE NOTICE '%', message;
    RETURN message;
END;
$$ LANGUAGE plpgsql;

-- Create views for data quality monitoring
CREATE OR REPLACE VIEW dw.vw_region_hierarchy AS
SELECT 
    r.region_id,
    r.region_name,
    r.region_type,
    p.region_name AS parent_region_name,
    p.region_type AS parent_region_type,
    r.country_code,
    r.canton_code
FROM dw.dim_region r
LEFT JOIN dw.dim_region p ON r.parent_region_id = p.region_id
WHERE r.is_active = TRUE;

CREATE OR REPLACE VIEW dw.vw_visitor_type_summary AS
SELECT 
    visitor_category,
    COUNT(*) AS type_count,
    COUNT(*) FILTER (WHERE is_domestic) AS domestic_count,
    COUNT(*) FILTER (WHERE is_overnight) AS overnight_count,
    COUNT(*) FILTER (WHERE is_business) AS business_count
FROM dw.dim_visitor_type
WHERE is_active = TRUE
GROUP BY visitor_category;

-- Set the current task name for logging
SET app.current_task = 'task1_setup';

-- Log completion
SELECT dw.report_status('Task 1: Initial setup completed successfully');