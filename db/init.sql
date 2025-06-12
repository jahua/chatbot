-- 创建 dw schema
CREATE SCHEMA IF NOT EXISTS dw;

-- 设置搜索路径
SET search_path TO dw, public;

-- 创建必要的扩展
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;

-- 创建维度表
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_id BIGINT PRIMARY KEY,
    full_date DATE,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    month INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dim_region (
    region_id BIGINT PRIMARY KEY,
    region_name VARCHAR(255),
    region_type VARCHAR(50),
    parent_region_id BIGINT,
    population INTEGER,
    area_sqkm FLOAT
);

CREATE TABLE IF NOT EXISTS dw.dim_visitor_segment (
    segment_id BIGINT PRIMARY KEY,
    segment_name VARCHAR(100),
    is_domestic BOOLEAN,
    is_overnight BOOLEAN,
    segment_description TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_spending_industry (
    industry_id BIGINT PRIMARY KEY,
    industry_name VARCHAR(255),
    sector VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dw.dim_spending_category (
    category_id BIGINT PRIMARY KEY,
    category_name VARCHAR(255),
    parent_category_id BIGINT
);

-- 创建事实表
CREATE TABLE IF NOT EXISTS dw.fact_visitor (
    visitor_id BIGINT,
    date_id BIGINT REFERENCES dw.dim_date(date_id),
    region_id BIGINT REFERENCES dw.dim_region(region_id),
    segment_id BIGINT REFERENCES dw.dim_visitor_segment(segment_id),
    visitor_count INTEGER,
    demographics JSONB,
    PRIMARY KEY (visitor_id)
);

CREATE TABLE IF NOT EXISTS dw.fact_spending (
    spending_id BIGINT,
    date_id BIGINT REFERENCES dw.dim_date(date_id),
    region_id BIGINT REFERENCES dw.dim_region(region_id),
    industry_id BIGINT REFERENCES dw.dim_spending_industry(industry_id),
    category_id BIGINT REFERENCES dw.dim_spending_category(category_id),
    total_amount DECIMAL,
    transaction_count INTEGER,
    segment VARCHAR(100),
    PRIMARY KEY (spending_id)
); 