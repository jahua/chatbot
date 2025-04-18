-- Create tourism visitors fact table
CREATE TABLE IF NOT EXISTS dw.fact_tourism_visitors (
    fact_id SERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES dw.dim_date(date_id),
    region_id INTEGER NOT NULL REFERENCES dw.dim_region(region_id),
    visit_type_id INTEGER NOT NULL REFERENCES dw.dim_visit_type(visit_type_id),
    total_visitors INTEGER NOT NULL,
    age_15_29 INTEGER,
    age_30_44 INTEGER,
    age_45_59 INTEGER,
    age_60_plus INTEGER,
    sex_male INTEGER,
    sex_female INTEGER,
    staydays NUMERIC(5,2),
    origin_D INTEGER,
    origin_F INTEGER,
    origin_I INTEGER,
    canton_AG INTEGER,
    canton_AR INTEGER,
    canton_AI INTEGER,
    canton_BL INTEGER,
    canton_BS INTEGER,
    canton_BE INTEGER,
    canton_FR INTEGER,
    canton_GE INTEGER,
    canton_GL INTEGER,
    canton_GR INTEGER,
    canton_JU INTEGER,
    canton_LU INTEGER,
    canton_NE INTEGER,
    canton_NW INTEGER,
    canton_OW INTEGER,
    canton_SH INTEGER,
    canton_SZ INTEGER,
    canton_SO INTEGER,
    canton_SG INTEGER,
    canton_TG INTEGER,
    canton_UR INTEGER,
    canton_VD INTEGER,
    canton_VS INTEGER,
    canton_ZG INTEGER,
    canton_ZH INTEGER,
    source_keys JSONB,
    data_completeness NUMERIC(5,2) DEFAULT 100.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fact_tourism_visitors_unique_key UNIQUE (date_id, region_id, visit_type_id)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_fact_tourism_visitors_date
ON dw.fact_tourism_visitors(date_id);

CREATE INDEX IF NOT EXISTS idx_fact_tourism_visitors_region
ON dw.fact_tourism_visitors(region_id);

CREATE INDEX IF NOT EXISTS idx_fact_tourism_visitors_completeness
ON dw.fact_tourism_visitors(data_completeness);

-- Create trigger for updating the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_fact_tourism_visitors_updated_at
    BEFORE UPDATE ON dw.fact_tourism_visitors
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

\echo 'Fact table fact_tourism_visitors created successfully.' 