-- Create dimension tables for the fact_tourism_unified table

-- Create object_type dimension if it doesn't exist
CREATE TABLE IF NOT EXISTS dw.dim_object_type (
    object_type_id SERIAL PRIMARY KEY,
    object_type_name VARCHAR(100) NOT NULL,
    object_type_description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create data_type dimension if it doesn't exist
CREATE TABLE IF NOT EXISTS dw.dim_data_type (
    data_type_id SERIAL PRIMARY KEY,
    data_type_name VARCHAR(100) NOT NULL,
    data_type_description TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create visit_type dimension if it doesn't exist
CREATE TABLE IF NOT EXISTS dw.dim_visit_type (
    visit_type_id SERIAL PRIMARY KEY,
    visit_type_name VARCHAR(100) NOT NULL,
    visit_type_description TEXT,
    is_domestic BOOLEAN NOT NULL DEFAULT TRUE,
    is_overnight BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_dim_object_type_name ON dw.dim_object_type(object_type_name);
CREATE INDEX IF NOT EXISTS idx_dim_data_type_name ON dw.dim_data_type(data_type_name);
CREATE INDEX IF NOT EXISTS idx_dim_visit_type_name ON dw.dim_visit_type(visit_type_name); 