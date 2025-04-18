-- Create the merchant dimension table
CREATE TABLE IF NOT EXISTS edw.dim_merchant (
    merchant_id SERIAL PRIMARY KEY,
    merchant_name VARCHAR(255) NOT NULL,
    normalized_name TEXT,
    industry VARCHAR(100),
    segment VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(merchant_name)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_merchant_name ON edw.dim_merchant(merchant_name);
CREATE INDEX IF NOT EXISTS idx_merchant_normalized ON edw.dim_merchant(normalized_name);
CREATE INDEX IF NOT EXISTS idx_merchant_industry ON edw.dim_merchant(industry);
CREATE INDEX IF NOT EXISTS idx_merchant_segment ON edw.dim_merchant(segment);

-- Create or replace the normalize_merchant_name function
CREATE OR REPLACE FUNCTION edw.normalize_merchant_name(input_text TEXT)
RETURNS TEXT AS $$
BEGIN
    IF input_text IS NULL THEN
        RETURN NULL;
    END IF;

    -- First fix any encoding issues
    input_text := edw.fix_encoding(input_text);
    
    -- Convert to lowercase
    input_text := lower(input_text);
    
    -- Replace umlauts and accents with simple equivalents
    input_text := translate(input_text, 'äàáâãåāăąǎǟǡǻȁȃạảấầẩẫậắằẳẵặ', 'a');
    input_text := translate(input_text, 'ëèéêēĕėęěȅȇẹẻẽếềểễệ', 'e');
    input_text := translate(input_text, 'ïìíîĩīĭįǐȉȋḭḯỉịớờởỡợ', 'i');
    input_text := translate(input_text, 'öòóôõōŏőơǒǫǭȍȏọỏốồổỗộớờởỡợ', 'o');
    input_text := translate(input_text, 'üùúûũūŭůűųưǔǖǘǚǜȕȗụủứừửữự', 'u');
    input_text := translate(input_text, 'ýÿŷȳỳỵỷỹ', 'y');
    input_text := translate(input_text, 'ñńņňṅṇṉṋṅ', 'n');
    
    -- Remove non-alphanumeric characters (except spaces and common business characters)
    input_text := regexp_replace(input_text, '[^a-z0-9 &@#-]', '', 'g');
    
    -- Remove leading/trailing spaces and collapse multiple spaces
    input_text := regexp_replace(trim(input_text), '\s+', ' ', 'g');
    
    RETURN input_text;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create or replace the find_or_create_merchant function
CREATE OR REPLACE FUNCTION edw.find_or_create_merchant(
    in_merchant_name TEXT,
    in_industry TEXT DEFAULT NULL,
    in_segment TEXT DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_merchant_id INTEGER;
    v_normalized_name TEXT;
BEGIN
    -- Input validation
    IF in_merchant_name IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Normalize the merchant name
    v_normalized_name := edw.normalize_merchant_name(in_merchant_name);
    
    -- Try to find existing merchant
    SELECT merchant_id INTO v_merchant_id
    FROM edw.dim_merchant
    WHERE normalized_name = v_normalized_name;
    
    -- If found, update industry and segment if provided
    IF v_merchant_id IS NOT NULL THEN
        IF in_industry IS NOT NULL OR in_segment IS NOT NULL THEN
            UPDATE edw.dim_merchant
            SET 
                industry = COALESCE(in_industry, industry),
                segment = COALESCE(in_segment, segment),
                updated_at = NOW()
            WHERE merchant_id = v_merchant_id;
        END IF;
        RETURN v_merchant_id;
    END IF;
    
    -- If not found, create new merchant
    INSERT INTO edw.dim_merchant (
        merchant_name,
        normalized_name,
        industry,
        segment
    ) VALUES (
        in_merchant_name,
        v_normalized_name,
        in_industry,
        in_segment
    )
    RETURNING merchant_id INTO v_merchant_id;
    
    RETURN v_merchant_id;
END;
$$ LANGUAGE plpgsql; 