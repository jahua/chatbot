-- Create conversation_history table in data_lake schema
CREATE TABLE IF NOT EXISTS data_lake.conversation_history (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) NOT NULL,
    prompt TEXT NOT NULL,
    sql_query TEXT,
    response TEXT,
    schema_context TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    query_metadata JSONB,
    vector_embedding JSONB
);

-- Create index on session_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_conversation_history_session_id ON data_lake.conversation_history(session_id);

-- Create index on created_at for time-based queries
CREATE INDEX IF NOT EXISTS idx_conversation_history_created_at ON data_lake.conversation_history(created_at); 