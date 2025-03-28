from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_schema_info():
    """Get database schema information for RAG context"""
    db = SessionLocal()
    try:
        # Query to get table information
        tables_query = text("""
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = :schema
        ORDER BY table_name, ordinal_position;
        """)
        
        # Query to get foreign key relationships
        fk_query = text("""
        SELECT
            tc.table_schema, 
            tc.constraint_name, 
            tc.table_name, 
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name 
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = :schema;
        """)
        
        # Execute queries with schema parameter
        tables_result = db.execute(tables_query, {"schema": settings.POSTGRES_SCHEMA}).fetchall()
        fk_result = db.execute(fk_query, {"schema": settings.POSTGRES_SCHEMA}).fetchall()
        
        # Process results
        tables = {}
        for row in tables_result:
            if row.table_name not in tables:
                tables[row.table_name] = []
            tables[row.table_name].append({
                "column_name": row.column_name,
                "data_type": row.data_type,
                "is_nullable": row.is_nullable
            })
            
        foreign_keys = {}
        for row in fk_result:
            if row.table_name not in foreign_keys:
                foreign_keys[row.table_name] = []
            foreign_keys[row.table_name].append({
                "column": row.column_name,
                "references": f"{row.foreign_table_schema}.{row.foreign_table_name}({row.foreign_column_name})"
            })
            
        return {
            "tables": tables,
            "foreign_keys": foreign_keys
        }
    finally:
        db.close() 