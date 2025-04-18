from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from app.core.config import settings

# Create base for declarative models
Base = declarative_base()

# Create engine for dw schema
DW_ENGINE = create_engine(
    settings.DW_DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800
)

# Create session factory
DWSession = sessionmaker(
    bind=DW_ENGINE,
    autocommit=False,
    autoflush=False
)

def get_dw_session():
    """Dependency for FastAPI to get a database session"""
    session = DWSession()
    try:
        yield session
    finally:
        session.close() 