from sqlalchemy import Column, Integer, String, JSON, ForeignKey, Numeric, DateTime, Text, Boolean
from sqlalchemy.sql import func
from app.db.dw_connection import Base

class FactVisitor(Base):
    __tablename__ = 'fact_visitor'
    __table_args__ = {'schema': 'dw'}
    
    fact_id = Column(Integer, primary_key=True)
    date_id = Column(Integer, ForeignKey('dw.dim_date.date_id'), nullable=False)
    region_id = Column(Integer, ForeignKey('dw.dim_region.region_id'), nullable=False)
    total_visitors = Column(Numeric)
    swiss_tourists = Column(Numeric)
    foreign_tourists = Column(Numeric)
    swiss_locals = Column(Numeric)
    foreign_workers = Column(Numeric)
    swiss_commuters = Column(Numeric)
    demographics = Column(JSON)
    dwell_time = Column(JSON)
    top_foreign_countries = Column(JSON)
    top_swiss_cantons = Column(JSON)
    top_municipalities = Column(JSON)
    top_last_cantons = Column(JSON)
    top_last_municipalities = Column(JSON)
    overnights_from_yesterday = Column(JSON)
    transaction_metrics = Column(JSON)
    aoi_id = Column(String(100))
    source_system = Column(Text, nullable=False)
    load_date = Column(DateTime)
    ingestion_timestamp = Column(DateTime, server_default=func.now())
    raw_content = Column(JSON)
    data_quality_metrics = Column(JSON)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

class FactSpending(Base):
    __tablename__ = 'fact_spending'
    __table_args__ = {'schema': 'dw'}
    
    fact_id = Column(Integer, primary_key=True)
    date_id = Column(Integer, ForeignKey('dw.dim_date.date_id'), nullable=False)
    region_id = Column(Integer, ForeignKey('dw.dim_region.region_id'), nullable=False)
    industry_id = Column(Integer, ForeignKey('dw.dim_industry.industry_id'), nullable=False)
    total_spending = Column(Numeric)
    avg_transaction = Column(Numeric)
    geo_latitude = Column(Numeric)
    geo_longitude = Column(Numeric)
    source_system = Column(Text, nullable=False)
    load_date = Column(DateTime)
    ingestion_timestamp = Column(DateTime, server_default=func.now())
    raw_content = Column(JSON)
    data_quality_metrics = Column(JSON)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

class DimRegion(Base):
    __tablename__ = 'dim_region'
    __table_args__ = {'schema': 'dw'}
    
    region_id = Column(Integer, primary_key=True)
    region_name = Column(String(255), nullable=False)
    region_type = Column(String(50), nullable=False)
    parent_region_id = Column(Integer, ForeignKey('dw.dim_region.region_id'))
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

class DimDate(Base):
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'dw'}
    
    date_id = Column(Integer, primary_key=True)
    full_date = Column(DateTime, nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    is_weekend = Column(Boolean, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

class DimIndustry(Base):
    __tablename__ = 'dim_industry'
    __table_args__ = {'schema': 'dw'}
    
    industry_id = Column(Integer, primary_key=True)
    industry_name = Column(Text, nullable=False)
    industry_category = Column(Text)
    description = Column(Text)
    source_system = Column(Text, nullable=False)
    load_date = Column(DateTime)
    ingestion_timestamp = Column(DateTime, server_default=func.now())
    raw_content = Column(JSON)
    data_quality_metrics = Column(JSON)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False) 