from sqlalchemy import Column, Integer, String, Date, DateTime, JSON, Boolean, ForeignKey, Text, Float, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

# Define conversation history table separately
conversation_metadata_obj = MetaData()
conversation_history = Table(
    "conversation_history",
    conversation_metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("session_id", String(100), nullable=False),
    Column("prompt", Text, nullable=False),
    Column("sql_query", Text),
    Column("response", Text),
    Column("schema_context", Text),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("query_metadata", JSON),  # Store additional metadata like region, date range, etc.
    Column("vector_embedding", JSON)  # Store vector embedding for semantic search
)

class AOIDay(Base):
    __tablename__ = "aoi_days_raw"
    __table_args__ = {'schema': 'data_lake'}

    id = Column(Integer, primary_key=True)
    aoi_date = Column(Date)
    aoi_id = Column(String)
    visitors = Column(JSON)
    dwelltimes = Column(JSON)
    demographics = Column(JSON)
    overnights_from_yesterday = Column(JSON)
    top_foreign_countries = Column(JSON)
    top_last_cantons = Column(JSON)
    top_last_municipalities = Column(JSON)
    top_swiss_cantons = Column(JSON)
    top_swiss_municipalities = Column(JSON)
    source_system = Column(String)
    load_date = Column(Date)
    ingestion_timestamp = Column(DateTime)
    raw_content = Column(JSON)

class Demographics(Base):
    __tablename__ = "demographics"
    __table_args__ = {'schema': 'data_lake'}

    demographic_id = Column(Integer, primary_key=True)
    demographic_type = Column(String)
    demographic_value = Column(String)
    description = Column(Text)

class GeoinsightsDataRaw(Base):
    __tablename__ = "geoinsights_data_raw"
    __table_args__ = {'schema': 'data_lake'}

    id = Column(Integer, primary_key=True)
    yr = Column(Integer)
    txn_date = Column(Date)
    industry = Column(String)
    segment = Column(String)
    geo_type = Column(String)
    geo_name = Column(String)
    quad_id = Column(Integer)
    central_latitude = Column(Float)
    central_longitude = Column(Float)
    bounding_box = Column(Text)
    txn_amt = Column(Float)
    txn_cnt = Column(Float)
    acct_cnt = Column(Float)
    avg_ticket = Column(Float)
    avg_freq = Column(Float)
    avg_spend_amt = Column(Float)
    yoy_txn_amt = Column(Float)
    yoy_txn_cnt = Column(Float)
    source_system = Column(String)
    load_date = Column(Date)
    ingestion_timestamp = Column(DateTime)

class Regions(Base):
    __tablename__ = "regions"
    __table_args__ = {'schema': 'data_lake'}

    region_id = Column(Integer, primary_key=True)
    region_name = Column(String)
    region_type = Column(String)
    parent_region_id = Column(Integer, ForeignKey('data_lake.regions.region_id'))

class TimePeriods(Base):
    __tablename__ = "time_periods"
    __table_args__ = {'schema': 'data_lake'}

    period_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    month = Column(Integer)
    month_name = Column(String)
    period_start = Column(Date)
    period_end = Column(Date)

class VisitTypes(Base):
    __tablename__ = "visit_types"
    __table_args__ = {'schema': 'data_lake'}

    visit_type_id = Column(Integer, primary_key=True)
    type_name = Column(String)
    description = Column(Text)

class Visitors(Base):
    __tablename__ = "visitors"
    __table_args__ = {'schema': 'data_lake'}

    visitor_id = Column(Integer, primary_key=True)
    period_id = Column(Integer, ForeignKey('data_lake.time_periods.period_id'))
    visitor_count = Column(Integer)
    is_predicted = Column(Boolean)
    demographic_id = Column(Integer, ForeignKey('data_lake.demographics.demographic_id'))
    demographic_count = Column(Integer)
    observation_count = Column(Integer)
    source_file = Column(String)
    ingestion_timestamp = Column(DateTime)

class Visits(Base):
    __tablename__ = "visits"
    __table_args__ = {'schema': 'data_lake'}

    visit_id = Column(Integer, primary_key=True)
    region_id = Column(Integer, ForeignKey('data_lake.regions.region_id'))
    visit_type_id = Column(Integer, ForeignKey('data_lake.visit_types.visit_type_id'))
    period_id = Column(Integer, ForeignKey('data_lake.time_periods.period_id'))
    visit_count = Column(Integer)
    is_predicted = Column(Boolean)
    demographic_id = Column(Integer, ForeignKey('data_lake.demographics.demographic_id'))
    demographic_count = Column(Integer)
    observation_count = Column(Integer)
    source_file = Column(String)
    ingestion_timestamp = Column(DateTime) 