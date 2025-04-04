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

  







