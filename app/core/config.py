from pydantic_settings import BaseSettings
from typing import Optional
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Settings(BaseSettings):
    PROJECT_NAME: str = "Tourism Data Analysis Chatbot"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    
    # API URL (for frontend use)
    API_URL: Optional[str] = os.getenv("API_URL", "http://localhost:8081")
    
    # Database settings
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")  # Default PostgreSQL port
    DB_SCHEMA: str = os.getenv("DB_SCHEMA", "public")  # Default PostgreSQL schema
    
    # Vector Store Settings
    CHROMA_HOST: str = os.getenv("CHROMA_HOST", "localhost")
    CHROMA_PORT: int = int(os.getenv("CHROMA_PORT", "8000"))
    
    # OpenAI Settings
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_API_BASE: str = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4")
    
    # LLM Settings
    LLM_API_TIMEOUT: int = int(os.getenv("LLM_API_TIMEOUT", "45"))  # Timeout for LLM API calls in seconds
    
    # Security Settings
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
    
    # LangChain Settings
    LANGCHAIN_PROJECT: str = os.getenv("LANGCHAIN_PROJECT", "default")
    LANGCHAIN_TRACING_V2: bool = os.getenv("LANGCHAIN_TRACING_V2", "false").lower() == "true"
    
    # DW Database settings
    DW_DATABASE_URL: str = "postgresql://postgres:336699@3.76.40.121:5432/trip_dw"
    DW_POOL_SIZE: int = 5
    DW_MAX_OVERFLOW: int = 10
    DW_POOL_TIMEOUT: int = 30
    DW_POOL_RECYCLE: int = 1800
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    class Config:
        case_sensitive = True
        env_file = ".env"

settings = Settings() 