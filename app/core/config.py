from pydantic_settings import BaseSettings
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    # API Settings
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Tourism Data Chatbot"
    
    # Database Settings
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "tourism_data"
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    DB_SCHEMA: str = os.getenv("DB_SCHEMA", "data_lake")
    
    # Vector Store Settings
    CHROMA_HOST: str = os.getenv("CHROMA_HOST", "localhost")
    CHROMA_PORT: int = int(os.getenv("CHROMA_PORT", "8000"))
    
    # LLM Settings
    OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "sqlcoder:7b")
    
    # Security Settings
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-here")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
    
    # Gemini API settings
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    GOOGLE_API_KEY: str = os.getenv("GOOGLE_API_KEY", "")
    
    # Anthropic Settings
    ANTHROPIC_API_KEY: str
    
    # LangSmith Settings
    LANGCHAIN_API_KEY: str
    LANGCHAIN_ENDPOINT: str = "https://api.smith.langchain.com"
    LANGCHAIN_PROJECT: str = "tourism_chatbot"
    LANGCHAIN_TRACING_V2: bool = True
    
    class Config:
        case_sensitive = True
        env_file = ".env"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

settings = Settings() 