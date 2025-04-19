import os
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path, override=True)

# API Configuration
def get_api_url():
    """Get the API URL with proper error handling and defaults"""
    base_url = os.getenv("API_URL", "http://localhost:8000").rstrip('/')
    if not base_url.startswith(('http://', 'https://')):
        base_url = f"http://{base_url}"
    
    # Add API version prefix
    api_prefix = os.getenv("API_V1_STR", "/api/v1").rstrip('/')
    return f"{base_url}{api_prefix}"

API_URL = get_api_url()

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
} 