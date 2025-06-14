import os
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path, override=True)

# API Configuration
def get_api_url():
    """Get the API URL with proper error handling and defaults"""
    base_url = os.getenv("API_URL", "http://localhost:8081").rstrip('/')
    if not base_url.startswith(('http://', 'https://')):
        base_url = f"http://{base_url}"
    return base_url

API_URL = get_api_url()

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
} 