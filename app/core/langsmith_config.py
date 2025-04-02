from langsmith import Client
from langsmith.run_helpers import traceable
from langsmith.utils import get_tracing_enabled
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize LangSmith client
langsmith_client = Client()

# Enable tracing
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGCHAIN_API_KEY", "")
os.environ["LANGCHAIN_PROJECT"] = "tourism-chatbot"

def get_traceable_decorator():
    """Get the traceable decorator for LangSmith"""
    return traceable(
        name="tourism_chatbot",
        project_name="tourism-chatbot",
        tags=["tourism", "chatbot"],
        run_type="chain"
    ) 