from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.schemas.chat import ChatRequest, ChatResponse
from app.services.chat_service import ChatService
from app.db.database import DatabaseService
from app.llm.openai_adapter import OpenAIAdapter
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="Tourism Data Analysis Chatbot")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global service instances
chat_service = None
openai_adapter = None
db_service = None

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    try:
        global chat_service, openai_adapter, db_service
        
        # Initialize OpenAI adapter
        openai_adapter = OpenAIAdapter()
        logger.info("OpenAI adapter initialized")
        
        # Initialize database service
        db_service = DatabaseService()
        logger.info("Database service initialized")
        
        # Initialize chat service with dependencies
        chat_service = ChatService(openai_adapter=openai_adapter, db=db_service)
        logger.info("Chat service initialized")
        
    except Exception as e:
        logger.error(f"Error initializing services: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup services on shutdown"""
    try:
        global chat_service
        if chat_service:
            chat_service.close()
            logger.info("Chat service closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")

@app.post("/chat")
async def chat(request: ChatRequest) -> ChatResponse:
    """Process chat messages"""
    try:
        if not chat_service:
            raise HTTPException(status_code=503, detail="Chat service not initialized")
            
        response = await chat_service.process_message(request.message)
        return ChatResponse(**response)
        
    except Exception as e:
        logger.error(f"Error processing chat request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"} 