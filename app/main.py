from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from app.services.chat_service import ChatService
from app.core.config import settings
from app.models.chat import ChatMessage, ChatResponse
from app.db.database import get_db, DatabaseService
from sqlalchemy.orm import Session
from app.llm.claude_adapter import ClaudeAdapter
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_service():
    db = next(get_db())
    db_service = DatabaseService(db)
    try:
        yield db_service
    finally:
        db_service.db.close()

def get_chat_service(db_service: DatabaseService = Depends(get_db_service)):
    # Create database URL
    database_url = f"postgresql://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    
    # Initialize ClaudeAdapter with database URL
    claude_adapter = ClaudeAdapter(database_url)
    
    # Initialize ChatService with dependencies
    chat_service = ChatService(db_service, claude_adapter)
    return chat_service

@app.get("/")
async def root():
    return {"message": "Welcome to the Chatbot API"}

@app.post("/api/chat/message")
async def chat(
    request: ChatMessage,
    chat_service: ChatService = Depends(get_chat_service)
) -> ChatResponse:
    """Process a chat message and return a response"""
    logger.debug(f"Received chat request: message='{request.message}' session_id='{request.session_id}'")
    
    try:
        # Process message with visualization
        response = await chat_service.process_message_with_visualization(
            message=request.message,
            session_id=request.session_id
        )
        
        # Format response according to ChatResponse model
        return ChatResponse(
            message=response.get("message", "No response available"),
            sql_query=response.get("sql_query"),
            results={"data": response.get("data", []), "type": response.get("type", "general")},
            type=response.get("type"),
            plot=response.get("plot"),
            statistics=response.get("statistics"),
            success=True,
            response=response.get("message", "No response available")
        )
        
    except Exception as e:
        logger.error(f"Error processing chat request: {str(e)}")
        return ChatResponse(
            message=f"Error processing chat request: {str(e)}",
            success=False,
            response=f"Error processing chat request: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.get("/api/schema")
async def get_schema(
    chat_service: ChatService = Depends(get_chat_service)
):
    try:
        schema_info = chat_service.get_schema_summary()
        return {"success": True, "schema": schema_info}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 