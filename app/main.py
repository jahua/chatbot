from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.services.chat_service import ChatService
from app.core.config import settings
from app.models.chat import ChatMessage

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

# Initialize chat service
chat_service = ChatService()

@app.get("/")
async def root():
    return {"message": "Welcome to Tourism SQL RAG System"}

@app.post("/api/chat/message")
async def process_message(message: ChatMessage):
    try:
        response = await chat_service.process_message(message.message, message.session_id)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/schema")
async def get_schema():
    try:
        schema_info = chat_service.get_schema_summary()
        return {"success": True, "schema": schema_info}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 