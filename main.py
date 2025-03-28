from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.api import chat, query, schema, visualization

app = FastAPI(
    title="Tourism SQL RAG System",
    description="A RAG-enhanced Text-to-SQL system for tourism data analysis",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(chat.router, prefix="/api/chat", tags=["chat"])
app.include_router(query.router, prefix="/api/query", tags=["query"])
app.include_router(schema.router, prefix="/api/schema", tags=["schema"])
app.include_router(visualization.router, prefix="/api/visualization", tags=["visualization"])

@app.get("/")
async def root():
    return {
        "message": "Welcome to Tourism SQL RAG System",
        "version": "1.0.0",
        "status": "operational"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 