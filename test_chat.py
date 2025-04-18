from app.services.chat_service import ChatService
from app.db.schema_manager import SchemaManager
from app.db.database import get_dw_db
import asyncio

async def test_chat():
    # Get database session
    dw_db = next(get_dw_db())
    
    # Initialize schema manager
    schema_manager = SchemaManager()
    
    # Initialize chat service with dependencies
    chat_service = ChatService(
        dw_db=dw_db,
        schema_manager=schema_manager
    )
    
    # Test the chat service with a query for highest spending industry
    response = await chat_service.process_chat(
        message="Which industry has the highest spending?"
    )
    
    print("Response:", response)
    
    # Clean up
    chat_service.close()

if __name__ == "__main__":
    asyncio.run(test_chat()) 