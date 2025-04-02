import asyncio
from app.services.chat_service import ChatService
from app.models.chat import ChatMessage

async def test_chat():
    try:
        chat_service = ChatService()
        
        # Test message
        message = ChatMessage(
            message="Show me all districts in Ticino",
            session_id="test-session-2"
        )
        
        # Process message
        response = await chat_service.process_message(message)
        
        print("Chat Response:")
        print(f"Success: {response.success}")
        print(f"SQL Query: {response.sql_query}")
        print(f"Response: {response.response}")
        print(f"Error: {response.error}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_chat()) 