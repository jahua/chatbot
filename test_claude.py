from anthropic import Anthropic
from app.core.config import settings

def test_claude_connection():
    try:
        # Initialize Anthropic client
        client = Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        
        # Test message
        message = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=100,
            messages=[{
                "role": "user",
                "content": "Say hello"
            }]
        )
        
        print("Claude API Connection Successful!")
        print("Response:", message.content[0].text)
        return True
        
    except Exception as e:
        print("Error connecting to Claude API:", str(e))
        return False

if __name__ == "__main__":
    test_claude_connection() 