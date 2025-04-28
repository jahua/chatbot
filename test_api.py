#!/usr/bin/env python3
import requests
import json
import sys

def test_chat_api(message):
    """Test the chat API with a specific message"""
    url = "http://localhost:8080/chat"
    headers = {"Content-Type": "application/json"}
    data = {"message": message}
    
    print(f"Testing API with message: '{message}'")
    print(f"Sending request to {url}...")
    
    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        
        # Print response status
        print(f"Response status: {response.status_code}")
        
        # Parse and print the response
        result = response.json()
        print("Response content:")
        print(json.dumps(result, indent=2))
        
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return False

def test_streaming_api(message):
    """Test the streaming chat API with a specific message"""
    url = "http://localhost:8080/chat/stream"
    headers = {"Content-Type": "application/json"}
    data = {"message": message}
    
    print(f"Testing streaming API with message: '{message}'")
    print(f"Sending request to {url}...")
    
    try:
        response = requests.post(url, headers=headers, json=data, stream=True)
        response.raise_for_status()
        
        # Print response status
        print(f"Response status: {response.status_code}")
        print("Streaming response:")
        
        # Process the stream
        for line in response.iter_lines():
            if line:
                # Skip the "data: " prefix
                if line.startswith(b'data: '):
                    line = line[6:]
                    
                try:
                    # Parse and print the JSON chunk
                    chunk = json.loads(line)
                    
                    # Check for SQL query
                    if chunk.get("type") == "sql_query":
                        print("\nSQL Query:")
                        print(chunk.get("sql_query"))
                    
                    # Check for result type
                    if chunk.get("type") == "visualization":
                        print("\nVisualization detected:")
                        viz_type = chunk.get("visualization", {}).get("type")
                        print(f"Visualization type: {viz_type}")
                    
                    # Check for content
                    if chunk.get("type") == "content":
                        print("\nContent:")
                        print(chunk.get("content"))
                    
                    # Check for end
                    if chunk.get("type") == "end":
                        print("\nEnd of stream")
                        
                except json.JSONDecodeError:
                    print(f"Invalid JSON: {line}")
        
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    # Default message
    message = "Can you visualize the Swiss tourists and international tourists per month in a bar chart?"
    
    # Use command line argument if provided
    if len(sys.argv) > 1:
        message = sys.argv[1]
    
    # Test the API
    print("Testing non-streaming API...")
    test_chat_api(message)
    
    print("\nTesting streaming API...")
    test_streaming_api(message) 