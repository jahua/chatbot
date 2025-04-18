import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List, Optional
import requests
from datetime import datetime
import uuid
import os
import logging
import traceback
import time
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import base64
from io import BytesIO
from PIL import Image
from dotenv import load_dotenv
import sseclient  # For server-sent events

# --- Load .env ONCE at the very top ---
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(dotenv_path):
    print(f"DEBUG app.py: Loading .env from: {dotenv_path}")
    # Use override=True to ensure .env values take precedence over environment variables
    load_dotenv(dotenv_path=dotenv_path, override=True)
    print(f"DEBUG app.py: Loaded .env. Checking POSTGRES_HOST: {os.getenv('POSTGRES_HOST')}")
else:
    print(f"DEBUG app.py: Warning - .env file not found at: {dotenv_path}")
# ---------------------------------------

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# --- Initialize session state ONCE --- 
if 'db_config' not in st.session_state:
    print("DEBUG app.py: Initializing st.session_state.db_config...")
    st.session_state.db_config = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD")
    }
    # Check if loading worked
    if not all(st.session_state.db_config.get(key) for key in ["host", "port", "dbname", "user", "password"]):
         print(f"DEBUG app.py: Warning - Some DB config values are missing after loading .env. Config: {st.session_state.db_config}")
    else:
        print("DEBUG app.py: DB config appears loaded into session state.")
else:
    print("DEBUG app.py: st.session_state.db_config already exists.")
# ---------------------------------------

# -- Initialize other session states ONCE --
if "messages" not in st.session_state:
    st.session_state.messages = []
if "current_chat_id" not in st.session_state:
    st.session_state.current_chat_id = str(uuid.uuid4())
if "chat_history" not in st.session_state:
    st.session_state.chat_history = {}
if "conversations" not in st.session_state:
    st.session_state.conversations = []
if "current_conversation_index" not in st.session_state:
    st.session_state.current_conversation_index = 0
if "processing" not in st.session_state:
    st.session_state.processing = False
if "last_request_id" not in st.session_state:
    st.session_state.last_request_id = None
if "processed_queries" not in st.session_state:
    st.session_state.processed_queries = set()
# -----------------------------------------

# --- Define process_query function EARLY ---
def process_query(query: str, use_streaming: bool = True):
    """Process a query and update the chat interface"""
    if not query:
        return

    # Check if we've already processed this query in this session
    if query in st.session_state.processed_queries:
        logger.debug(f"Skipping already processed query: {query}")
        return
    
    # Add to processed queries set immediately to prevent duplicate processing
    st.session_state.processed_queries.add(query)
    
    # Generate a unique request ID to track this specific request
    request_id = str(uuid.uuid4())
    st.session_state.last_request_id = request_id
        
    # Add user message
    st.session_state.messages.append({"role": "user", "content": query, "request_id": request_id})
    st.session_state.processing = True
    
    # Use streaming if enabled
    if use_streaming:
        # Hand off to streaming function
        process_streaming_query(query, request_id)
    else:
        # Use the original non-streaming implementation
        try:
            logger.debug(f"Sending query to API: {query}")
            # Make API call
            response = requests.post(
                "http://localhost:8000/chat",
                json={"message": query},
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    logger.debug(f"Received response for request {request_id}")
                    
                    # Add bot response to chat
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response_data.get("response", "Sorry, I couldn't generate a response."),
                        "visualization": response_data.get("visualization"),
                        "sql_query": response_data.get("sql_query"),
                        "debug_info": response_data.get("debug_info"),
                        "request_id": request_id
                    })
                    
                except json.JSONDecodeError as e:
                    st.error(f"Error parsing response: {str(e)}")
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": "Sorry, I encountered an error processing the response.",
                        "request_id": request_id
                    })
            else:
                st.error(f"Error: Server returned status code {response.status_code}")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"Sorry, there was an error processing your request. Status code: {response.status_code}",
                    "request_id": request_id
                })
        except requests.RequestException as e:
            st.error(f"Error connecting to server: {str(e)}")
            st.session_state.messages.append({
                "role": "assistant",
                "content": "Sorry, I couldn't connect to the server. Please try again later.",
                "request_id": request_id
            })
        finally:
            st.session_state.processing = False
            # Force a rerun to update the UI immediately
            st.rerun()
# --- END Define process_query function ---

# --- Define streaming process function ---
def process_streaming_query(query: str, request_id: str):
    """Process a query with streaming and update the chat interface incrementally"""
    logger.debug(f"Starting streaming query: {query}")
    
    # Create a placeholder for assistant message
    assistant_message = {
        "role": "assistant",
        "content": "",
        "is_streaming": True,  # Flag to indicate this is being streamed
        "request_id": request_id,
        "visualization": None,
        "sql_query": None,
        "debug_info": None
    }
    
    # Add initial placeholder to messages
    st.session_state.messages.append(assistant_message)
    
    try:
        # Start streaming request with a timeout to prevent hanging
        with requests.post(
            "http://localhost:8000/chat/stream",
            json={"message": query},
            headers={"Content-Type": "application/json"},
            stream=True,
            timeout=60  # 60 second timeout
        ) as response:
            
            if response.status_code != 200:
                logger.error(f"Error: Server returned status code {response.status_code}")
                st.session_state.messages[-1]["content"] = f"Sorry, there was an error processing your request. Status code: {response.status_code}"
                st.session_state.messages[-1]["is_streaming"] = False
                st.session_state.processing = False
                st.rerun()
                return
            
            # Parse the SSE stream
            client = sseclient.SSEClient(response)
            
            # Process streaming response
            for event in client.events():
                try:
                    data = json.loads(event.data)
                    logger.debug(f"Received streaming chunk: {data.get('type')}")
                    
                    # Update the message based on the chunk type
                    if data.get("type") == "content":
                        # Append content to existing message
                        st.session_state.messages[-1]["content"] += data.get("content", "")
                        # Force UI update
                        st.rerun()
                        
                    elif data.get("type") == "sql_query":
                        # Set SQL query
                        st.session_state.messages[-1]["sql_query"] = data.get("sql_query")
                        
                    elif data.get("type") == "visualization":
                        # Store visualization data
                        vis_data = data.get("visualization", {})
                        st.session_state.messages[-1]["visualization"] = vis_data
                        
                        # Handle visualization display based on type
                        vis_type = vis_data.get("type")
                        
                        if vis_type == "table":
                            # Convert data to pandas DataFrame and display
                            table_data = vis_data.get("data", [])
                            if table_data:
                                df = pd.DataFrame(table_data)
                                st.dataframe(df)
                        elif vis_type == "image":
                            try:
                                # Extract base64 data - handle both formats
                                image_data = vis_data.get("data", "")
                                if "base64," in image_data:
                                    # Extract the actual base64 part if it includes the data URI prefix
                                    b64_data = image_data.split("base64,")[1]
                                else:
                                    b64_data = image_data
                                    
                                # Decode base64 and display the image
                                image_bytes = base64.b64decode(b64_data)
                                image = Image.open(BytesIO(image_bytes))
                                st.image(image)
                            except Exception as e:
                                st.error(f"Error displaying image visualization: {str(e)}")
                                logger.error(f"Error displaying image: {str(e)}")
                                debug_info["error"] = str(e)
                        else:
                            # Default handling for other visualization types
                            st.json(vis_data)
                            
                        # Force UI update
                        st.rerun()
                        
                    elif data.get("type") == "content_chunk":
                        # Handle content chunks (alternative format)
                        st.session_state.messages[-1]["content"] += data.get("content_chunk", "")
                        # Force UI update
                        st.rerun()
                        
                    elif data.get("type") == "debug_info":
                        # Store debug info for later use
                        st.session_state.messages[-1]["debug_info"] = data.get("debug_info", {})
                        # Also store at session level for easy access
                        st.session_state.debug_info = data.get("debug_info", {})
                        
                    elif data.get("type") == "error":
                        # Handle error messages
                        error_msg = data.get("error", "An unknown error occurred")
                        st.session_state.messages[-1]["content"] += f"\n\nError: {error_msg}"
                        # Force UI update
                        st.rerun()
                        
                    elif data.get("type") == "end":
                        # End of stream, mark streaming as complete
                        st.session_state.messages[-1]["is_streaming"] = False
                        st.session_state.processing = False
                        # Force final UI update
                        st.rerun()
                        return
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing SSE event: {str(e)}")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error processing SSE event: {str(e)}")
                    continue
                    
    except requests.RequestException as e:
        logger.error(f"Error with streaming request: {str(e)}")
        st.session_state.messages[-1]["content"] = f"Sorry, I encountered an error: {str(e)}"
        st.session_state.messages[-1]["is_streaming"] = False
        st.session_state.processing = False
        st.rerun()
        
    except Exception as e:
        logger.error(f"Unexpected error in streaming process: {str(e)}")
        st.session_state.messages[-1]["content"] = f"Sorry, I encountered an unexpected error: {str(e)}"
        st.session_state.messages[-1]["is_streaming"] = False 
        st.session_state.processing = False
        st.rerun()

def display_rag_flow(steps: List[Dict[str, Any]], visualization: Optional[str] = None):
    """Display RAG flow information in a expandable section"""
    with st.expander("How this response was generated", expanded=False):
        cols = st.columns([1, 4])
        
        for i, step in enumerate(steps):
            step_name = step.get("name", f"Step {i+1}")
            step_description = step.get("description", "")
            step_success = step.get("success", True)
            
            # Create indicator for step success/failure
            with cols[0]:
                if step_success:
                    st.markdown(f"✅ **{step_name}**")
                else:
                    st.markdown(f"❌ **{step_name}**")
            
            # Create description and details
            with cols[1]:
                st.markdown(step_description)
                
                # If step has details, show them
                if "details" in step and step["details"]:
                    with st.expander("Details", expanded=False):
                        for key, value in step["details"].items():
                            st.markdown(f"**{key}:** {value}")
        
        # If visualization is provided, show it
        if visualization:
            try:
                # Decode and display image
                image_bytes = base64.b64decode(visualization)
                debug_info["image_bytes_len"] = len(image_bytes)
                st.image(image_bytes)
            except Exception as e:
                st.error(f"Error displaying image: {str(e)}")
                logger.error(f"Error displaying image: {str(e)}")
                debug_info["error"] = str(e)

def display_message(message):
    """Display a single message in the chat interface"""
    if message["role"] == "user":
        st.chat_message("user").write(message["content"])
    else:
        with st.chat_message("assistant"):
            if message.get("content"):
                st.write(message["content"])
            
            # Display SQL query if available
            if message.get("sql_query"):
                with st.expander("SQL Query", expanded=False):
                    st.code(message["sql_query"], language="sql")
            
            # Display visualization if available
            if message.get("visualization"):
                vis_data = message["visualization"]
                
                # Check the type of visualization
                vis_type = vis_data.get("type", "")
                
                if vis_type == "table":
                    # If it's table data, display as dataframe
                    try:
                        table_data = vis_data.get("data", [])
                        if table_data:
                            df = pd.DataFrame(table_data)
                            st.dataframe(df, use_container_width=True)
                    except Exception as e:
                        st.error(f"Error displaying table visualization: {str(e)}")
                
                elif vis_type == "image":
                    # If it's an image, display it
                    try:
                        # Handle base64 image data
                        image_data = vis_data.get("data", "")
                        
                        # Check if it's a data URI or just base64
                        if "base64," in image_data:
                            # Extract the actual base64 part
                            b64_data = image_data.split("base64,")[1]
                        else:
                            b64_data = image_data
                            
                        # Decode and display
                        image_bytes = base64.b64decode(b64_data)
                        image = Image.open(BytesIO(image_bytes))
                        st.image(image, use_column_width=True)
                    except Exception as e:
                        st.error(f"Error displaying image visualization: {str(e)}")
                        logger.error(f"Visualization error: {str(e)}", exc_info=True)
                        
                else:
                    # For other types, show as JSON
                    st.json(vis_data)
            
            # Show a debug option for detailed flow information
            if message.get("debug_info"):
                debug_data = message["debug_info"]
                with st.expander("Debug Info", expanded=False):
                    
                    # Show flow metadata
                    if "message_id" in debug_data:
                        st.markdown(f"**Message ID:** {debug_data['message_id']}")
                    if "flow_id" in debug_data:
                        st.markdown(f"**Flow ID:** {debug_data['flow_id']}")
                    
                    # Show timing information if available
                    if "timings" in debug_data and debug_data["timings"]:
                        st.markdown("### Timing")
                        timing_df = pd.DataFrame([
                            {"Step": step, "Time (s)": time}
                            for step, time in debug_data["timings"].items()
                        ])
                        st.dataframe(timing_df)
                    
                    # Show steps information with status
                    if "steps" in debug_data and debug_data["steps"]:
                        st.markdown("### Processing Steps")
                        
                        # Extract step info
                        steps_info = []
                        for step in debug_data["steps"]:
                            step_info = {
                                "Step": step.get("name", "Unknown"),
                                "Success": "✅" if step.get("success") else "❌",
                                "Duration (s)": step.get("duration", "-")
                            }
                            steps_info.append(step_info)
                        
                        # Display steps as dataframe
                        steps_df = pd.DataFrame(steps_info)
                        st.dataframe(steps_df)
                        
                        # Show detailed step info if we have errors
                        for step in debug_data["steps"]:
                            if not step.get("success"):
                                st.markdown(f"**Error in {step.get('name', 'Unknown')}:** {step.get('error', 'Unknown error')}")
                    
                    # Show raw data if user wants it
                    if st.checkbox("Show Raw Debug Data", value=False):
                        st.json(debug_data)

# Sidebar for chat history and settings
with st.sidebar:
    st.title("💬 Chat History")
    
    # New chat button
    if st.button("🆕 New Chat"):
        # Save current conversation if it exists
        if st.session_state.messages:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            first_msg = next((msg["content"] for msg in st.session_state.messages if msg["role"] == "user"), "New Chat")
            st.session_state.conversations.append({
                "timestamp": timestamp,
                "title": first_msg[:30] + "..." if len(first_msg) > 30 else first_msg,
                "messages": st.session_state.messages.copy()
            })
        # Clear current messages
        st.session_state.messages = []
        st.session_state.current_conversation_index = len(st.session_state.conversations)
        st.session_state.processed_queries = set()
    
    # Display saved conversations
    for i, conv in enumerate(st.session_state.conversations):
        if st.button(f"📝 {conv['timestamp']}: {conv['title']}", key=f"conv_{i}"):
            st.session_state.messages = conv["messages"].copy()
            st.session_state.current_conversation_index = i
            st.session_state.processed_queries = set()  # Reset processed queries when switching conversations
    
    # Clear all chats button
    if st.button("🗑️ Clear All Chats"):
        st.session_state.messages = []
        st.session_state.conversations = []
        st.session_state.current_conversation_index = 0
        st.session_state.processed_queries = set()
    
    st.markdown("---")
    st.markdown("### Settings")
    st.session_state.model = st.selectbox(
        "Select Model",
        ["claude"],
        index=0
    )
    
    # Add a debug mode toggle
    if st.checkbox("Debug Mode", value=False):
        st.write("Session State:")
        st.write({
            "messages_count": len(st.session_state.messages),
            "conversations_count": len(st.session_state.conversations),
            "current_index": st.session_state.current_conversation_index,
            "processing": st.session_state.processing,
            "processed_queries": list(st.session_state.processed_queries)[:5] + ["..."] if len(st.session_state.processed_queries) > 5 else list(st.session_state.processed_queries)
        })
    
    st.markdown("---")
    st.markdown("### About")
    st.markdown("""
    This chatbot helps you explore tourism data in Switzerland.
    You can ask questions about visitor patterns, demographics, and more.
    """)

# The main function that defines the application
def main():
    """Main function to run the Streamlit application"""
    st.title("Tourism Data Insights Chatbot 💬")
    
    # Sidebar for configuration and examples
    with st.sidebar:
        st.markdown("### Example questions:")
        example_questions = [
            # New questions from the user
            "What was the busiest week in spring 2023?",
            "What day had the most visitors in 2023?", 
            "Compare Swiss vs foreign tourists in April 2023",
            "Which industry had the highest spending?",
            "Show visitor counts for top 3 days in summer 2023"
        ]
        
        for i, q in enumerate(example_questions):
            # Use a more unique key with a random prefix
            if st.button(q, key=f"main_sidebar_example_{i}"):
                # Process the query directly, the duplicate check happens in process_query
                process_query(q)

    # Display chat messages
    for message in st.session_state.messages:
        display_message(message)

    # Show loading animation when processing
    if st.session_state.processing:
        st.markdown('<div class="chat-message bot-message"><div class="message-content">🤖 Analyzing<span class="loading"></span></div></div>', unsafe_allow_html=True)

    # Chat input using form
    with st.form(key="main_chat_form", clear_on_submit=True):
        cols = st.columns([7, 2, 1])
        with cols[0]:
            user_input = st.text_input("Ask a question about tourism data:", 
                                      key="main_chat_input",
                                      placeholder="Type your question here...",
                                      label_visibility="collapsed")
        with cols[1]:
            submit_button = st.form_submit_button("Send")
        with cols[2]:
            clear_button = st.form_submit_button("Clear Cache")
        
        if submit_button and user_input:
            # Process the query directly, the duplicate check happens in process_query
            process_query(user_input)
        
        if clear_button:
            # Clear session state cache to fix stuck responses
            st.session_state.messages = []
            st.session_state.processing = False
            st.session_state.last_request_id = None
            st.session_state.processed_queries = set()
            st.rerun()

    # Footer
    st.markdown("---")
    st.markdown("Built with LangChain, Streamlit and Plotly 🤖😊")
    
    # Add debug panel at the bottom if there's debug info
    if "debug_info" in st.session_state:
        with st.expander("Debug Information", expanded=False):
            debug_info = st.session_state.debug_info
            
            # Display SQL query if available
            if "sql_query" in debug_info:
                st.markdown("### SQL Query")
                st.code(debug_info.get("sql_query", ""), language="sql")
            
            # Display timing information if available
            if "timing" in debug_info:
                st.markdown("### Timing Information")
                timing_data = debug_info.get("timing", {})
                for step, time_value in timing_data.items():
                    st.text(f"{step}: {time_value}s")
            
            # Display raw debug info as JSON
            st.markdown("### Raw Debug Info")
            st.json(debug_info)

# Entry point - call the main function
if __name__ == "__main__":
    main() 