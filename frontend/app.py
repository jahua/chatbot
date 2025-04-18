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
                        # Set visualization data
                        st.session_state.messages[-1]["visualization"] = data.get("visualization")
                        
                    elif data.get("type") == "debug_info":
                        # Set debug info
                        st.session_state.messages[-1]["debug_info"] = data.get("debug_info")
                        
                    elif data.get("type") == "error":
                        # Handle error
                        error_msg = data.get("error", "Unknown error occurred")
                        st.session_state.messages[-1]["content"] += f"\n\nError: {error_msg}"
                        st.session_state.messages[-1]["is_streaming"] = False
                        logger.error(f"Error in streaming: {error_msg}")
                        
                    elif data.get("type") == "end":
                        # End of stream
                        st.session_state.messages[-1]["is_streaming"] = False
                        logger.debug("End of streaming response")
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing streaming chunk: {str(e)}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing streaming chunk: {str(e)}")
                    logger.error(traceback.format_exc())
                    continue
            
    except requests.RequestException as e:
        logger.error(f"Error in streaming request: {str(e)}")
        st.session_state.messages[-1]["content"] = f"Sorry, I encountered an error while processing your request: {str(e)}"
        st.session_state.messages[-1]["is_streaming"] = False
    finally:
        # Ensure we mark processing as done
        st.session_state.processing = False
        # Mark streaming as complete in case it wasn't already
        if st.session_state.messages and st.session_state.messages[-1].get("request_id") == request_id:
            st.session_state.messages[-1]["is_streaming"] = False
        # Force a final UI update
        st.rerun()
# --- END Define streaming process function ---

def display_rag_flow(steps: List[Dict[str, Any]], visualization: Optional[str] = None):
    """Display the RAG flow steps and visualization"""
    logger.debug(f"Displaying RAG flow with {len(steps)} steps")
    st.subheader("Analysis Process")
    
    # Create columns for steps and visualization
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Display steps
        for step in steps:
            status_icon = "✅" if step["status"] == "completed" else "❌" if step["status"] == "failed" else "⏳"
            st.markdown(f"{status_icon} **{step['name']}**")
            if "details" in step:
                st.markdown(f"*{step['details']}*")
            st.markdown("---")
    
    with col2:
        # Display visualization if available
        if visualization is not None:
            st.subheader("Visualization")
            st.image(f"data:image/png;base64,{visualization}")
            st.markdown("---")

def display_message(message):
    if message["role"] == "user":
        st.chat_message("user").write(message["content"])
    else:
        with st.chat_message("assistant"):
            # Display the content
            st.markdown(message["content"])
            
            # If the message is still streaming, show a typing indicator
            if message.get("is_streaming", False):
                st.markdown('<div class="typing-indicator"><span></span><span></span><span></span></div>', unsafe_allow_html=True)
            
            # Add debug info toggle for visualization errors
            debug_info = {}
            debug_status = "success"
            
            if "visualization" in message:
                st.write("---")
                try:
                    visualization = message["visualization"]
                    
                    # Debugging information
                    debug_info["viz_type"] = type(visualization).__name__
                    debug_info["viz_length"] = len(str(visualization)) if visualization else 0
                    
                    if visualization:
                        if isinstance(visualization, str):
                            # For base64 images
                            if len(visualization) > 100:
                                try:
                                    # Add padding if needed to make it a multiple of 4
                                    padding = len(visualization) % 4
                                    if padding:
                                        visualization += '=' * (4 - padding)
                                        
                                    # Decode and display image
                                    image_bytes = base64.b64decode(visualization)
                                    debug_info["image_bytes_len"] = len(image_bytes)
                                    st.image(image_bytes)
                                except Exception as e:
                                    st.error(f"Error displaying image: {str(e)}")
                                    logger.error(f"Error displaying image: {str(e)}")
                                    debug_info["error"] = str(e)
                                    debug_status = "error"
                            else:
                                st.warning(f"Visualization data too short: {visualization}")
                                debug_info["error"] = "Visualization data too short"
                                debug_status = "warning"
                        
                        # For JSON data
                        elif isinstance(visualization, (dict, str)):
                            try:
                                # If it's a string, try to parse it as JSON
                                if isinstance(visualization, str):
                                    logger.debug(f"Attempting to parse visualization from string: {visualization[:100]}...")
                                    viz_data = json.loads(visualization)
                                    debug_info["parsed_from"] = "string"
                                else:
                                    viz_data = visualization
                                    debug_info["parsed_from"] = "dict"
                                
                                logger.debug(f"Visualization data keys: {list(viz_data.keys()) if isinstance(viz_data, dict) else 'not a dict'}")
                                debug_info["viz_keys"] = list(viz_data.keys()) if isinstance(viz_data, dict) else 'not a dict'
                                
                                # Check if the expected structure is present
                                if isinstance(viz_data, dict) and "figure" in viz_data and "type" in viz_data:
                                    fig_data = viz_data["figure"]
                                    
                                    # Create a figure from the data
                                    fig = go.Figure(fig_data)
                                    
                                    # Update for dark mode if needed
                                    if st.get_option("theme.base") == "dark":
                                        fig.update_layout(
                                            paper_bgcolor="rgba(0,0,0,0)",
                                            plot_bgcolor="rgba(0,0,0,0)",
                                            font_color="white"
                                        )
                                    
                                    # Display the figure
                                    st.plotly_chart(fig, use_container_width=True)
                                    debug_info["display_method"] = "plotly_chart"
                                else:
                                    st.warning("Invalid visualization structure")
                                    debug_info["error"] = "Invalid structure"
                                    debug_info["available_keys"] = list(viz_data.keys()) if isinstance(viz_data, dict) else 'not a dict'
                                    debug_status = "warning"
                                    logger.error(f"Invalid visualization structure: {viz_data}")
                            except json.JSONDecodeError as e:
                                st.error(f"Error parsing visualization: {str(e)}")
                                logger.error(f"JSON decode error: {str(e)}")
                                debug_info["error"] = f"JSON decode: {str(e)}"
                                debug_status = "error"
                            except Exception as e:
                                st.error(f"Error displaying visualization: {str(e)}")
                                logger.error(f"Error displaying visualization: {str(e)}")
                                debug_info["error"] = str(e)
                                debug_status = "error"
                        else:
                            st.warning(f"Unexpected visualization type: {type(visualization).__name__}")
                            debug_info["error"] = f"Unexpected type: {type(visualization).__name__}"
                            debug_status = "warning"
                except Exception as e:
                    st.error(f"Error processing visualization: {str(e)}")
                    logger.error(f"Error processing visualization: {str(e)}")
                    debug_info["error"] = str(e)
                    debug_status = "error"
            
            # Add debug details from backend if available
            if "debug_info" in message and message["debug_info"]:
                try:
                    if isinstance(message["debug_info"], str):
                        debug_info["debug_from_backend"] = json.loads(message["debug_info"])
                    else:
                        debug_info["debug_from_backend"] = message["debug_info"]
                except Exception as e:
                    debug_info["debug_parse_error"] = str(e)
            
            # Add debug info expander if there's any debug info
            if debug_info:
                with st.expander("Debug Info", expanded=False):
                    # Display status tag based on debug_status
                    st.markdown(f'<div class="debug-tag debug-tag-{debug_status}">{debug_status.upper()}</div>', unsafe_allow_html=True)
                    
                    # Split debug info into sections
                    if "error" in debug_info:
                        st.markdown("### Error")
                        st.error(debug_info["error"])
                        # Remove from dictionary to avoid duplication
                        error_info = {"error": debug_info.pop("error")}
                    
                    # Display visualization info
                    st.markdown("### Visualization Details")
                    viz_info = {k: v for k, v in debug_info.items() if k not in ["debug_from_backend"]}
                    if viz_info:
                        st.json(viz_info)
                    
                    # Display backend debug info separately if available
                    if "debug_from_backend" in debug_info:
                        st.markdown("### Backend Debug Info")
                        st.json(debug_info["debug_from_backend"])
            
            # Display SQL query if available
            if "sql_query" in message and message["sql_query"]:
                with st.expander("SQL Query", expanded=False):
                    st.code(message["sql_query"], language="sql")

# Configure the page
st.set_page_config(
    page_title="Tourism Data Analysis Chatbot",
    page_icon="🏔️",
    layout="wide"
)

# Custom CSS for ChatGPT-like styling
st.markdown("""
<style>
/* Main container styling */
.stApp {
    background-color: #343541;
    color: #ECECF1;
}

/* Sidebar styling */
[data-testid="stSidebar"] {
    background-color: #202123;
    padding: 2rem 1rem;
}

[data-testid="stSidebar"] .stButton > button {
    width: 100%;
    background-color: transparent;
    color: #ECECF1;
    border: 1px solid #4A4B53;
    border-radius: 4px;
    padding: 0.5rem;
    margin: 0.25rem 0;
    text-align: left;
}

[data-testid="stSidebar"] .stButton > button:hover {
    background-color: #2A2B32;
}

/* Chat message styling */
div[data-testid="stChatMessage"] {
    background-color: #444654;
    border-radius: 8px;
    padding: 0.75rem;
    margin-bottom: 1rem;
    border: 1px solid #565869;
}

div[data-testid="stChatMessage"][data-testid*="user"] {
    background-color: #343541;
}

/* Expander styling for debug info */
div[data-testid="stExpander"] {
    border: 1px solid #565869;
    border-radius: 8px;
    margin-top: 0.5rem;
    background-color: #2A2B32;
}

div[data-testid="stExpander"] > div[role="button"] {
    color: #ECECF1;
    font-weight: 500;
}

/* JSON display styling in debug info */
div[data-testid="stJson"] {
    background-color: #2A2B32 !important;
    border-radius: 0 0 8px 8px;
    border: none !important;
    max-height: 300px;
    overflow-y: auto;
}

/* Code block styling for SQL */
div[data-testid="stCodeBlock"] {
    background-color: #1E1E2E !important;
    border-radius: 4px;
    font-family: 'JetBrains Mono', 'Consolas', monospace;
}

/* Error and warning styling */
div[data-testid="stAlert"] {
    background-color: rgba(255, 0, 0, 0.2);
    border: 1px solid rgba(255, 0, 0, 0.5);
    border-radius: 4px;
}

div[data-testid="stAlert"][data-baseweb="notification"][kind="warning"] {
    background-color: rgba(255, 165, 0, 0.2);
    border: 1px solid rgba(255, 165, 0, 0.5);
}

/* Plotly chart container */
div[data-testid="stPlotlyChart"] {
    background-color: #2A2B32;
    border-radius: 8px;
    padding: 0.5rem;
    border: 1px solid #565869;
}

/* Loading animation */
@keyframes dots {
    0%, 20% { content: '.'; }
    40% { content: '..'; }
    60% { content: '...'; }
    80% { content: '....'; }
    100% { content: '.....'; }
}

.loading::after {
    content: '.';
    animation: dots 1.5s steps(5, end) infinite;
    display: inline-block;
    width: 30px;
    text-align: left;
}

/* Typing indicator animation */
.typing-indicator {
    display: flex;
    margin-top: 10px;
}

.typing-indicator span {
    height: 8px;
    width: 8px;
    background: #9e9ea1;
    border-radius: 50%;
    margin: 0 2px;
    display: inline-block;
    animation: bounce 1.3s linear infinite;
}

.typing-indicator span:nth-child(2) {
    animation-delay: 0.15s;
}

.typing-indicator span:nth-child(3) {
    animation-delay: 0.3s;
}

@keyframes bounce {
    0%, 60%, 100% {
        transform: translateY(0);
    }
    30% {
        transform: translateY(-4px);
    }
}

/* Form styling */
.stForm {
    background-color: transparent !important;
    border: none !important;
    padding: 0 !important;
}

.stForm > div {
    background-color: transparent !important;
    border: none !important;
}

/* Debug info tags */
.debug-tag {
    display: inline-block;
    padding: 3px 8px;
    margin: 2px;
    border-radius: 12px;
    font-size: 0.8rem;
    color: white;
}

.debug-tag-success {
    background-color: rgba(0, 128, 0, 0.7);
}

.debug-tag-error {
    background-color: rgba(255, 0, 0, 0.7);
}

.debug-tag-warning {
    background-color: rgba(255, 165, 0, 0.7);
}

.debug-tag-info {
    background-color: rgba(0, 123, 255, 0.7);
}
</style>
""", unsafe_allow_html=True)

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

# Main chat interface
st.title("🏔️ Tourism Data Analysis Chatbot")
st.markdown("""
Ask questions about:
- Visitor statistics
- Spending patterns
- Seasonal trends
- Industry analysis
""")

# Sidebar with example questions
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
    
    for q in example_questions:
        if st.button(q):
            # Process the query directly, the duplicate check happens in process_query
            process_query(q)

# Display chat messages
for message in st.session_state.messages:
    display_message(message)

# Show loading animation when processing
if st.session_state.processing:
    st.markdown('<div class="chat-message bot-message"><div class="message-content">🤖 Analyzing<span class="loading"></span></div></div>', unsafe_allow_html=True)

# Chat input using form
with st.form(key="chat_form", clear_on_submit=True):
    cols = st.columns([7, 2, 1])
    with cols[0]:
        user_input = st.text_input("Ask a question about tourism data:", 
                                  key="chat_input",
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