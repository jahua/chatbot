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
import sys # <-- Add import for flush
import config # Import configuration

# Set page config first thing
st.set_page_config(layout="wide", initial_sidebar_state="collapsed", page_title="Swisscom Data Explorer")

# Add custom CSS to make content area wider and more responsive
st.markdown("""
<style>
    .main .block-container {
        max-width: 95% !important;
        padding: 1rem 2rem !important;
    }
    
    /* Make the plotly charts responsive */
    .stPlotlyChart {
        width: 100% !important;
    }
    
    /* Improve chat message display */
    [data-testid="stChatMessage"] {
        max-width: 90% !important;
    }
    
    /* Make dataframes use full width */
    .stDataFrame {
        width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)

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
    st.session_state.db_config = config.DB_CONFIG
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

# --- API URL config ---
# Always ensure api_url is set in session state
# Update reference back
st.session_state.api_url = config.API_URL
print(f"DEBUG app.py: Ensuring API URL is set to {st.session_state.api_url}")
# ---------------------------------------

# --- Define process_query function EARLY ---
def process_query(query: str, use_streaming: bool = True):
    """Process a user query and display the result in the chat interface"""
    # Check if the query is already being processed
    if query in st.session_state.processed_queries:
        logger.warning(f"Duplicate query detected: {query}")
        return
    
    # Add to processed queries
    st.session_state.processed_queries.add(query)
    
    # Add user message to chat
    st.session_state.messages.append({"role": "user", "content": query})
    
    # Set the processing flag to display a loading indicator
    st.session_state.processing = True
    
    # Generate a unique request ID
    request_id = str(uuid.uuid4())
    st.session_state.last_request_id = request_id
    
    # Log the request
    logger.info(f"Processing query: '{query}' (request_id: {request_id}, streaming: {use_streaming})")
    
    # Use streaming process if enabled
    if use_streaming:
        process_streaming_query(query, request_id)
        return
    
    # Non-streaming process (fallback or user preference)
    try:
        # Make the API request to the backend
        logger.debug(f"Sending request to {config.API_URL}/chat")
        response = requests.post(
            f"{config.API_URL}/chat",
            json={"message": query, "session_id": st.session_state.current_chat_id, "is_direct_query": False},
            headers={"Content-Type": "application/json"},
            timeout=60  # 60 second timeout
        )
        
        # Log response info
        logger.info(f"Received response from /chat: Status {response.status_code}")
        
        # Handle the response
        if response.status_code == 200:
            try:
                # Parse the response
                result = response.json()
                
                # Create a message from the response
                assistant_message = {
                    "role": "assistant",
                    "content": result.get("content", "I don't have a response for that."),
                    "request_id": request_id
                }
                
                # Add additional data if available
                if "sql_query" in result:
                    assistant_message["sql_query"] = result["sql_query"]
                
                if "visualization" in result:
                    assistant_message["visualization"] = result["visualization"]
                
                if "plotly_json" in result:
                    assistant_message["plotly_json"] = result["plotly_json"]
                
                if "debug_info" in result:
                    assistant_message["debug_info"] = result["debug_info"]
                
                # Add the message to the chat
                st.session_state.messages.append(assistant_message)
                
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing response JSON: {str(e)}")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": "Sorry, I encountered an error processing the response.",
                    "request_id": request_id
                })
        else:
            logger.error(f"Error: Server returned status code {response.status_code}")
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"Sorry, there was an error processing your request. Status code: {response.status_code}",
                "request_id": request_id
            })
    except requests.RequestException as e:
        logger.error(f"Error connecting to server: {str(e)}")
        st.session_state.messages.append({
            "role": "assistant",
            "content": "Sorry, I couldn't connect to the server. Please try again later.",
            "request_id": request_id
        })
    finally:
        # Reset processing flag
        st.session_state.processing = False
        # Force a rerun to update the UI immediately
        st.rerun()
# --- END Define process_query function ---

# --- Define streaming process function ---
def process_streaming_query(query: str, request_id: str):
    """Process a query with streaming and update the chat interface incrementally"""
    logger.info(f"Starting streaming query (request_id: {request_id})")
    
    # Create an initial assistant message
    assistant_message = {
        "role": "assistant",
        "content": "",
        "is_streaming": True,
        "request_id": request_id,
        "visualization": None,
        "sql_query": None,
        "debug_info": None,
        "plotly_json": None
    }
    
    # Add initial placeholder to messages
    st.session_state.messages.append(assistant_message)
    
    # Initialize tracking variables
    rerun_needed = False
    content_received = False
    last_rerun_time = time.time()
    
    # Add debug info directly to UI
    debug_container = st.empty()
    debug_container.info("Starting streaming request...")
    
    try:
        # Log API URL being used
        logger.info(f"Using API URL: {config.API_URL}/chat/stream")
        debug_container.info(f"Connecting to: {config.API_URL}/chat/stream")
        
        # Send the streaming request
        with requests.post(
            f"{config.API_URL}/chat/stream",
            json={"message": query, "session_id": st.session_state.current_chat_id, "is_direct_query": False},
            headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
            stream=True,
            timeout=60
        ) as response:
            # Log response status
            logger.info(f"Streaming response received: Status {response.status_code}, Headers: {dict(response.headers)}")
            debug_container.success(f"Connected! Status: {response.status_code}")
            
            # Check for error status
            if response.status_code != 200:
                logger.error(f"Error status received: {response.status_code}")
                debug_container.error(f"Error status: {response.status_code}")
                st.session_state.messages[-1]["content"] = f"Sorry, there was an error processing your request. Status code: {response.status_code}"
                st.session_state.messages[-1]["is_streaming"] = False
                st.session_state.processing = False
                st.rerun()
                return
            
            # Process the streaming response line by line
            line_count = 0
            event_count = 0
            debug_container.info("Processing stream data...")
            
            for line in response.iter_lines():
                line_count += 1
                if not line:
                    continue
                    
                # Process the line
                decoded_line = line.decode('utf-8')
                
                # Skip non-data lines
                if not decoded_line.startswith('data:'):
                    logger.debug(f"Non-data line: {decoded_line[:100]}")
                    continue
                    
                # Parse the data
                try:
                    json_data = decoded_line[len('data:'):].strip()
                    if not json_data:
                        continue
                        
                    data = json.loads(json_data)
                    event_type = data.get("type", "")
                    event_count += 1
                    
                    logger.info(f"Event {event_count}: type={event_type}")
                    debug_container.info(f"Event {event_count}: {event_type}")
                    
                    # Process based on event type
                    if event_type == "content_start":
                        # Content stream is starting
                        pass
                    elif event_type == "content":
                        # Add content chunk
                        content = data.get("content", "")
                        st.session_state.messages[-1]["content"] += content
                        content_received = True
                        rerun_needed = True
                        logger.info(f"Content chunk received: '{content}', total content length now: {len(st.session_state.messages[-1]['content'])}")
                        debug_container.info(f"Content chunk received (length: {len(content)})")
                    elif event_type == "sql_query":
                        # Store SQL query
                        sql = data.get("sql_query", "")
                        st.session_state.messages[-1]["sql_query"] = sql
                        rerun_needed = True
                        logger.info(f"SQL query received, length: {len(sql)}")
                    elif event_type == "visualization":
                        # Store legacy visualization
                        vis_data = data.get("visualization", {})
                        st.session_state.messages[-1]["visualization"] = vis_data
                        rerun_needed = True
                        logger.info(f"Visualization received: {type(vis_data)}")
                    elif event_type == "plotly_json":
                        # Store Plotly JSON
                        plotly_data = data.get("data", "")
                        st.session_state.messages[-1]["plotly_json"] = plotly_data
                        rerun_needed = True
                        logger.info(f"Plotly JSON received, length: {len(plotly_data) if plotly_data else 0}")
                    elif event_type == "debug_info":
                        # Store debug info
                        debug_info = data.get("debug_info", {})
                        st.session_state.messages[-1]["debug_info"] = debug_info
                    elif event_type == "end":
                        # End of stream
                        logger.info("End of stream received")
                        debug_container.success("Stream completed successfully!")
                        st.session_state.messages[-1]["is_streaming"] = False
                        st.session_state.processing = False
                        st.rerun()
                        return
                    
                    # Periodically rerun to update UI (at most every 0.5 seconds)
                    # Only if we have content or a visualization change
                    current_time = time.time()
                    if rerun_needed and (current_time - last_rerun_time > 0.5):
                        debug_container.info(f"Updating UI with content {len(st.session_state.messages[-1]['content'])} chars")
                        st.rerun()
                        last_rerun_time = current_time
                        rerun_needed = False
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing SSE event JSON: {str(e)} on line: {decoded_line[:100]}...")
                    debug_container.warning(f"JSON parsing error")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error processing SSE line: {str(e)}")
                    debug_container.warning(f"Error: {str(e)}")
                    continue
            
            # If we got here, the stream ended without an "end" event
            logger.warning(f"Stream ended without 'end' event. Processed {line_count} lines, {event_count} events")
            debug_container.warning(f"Stream ended without 'end' event after {event_count} events")
            
    except requests.RequestException as e:
        logger.error(f"Request error during streaming: {str(e)}")
        debug_container.error(f"Connection error: {str(e)}")
        if not content_received:
            # Only show error if we didn't receive any content yet
            st.session_state.messages[-1]["content"] = f"Sorry, I encountered a connection error. Please try again."
    except Exception as e:
        logger.error(f"Unexpected error in streaming process: {str(e)}", exc_info=True)
        debug_container.error(f"Error: {str(e)}")
        if not content_received:
            st.session_state.messages[-1]["content"] = f"Sorry, something went wrong. Please try again."
    finally:
        # Always make sure we clean up properly
        logger.info("Cleaning up streaming process")
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
        logger.info(f"Displaying assistant message, content length: {len(message.get('content', ''))}, has SQL: {bool(message.get('sql_query'))}, has viz: {bool(message.get('visualization') or message.get('plotly_json'))}")
        
        with st.chat_message("assistant"):
            if message.get("content"):
                st.write(message["content"])
                logger.debug(f"Assistant content displayed: {message['content'][:100]}...")
            else:
                logger.warning("Assistant message has no content to display")
            
            # Create a container for visualization and SQL
            vis_container = st.container()
            
            with vis_container:
                # --- Enhanced Visualization Handling ---
                plotly_json = message.get("plotly_json")
                legacy_vis = message.get("visualization")
                
                # Display SQL query if available
                if message.get("sql_query"):
                    with st.expander("SQL Query", expanded=False):
                        st.code(message["sql_query"], language="sql")
                
                # Handle Plotly visualizations
                if plotly_json:
                    try:
                        logger.debug("Attempting to display Plotly JSON chart")
                        # Check if we're dealing with a raw JSON string or a dict
                        if isinstance(plotly_json, str):
                            fig_dict = json.loads(plotly_json)
                        else:
                            fig_dict = plotly_json
                            
                        fig = go.Figure(fig_dict)
                        
                        # Add responsive layout settings
                        fig.update_layout(
                            autosize=True,
                            margin=dict(l=20, r=20, t=30, b=20),
                            height=400,
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        logger.debug("Successfully displayed Plotly chart")
                    except Exception as e:
                        logger.error(f"Error displaying Plotly JSON chart: {str(e)}", exc_info=True)
                        st.error(f"Could not display interactive chart: {str(e)}")
                        # Try to show the JSON for debugging
                        with st.expander("Raw Plotly Data", expanded=False):
                            st.json(plotly_json)
                
                # Handle legacy visualizations as fallback
                elif legacy_vis:
                    logger.debug(f"Displaying legacy visualization: {type(legacy_vis)}")
                    try:
                        if isinstance(legacy_vis, dict):
                            vis_type = legacy_vis.get("type", "")
                            
                            # Handle tables with improved formatting
                            if vis_type == "table":
                                table_data = legacy_vis.get("data", [])
                                if table_data:
                                    df = pd.DataFrame(table_data)
                                    
                                    # Format numeric columns
                                    for col in df.select_dtypes(include=['float64', 'int64']).columns:
                                        df[col] = df[col].map(lambda x: f"{x:,.2f}" if isinstance(x, float) else x)
                                    
                                    st.dataframe(
                                        df, 
                                        use_container_width=True,
                                        height=min(400, 50 + 35 * len(df))  # Dynamic height based on row count
                                    )
                            
                            # Handle images with improved display
                            elif vis_type == "image":
                                image_data = legacy_vis.get("data", "")
                                if "base64," in image_data:
                                    b64_data = image_data.split("base64,")[1]
                                else:
                                    b64_data = image_data
                                    
                                image_bytes = base64.b64decode(b64_data)
                                st.image(image_bytes, use_column_width=True)
                                
                            # Handle plotly_json inside visualization dict (newer format)
                            elif vis_type == "plotly_json":
                                plotly_data = legacy_vis.get("data", "")
                                try:
                                    if isinstance(plotly_data, str):
                                        fig_dict = json.loads(plotly_data)
                                    else:
                                        fig_dict = plotly_data
                                        
                                    fig = go.Figure(fig_dict)
                                    st.plotly_chart(fig, use_container_width=True)
                                except Exception as e:
                                    logger.error(f"Error displaying embedded Plotly JSON: {str(e)}")
                                    st.error("Could not display interactive chart")
                            
                            # Handle unknown visualization types
                            else:
                                logger.warning(f"Unknown legacy visualization type: {vis_type}")
                                with st.expander("Raw Visualization Data", expanded=False):
                                    st.json(legacy_vis)
                        else:
                            logger.warning(f"Legacy visualization is not a dict: {type(legacy_vis)}")
                            st.write(legacy_vis)
                    except Exception as e:
                        logger.error(f"Error displaying legacy visualization: {str(e)}", exc_info=True)
                        st.error("Could not display visualization.")
                # --- End Enhanced Visualization Handling ---
                        
            # Display debug info if available (moved outside visualization container)
            if message.get("debug_info"):
                with st.expander("Debug Information", expanded=False):
                    # Create a copy and remove potentially large/binary fields before display
                    debug_display = message.copy()
                    debug_display.pop('visualization', None)
                    debug_display.pop('plotly_json', None)
                    st.json(debug_display)

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
    # Use columns for the header to add a logo on the right
    header_cols = st.columns([4, 1])
    with header_cols[0]:
        st.title("Tourism Data Insights Chatbot 💬")
        st.markdown("""
        Ask questions about Swiss tourism data to get insights and visualizations.
        """)
    
    # Optional: Add logo or status indicator in the second column
    with header_cols[1]:
        st.markdown("### Status")
        # Check backend connection
        try:
            response = requests.get("http://localhost:8000/health", timeout=2)
            if response.status_code == 200:
                st.success("Backend Connected ✅")
            else:
                st.error("Backend Error ❌")
        except:
            st.error("Backend Unavailable ❌")
    
    # Add tab-based navigation
    tabs = st.tabs(["Chat", "Examples", "Debug", "About"])
    
    # Chat tab
    with tabs[0]:
        # Display chat messages
        for message in st.session_state.messages:
            display_message(message)

        # Show loading animation when processing
        if st.session_state.processing:
            with st.chat_message("assistant"):
                st.markdown('<div style="display: flex; align-items: center; gap: 8px;">🤖 <div class="stSpinner"></div> Analyzing data...</div>', unsafe_allow_html=True)

        # Chat input using form with improved layout
        with st.form(key="main_chat_form", clear_on_submit=True):
            cols = st.columns([7, 1, 1, 1])
            with cols[0]:
                user_input = st.text_input("Ask a question about tourism data:", 
                                          key="main_chat_input",
                                          placeholder="Type your question here...",
                                          label_visibility="collapsed")
            with cols[1]:
                submit_button = st.form_submit_button("Send")
            with cols[2]:
                clear_button = st.form_submit_button("Clear")
            with cols[3]:
                streaming = st.toggle("Stream", value=True, key="use_streaming")
            
            if submit_button and user_input:
                process_query(user_input, use_streaming=streaming)
            
            if clear_button:
                st.session_state.messages = []
                st.session_state.processed_queries = set()
    
    # Examples tab
    with tabs[1]:
        st.markdown("### Example Questions")
        example_questions = [
            # New questions from the user
            "What was the busiest week in spring 2023?",
            "What day had the most visitors in 2023?", 
            "Compare Swiss vs foreign tourists in April 2023",
            "Which industry had the highest spending?",
            "Show visitor counts for top 3 days in summer 2023",
            "Plot the trend of visitors from Germany in 2023", 
            "What percentage of visitors came from Asia in 2023?",
            "Show me the spending patterns in hotels across different regions"
        ]
        
        # Display examples in a grid of buttons
        cols = st.columns(2)
        for i, q in enumerate(example_questions):
            with cols[i % 2]:
                if st.button(q, key=f"example_{i}"):
                    process_query(q)
    
    # Debug tab
    with tabs[2]:
        st.markdown("### Debugging Tools")
        
        # Connection test section
        st.subheader("Connection Test")
        test_cols = st.columns([3, 1])
        with test_cols[0]:
            test_query = st.text_input("Test query:", value="Which industry had the highest spending?", key="debug_test_query")
        with test_cols[1]:
            if st.button("Run Test", key="debug_test_button"):
                debug_placeholder = st.empty()
                debug_placeholder.info("Testing streaming connection...")
                
                # Create test message in session
                test_message = {
                    "role": "user",
                    "content": test_query
                }
                st.session_state.messages.append(test_message)
                
                # Logging state
                log_container = st.container()
                with log_container:
                    st.write("### Connection Logs")
                    log_output = st.empty()
                    log_text = ""
                    
                    def add_log(msg):
                        nonlocal log_text
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        log_text += f"[{timestamp}] {msg}\n"
                        log_output.code(log_text)
                    
                    # Run test
                    add_log(f"Starting test with query: {test_query}")
                    
                    try:
                        # Test connection
                        add_log(f"Connecting to {config.API_URL}/chat/stream")
                        
                        request_id = str(uuid.uuid4())
                        response = requests.post(
                            f"{config.API_URL}/chat/stream",
                            json={"message": test_query, "session_id": str(uuid.uuid4()), "is_direct_query": False},
                            headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
                            stream=True,
                            timeout=60
                        )
                        
                        add_log(f"Connection established: Status {response.status_code}")
                        debug_placeholder.success(f"Connected! Status: {response.status_code}")
                        
                        # Process streaming response 
                        event_count = 0
                        content_received = False
                        
                        for line in response.iter_lines():
                            if not line:
                                continue
                                
                            decoded_line = line.decode('utf-8')
                            if not decoded_line.startswith('data:'):
                                continue
                                
                            try:
                                json_data = decoded_line[len('data:'):].strip()
                                if not json_data:
                                    continue
                                    
                                data = json.loads(json_data)
                                event_type = data.get("type", "")
                                event_count += 1
                                
                                add_log(f"Event {event_count}: {event_type}")
                                
                                if event_type == "content":
                                    content = data.get("content", "")
                                    content_received = True
                                    add_log(f"Content: {content[:50]}...")
                                
                                if event_type == "end":
                                    add_log("Stream completed successfully")
                                    debug_placeholder.success("Test completed successfully!")
                                    break
                                    
                            except Exception as e:
                                add_log(f"Error processing event: {str(e)}")
                        
                        # Log final stats
                        add_log(f"Test finished. Processed {event_count} events.")
                        if not content_received:
                            add_log("WARNING: No content was received in the stream")
                        
                    except Exception as e:
                        add_log(f"Error: {str(e)}")
                        debug_placeholder.error(f"Test failed: {str(e)}")
        
        # API information
        st.subheader("API Information")
        st.write(f"API URL: {config.API_URL}")
        st.write(f"Session ID: {st.session_state.current_chat_id}")
        
        # Debug settings
        st.subheader("Debug Settings")
        if st.checkbox("Show all debugging information", value=False):
            st.json({
                "API URL": config.API_URL,
                "Session ID": st.session_state.current_chat_id,
                "Message count": len(st.session_state.messages),
                "Processing state": st.session_state.processing,
                "DB Config": st.session_state.db_config
            })
    
    # About tab
    with tabs[3]:
        st.markdown("""
        ### About This Chatbot
        
        This application provides insights into Swiss tourism data using natural language queries. 
        
        **Features:**
        - Ask questions in plain English about tourism data
        - Get visualizations of trends and patterns
        - View SQL queries used to analyze the data
        - See detailed explanations of the results
        
        **Data Sources:**
        The chatbot analyzes tourism data from various Swiss sources, including visitor numbers, 
        demographics, spending patterns, and regional statistics.
        
        **Technologies:**
        - Frontend: Streamlit
        - Backend: FastAPI
        - LLM: Claude Sonnet 3.5
        - Database: PostgreSQL
        """)

# Entry point - call the main function
if __name__ == "__main__":
    main() 