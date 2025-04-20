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
import sys  # <-- Add import for flush
import config  # Import configuration

# Add the project root to the Python path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the visualization service
from visualization_service import StreamlitVisualizationService

# Set page config first thing
st.set_page_config(
    layout="wide",
    initial_sidebar_state="collapsed",
    page_title="Swisscom Data Explorer")

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
    # Use override=True to ensure .env values take precedence over environment
    # variables
    load_dotenv(dotenv_path=dotenv_path, override=True)
    print(
        f"DEBUG app.py: Loaded .env. Checking POSTGRES_HOST: {os.getenv('POSTGRES_HOST')}")
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
    if not all(st.session_state.db_config.get(key)
               for key in ["host", "port", "dbname", "user", "password"]):
        print(
            f"DEBUG app.py: Warning - Some DB config values are missing after loading .env. Config: {st.session_state.db_config}")
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
if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False
if "show_raw_events" not in st.session_state:
    st.session_state.show_raw_events = False
# -----------------------------------------

# --- API URL config ---
# Always ensure api_url is set in session state
st.session_state.api_url = config.API_URL.rstrip('/')  # Remove trailing slash if present
print(f"DEBUG app.py: Ensuring API URL is set to {st.session_state.api_url}")

def get_base_url():
    """Get the base URL"""
    base_url = os.getenv("API_URL", "http://localhost:8000").rstrip('/')
    if not base_url.startswith(('http://', 'https://')):
        base_url = f"http://{base_url}"
    return base_url

def check_api_connection():
    """Check if the API is accessible"""
    try:
        base_url = get_base_url()
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code == 200:
            logger.info("API connection successful")
            return True
        else:
            logger.error(f"API health check failed with status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        logger.error(f"Cannot connect to API at {base_url}")
        return False
    except requests.exceptions.Timeout:
        logger.error("API connection timed out")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking API connection: {str(e)}")
        return False

# --- Define process_query function EARLY ---
def process_query(query: str, use_streaming: bool = True):
    """Process a user query and display the result in the chat interface"""
    # Check if the query is already being processed
    if query in st.session_state.processed_queries:
        logger.warning(f"Duplicate query detected: {query}")
        return

    # Check API connection first
    if not check_api_connection():
        st.error("Cannot connect to the API. Please check if the server is running.")
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
    logger.info(
        f"Processing query: '{query}' (request_id: {request_id}, streaming: {use_streaming})")

    # Use streaming process if enabled
    if use_streaming:
        process_streaming_query(query, request_id)
        return

    # Non-streaming process (fallback or user preference)
    try:
        # Make the API request to the backend
        api_endpoint = f"{st.session_state.api_url}/chat"
        logger.debug(f"Sending request to {api_endpoint}")
        response = requests.post(
            api_endpoint,
            json={
                "message": query,
                "session_id": st.session_state.current_chat_id
            },
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        
        # Process the response
        if response.status_code == 200:
            response_data = response.json()
            st.session_state.messages.append({
                "role": "assistant",
                "content": response_data.get("content", "No response content"),
                "request_id": request_id
            })
        else:
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"Error: Server returned status code {response.status_code}",
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
    """Process a query with streaming and update chat interface incrementally."""
    try:
        # Initialize message containers
        message_containers = {
            "content": st.empty(),
            "sql": st.empty(),
            "viz": st.empty(),
            "debug": st.empty() if st.session_state.debug_mode else None
        }
        
        # Initialize message data
        current_message = {
            "role": "assistant",
            "content": "",
            "request_id": request_id
        }
        
        # Start streaming request
        with requests.post(
            f"{st.session_state.api_url}/chat/stream",
            json={
                "message": query,
                "session_id": st.session_state.current_chat_id
            },
            headers={
                "Content-Type": "application/json",
                "Accept": "text/event-stream"
            },
            stream=True,
            timeout=60
        ) as response:
            if response.status_code != 200:
                raise Exception(f"API returned status code {response.status_code}")
            
            client = sseclient.SSEClient(response)
            
            # Process events
            for event in client.events():
                if not event.data:
                    continue
                
                try:
                    data = json.loads(event.data)
                    event_type = data.get("type", "")
                    
                    # Show raw events if debug mode is enabled
                    if st.session_state.show_raw_events and message_containers["debug"]:
                        with message_containers["debug"]:
                            st.json(data)
                    
                    if event_type == "content":
                        content = data.get("content", "")
                        current_message["content"] += content
                        with message_containers["content"]:
                            st.markdown(current_message["content"])
                    
                    elif event_type == "sql_query":
                        sql_query = data.get("sql_query", "")
                        current_message["sql_query"] = sql_query
                        with message_containers["sql"]:
                            with st.expander("View SQL Query"):
                                st.code(sql_query, language="sql")
                    
                    elif event_type == "sql_results":
                        sql_results = data.get("sql_results", {})
                        current_message["sql_results"] = sql_results
                    
                    elif event_type == "visualization":
                        viz_data = data.get("visualization", {})
                        current_message["visualization"] = viz_data
                        with message_containers["viz"]:
                            if isinstance(viz_data, dict) and "data" in viz_data:
                                viz_service = StreamlitVisualizationService()
                                viz_service.create_visualization(
                                    viz_data["data"],
                                    current_message.get("content", "")
                                )
                    
                    elif event_type == "plotly_json":
                        plotly_data = data.get("data", {})
                        current_message["plotly_json"] = plotly_data
                        with message_containers["viz"]:
                            if plotly_data:
                                fig = go.Figure(plotly_data)
                                st.plotly_chart(fig, use_container_width=True)
                    
                    elif event_type == "debug_info":
                        debug_info = data.get("debug_info", {})
                        current_message["debug_info"] = debug_info
                        if st.session_state.debug_mode and message_containers["debug"]:
                            with message_containers["debug"]:
                                with st.expander("Debug Info"):
                                    st.json(debug_info)
                    
                    elif event_type == "error":
                        error_msg = data.get("error", "An unknown error occurred")
                        current_message["content"] = f"Error: {error_msg}"
                        current_message["error"] = error_msg
                        with message_containers["content"]:
                            st.error(error_msg)
                    
                    elif event_type == "end":
                        # Add the final message to session state
                        st.session_state.messages.append(current_message)
                        break
                
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse event data: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    continue
            
            # Clean up streaming client
            client.close()
            
    except TimeoutError:
        st.error("The request timed out. Please try again or rephrase your question.")
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to connect to the API: {str(e)}")
    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
    finally:
        # Reset processing state
        st.session_state.processing = False


def display_rag_flow(steps: List[Dict[str, Any]],
                     visualization: Optional[str] = None):
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


def display_message(message: Dict[str, Any]) -> None:
    """Display a chat message with its associated content."""
    with st.chat_message(message["role"]):
        # Display the main content
        st.markdown(message.get("content", ""))
        
        # Initialize visualization service if needed
        viz_service = StreamlitVisualizationService()
        
        # Display SQL query if present
        if "sql_query" in message:
            with st.expander("View SQL Query"):
                st.code(message["sql_query"], language="sql")
        
        # Display SQL results if present
        if "sql_results" in message:
            with st.expander("View Data"):
                viz_service.create_visualization(
                    message["sql_results"],
                    message.get("content", "")  # Use message content as context
                )
        
        # Display visualization if present
        if "visualization" in message:
            viz_data = message["visualization"]
            if isinstance(viz_data, dict) and "data" in viz_data:
                viz_service.create_visualization(
                    viz_data["data"],
                    message.get("content", "")
                )
        
        # Display debug info if present and debug mode is enabled
        if st.session_state.get("debug_mode", False) and "debug_info" in message:
            with st.expander("Debug Info"):
                st.json(message["debug_info"])


# Sidebar for chat history and settings
with st.sidebar:
    st.title("💬 Chat History")
    
    # New chat button
    if st.button("🆕 New Chat"):
        # Save current conversation if it exists
        if st.session_state.messages:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            first_msg = next(
                (msg["content"] for msg in st.session_state.messages if msg["role"] == "user"),
                "New Chat")
            st.session_state.conversations.append({
                "timestamp": timestamp,
                "title": first_msg[:30] + "..." if len(first_msg) > 30 else first_msg,
                "messages": st.session_state.messages.copy()
            })
        # Clear current messages
        st.session_state.messages = []
        st.session_state.current_conversation_index = len(
            st.session_state.conversations)
        st.session_state.processed_queries = set()
    
    # Display saved conversations
    for i, conv in enumerate(st.session_state.conversations):
        if st.button(
            f"📝 {conv['timestamp']}: {conv['title']}",
                key=f"conv_{i}"):
            st.session_state.messages = conv["messages"].copy()
            st.session_state.current_conversation_index = i
            # Reset processed queries when switching conversations
            st.session_state.processed_queries = set()
    
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
        st.write(
            {
                "messages_count": len(
                    st.session_state.messages),
                "conversations_count": len(
                    st.session_state.conversations),
            "current_index": st.session_state.current_conversation_index,
            "processing": st.session_state.processing,
                "processed_queries": list(
                    st.session_state.processed_queries)[
                        :5] +
                ["..."] if len(
                    st.session_state.processed_queries) > 5 else list(
                    st.session_state.processed_queries)})
    
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

    # Add API connection status in the second column
    with header_cols[1]:
        st.markdown("### Status")
        if check_api_connection():
            st.success("API Connected ✅")
        else:
            st.error("API Unavailable ❌")
            st.info("Please check if the backend server is running at: " + st.session_state.api_url)

    # Add tab-based navigation
    tabs = st.tabs(["Chat", "Examples", "Debug", "About"])

    # Chat tab
    with tabs[0]:
        st.title("Tourism Data Insights Chatbot")
        
        # Add a debug toggle in the main chat interface
        col1, col2 = st.columns([4, 1])
        with col2:
            show_debug = st.checkbox("Debug Mode", value=st.session_state.debug_mode, key="main_debug_mode")
            st.session_state.debug_mode = show_debug
            st.session_state.show_raw_events = st.checkbox("Show Raw Data", value=st.session_state.show_raw_events, key="main_raw_data")
        
        # Display the chat interface
        for message in st.session_state.messages:
            display_message(message)

        # Show loading animation when processing
        if st.session_state.processing:
            with st.chat_message("assistant"):
                st.markdown(
                    '<div style="display: flex; align-items: center; gap: 8px;">🤖 <div class="stSpinner"></div> Analyzing data...</div>',
                    unsafe_allow_html=True)

        # Chat input using form with improved layout
        with st.form(key="main_chat_form", clear_on_submit=True):
            cols = st.columns([7, 1, 1, 1])
            with cols[0]:
                user_input = st.text_input(
                    "Ask a question about tourism data:",
                    key="main_chat_input",
                    placeholder="Type your question here...",
                    label_visibility="collapsed")
            with cols[1]:
                submit_button = st.form_submit_button("Send")
            with cols[2]:
                clear_button = st.form_submit_button("Clear")
            with cols[3]:
                streaming = st.toggle(
                    "Stream", value=True, key="use_streaming")

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
            "Show visitor counts for top 3 days in summer 2023?",
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
        
        # Add raw event data toggle
        st.checkbox("Show raw event data", value=False, key="show_raw_events", 
                   help="When enabled, shows the complete JSON data for each event during streaming")
        
        # Connection test tool
        st.subheader("Connection Test")
        test_cols = st.columns([3, 1])
        with test_cols[0]:
            test_query = st.text_input(
                "Test query:",
                value="Which industry had the highest spending?",
                key="debug_test_query")
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
                        add_log(f"Connecting to {st.session_state.api_url}/chat/stream")

                        request_id = str(uuid.uuid4())
                        response = requests.post(
                            f"{st.session_state.api_url}/chat/stream",
                            json={
                                "message": test_query,
                                "session_id": str(
                                    uuid.uuid4()),
                                "is_direct_query": False},
                            headers={
                                "Content-Type": "application/json",
                                "Accept": "text/event-stream"},
                            stream=True,
                            timeout=60)

                        add_log(
                            f"Connection established: Status {response.status_code}")
                        debug_placeholder.success(
                            f"Connected! Status: {response.status_code}")

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
                                    debug_placeholder.success(
                                        "Test completed successfully!")
                                    break

                            except Exception as e:
                                add_log(f"Error processing event: {str(e)}")

                        # Log final stats
                        add_log(
                            f"Test finished. Processed {event_count} events.")
                        if not content_received:
                            add_log(
                                "WARNING: No content was received in the stream")

                    except Exception as e:
                        add_log(f"Error: {str(e)}")
                        debug_placeholder.error(f"Test failed: {str(e)}")

        # API information
        st.subheader("API Information")
        st.write(f"API URL: {st.session_state.api_url}")
        st.write(f"Session ID: {st.session_state.current_chat_id}")

        # Debug settings
        st.subheader("Debug Settings")
        if st.checkbox("Show all debugging information", value=False):
            st.json({
                "API URL": st.session_state.api_url,
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
