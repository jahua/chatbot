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
# Update reference back
st.session_state.api_url = config.API_URL
print(f"DEBUG app.py: Ensuring API URL is set to {st.session_state.api_url}")
# ---------------------------------------

# --- Define API health check function ---
def get_base_url():
    """Get the base URL without API prefix"""
    base_url = os.getenv("API_URL", "http://localhost:8000").rstrip('/')
    if not base_url.startswith(('http://', 'https://')):
        base_url = f"http://{base_url}"
    return base_url

def check_api_connection():
    """Check if the API is accessible"""
    try:
        # Health check uses base URL
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
                "session_id": st.session_state.current_chat_id,
                "is_direct_query": False
            },
            headers={"Content-Type": "application/json"},
            timeout=60
        )

        # Log response info
        logger.info(
            f"Received response from /chat: Status {response.status_code}")

        # Handle the response
        if response.status_code == 200:
            try:
                # Parse the response
                result = response.json()
                
                # Create a message from the response
                assistant_message = {
                    "role": "assistant",
                    "content": result.get(
                        "content",
                        "I don't have a response for that."),
                    "request_id": request_id}

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
            logger.error(
                f"Error: Server returned status code {response.status_code}")
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
    """Process a query with streaming and update chat interface incrementally."""
    try:
        # Initialize assistant message
        assistant_placeholder = st.empty()
        with assistant_placeholder.chat_message("assistant"):
            st.markdown("🤖 Analyzing your query...")

        # Initialize containers for different components
        content_container = st.empty()
        sql_container = st.empty()
        viz_container = st.empty()
        debug_container = st.empty() if st.session_state.debug_mode else None

        # Prepare streaming request with timeout
        timeout = 60  # 60 seconds timeout
        current_content = ""
        has_error = False
        
        with requests.post(
            f"{st.session_state.api_url}/chat/stream",
            json={
                "message": query,
                "session_id": str(uuid.uuid4()),
                "is_direct_query": False
            },
            headers={
                "Content-Type": "application/json",
                "Accept": "text/event-stream"
            },
            stream=True,
            timeout=timeout
        ) as response:
            
            if response.status_code != 200:
                raise Exception(f"API returned status code {response.status_code}")
            
            client = sseclient.SSEClient(response)
            
            # Process events with timeout
            start_time = time.time()
            for event in client.events():
                # Check if we've exceeded timeout
                if time.time() - start_time > timeout:
                    raise TimeoutError("Stream processing exceeded timeout")
                
                if not event.data:
                    continue
                
                try:
                    data = json.loads(event.data)
                    event_type = data.get("type", "")
                    
                    # Show raw event data if debug mode is enabled
                    if st.session_state.show_raw_events:
                        with debug_container:
                            st.json(data)
                    
                    if event_type == "content":
                        content = data.get("content", "")
                        current_content += content
                        with content_container:
                            st.markdown(current_content)
                    
                    elif event_type == "sql":
                        sql_query = data.get("sql", "")
                        with sql_container:
                            st.code(sql_query, language="sql")
                    
                    elif event_type == "visualization":
                        viz_data = data.get("visualization", "")
                        with viz_container:
                            st.markdown(viz_data)
                    
                    elif event_type == "error":
                        error_msg = data.get("error", "An unknown error occurred")
                        has_error = True
                        with content_container:
                            st.error(f"Error: {error_msg}")
                    
                    elif event_type == "end":
                        # Update final message in session state
                        if not has_error:
                            st.session_state.messages.append({
                                "role": "assistant",
                                "content": current_content,
                                "sql": sql_container.code if sql_container else None,
                                "visualization": viz_data if 'viz_data' in locals() else None
                            })
                        break
                
                except json.JSONDecodeError as e:
                    logging.error(f"Failed to parse event data: {e}")
                    continue
                except Exception as e:
                    logging.error(f"Error processing event: {e}")
                    continue
            
            # Clean up streaming client
            client.close()
            
    except TimeoutError:
        with content_container:
            st.error("The request timed out. Please try again or rephrase your question.")
    except requests.exceptions.RequestException as e:
        with content_container:
            st.error(f"Failed to connect to the API: {str(e)}")
    except Exception as e:
        with content_container:
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


def display_message(message):
    """Display a single message in the chat interface"""
    if message["role"] == "user":
        st.chat_message("user").write(message["content"])
    else:
        logger.info(
            f"Displaying assistant message, content length: {len(message.get('content', ''))}, has SQL: {bool(message.get('sql_query'))}, has viz: {bool(message.get('visualization') or message.get('plotly_json'))}")

        with st.chat_message("assistant"):
            if message.get("content"):
                st.write(message["content"])
                logger.debug(
                    f"Assistant content displayed: {message['content'][:100]}...")

                # Check if no data was found and no visualization is present
                content = message.get("content", "").lower()
                no_data_keywords = [
                    "no data",
                    "no results",
                    "no information",
                    "couldn't find",
                    "i don't have",
                    "didn't find"
                ]
                has_no_data = any(
                    keyword in content for keyword in no_data_keywords)
                has_viz = bool(message.get("visualization")
                               or message.get("plotly_json"))
                has_sql_results = bool(message.get("sql_results") and len(message.get("sql_results", [])) > 0)
                
                # If no data was found and no visualization available, show suggestions
                if (has_no_data and not has_viz) or (not has_sql_results and "no data" in content):
                    # Get alternative suggestions from debug_info if available
                    alternative_suggestions = None
                    if message.get("debug_info") and message.get("debug_info").get("alternative_suggestions"):
                        alternative_suggestions = message.get("debug_info").get("alternative_suggestions")
                    
                    # If we have suggestions from the backend, use them
                    if alternative_suggestions and isinstance(alternative_suggestions, list) and len(alternative_suggestions) > 0:
                        # Format the suggestions as a list
                        suggestion_text = "\n".join([f"- {suggestion}" for suggestion in alternative_suggestions])
                        st.info(f"""
                        **Try these alternative queries:**
                        {suggestion_text}
                        """)
                    else:
                        # Fallback to default suggestions
                        st.info("""
                        **Try these alternative queries:**
                        - "How many tourists visited Lugano in 2023?"
                        - "Show me tourism trends across Ticino"
                        - "What was the breakdown of spending by industry?"
                        - "Compare Swiss tourists vs foreign tourists in Bellinzona"
                        """)
            elif message.get("is_streaming", False):
                # Display loading for streaming messages with no content
                st.info("Loading response...")
            else:
                # Check if we have API connection issue with SQL results but no
                # content
                if message.get("sql_query") and not message.get("content"):
                    # Create a fallback response from SQL results
                    fallback_msg = "I've found some results for your query, but I'm having trouble generating a detailed explanation."
                    plotly_json = message.get("plotly_json", {})

                    # Check for single value in plotly_json
                    if plotly_json and isinstance(
                            plotly_json, dict) and plotly_json.get("single_value"):
                        column_name = plotly_json.get(
                            "column_name", "").replace("_", " ")
                        value = plotly_json.get("value")
                        if column_name and value is not None:
                            fallback_msg = f"Based on the data, the {column_name} is {value}."

                    st.write(fallback_msg)
                    logger.warning(
                        "Generated fallback content for message with SQL but no content")
                else:
                    logger.warning(
                        "Assistant message has no content to display")

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
                        # Check if we're dealing with a raw JSON string or a
                        # dict
                        if isinstance(plotly_json, str):
                            fig_dict = json.loads(plotly_json)
                        else:
                            fig_dict = plotly_json

                        # Handle single value visualization specially
                        is_single_value = False
                        if isinstance(fig_dict,
                                      dict) and fig_dict.get("single_value"):
                            is_single_value = True
                            logger.info("Detected single value visualization")

                        fig = go.Figure(fig_dict)

                        # Add responsive layout settings
                        if is_single_value:
                            # Special layout for single values
                            fig.update_layout(
                                autosize=True,
                                margin=dict(l=20, r=20, t=30, b=20),
                                height=200,  # Smaller height for single values
                            )
                        else:
                            # Normal chart layout
                            fig.update_layout(
                                autosize=True,
                                margin=dict(l=20, r=20, t=30, b=20),
                                height=400,
                            )

                        st.plotly_chart(fig, use_container_width=True)
                        logger.debug("Successfully displayed Plotly chart")
                    except Exception as e:
                        logger.error(
                            f"Error displaying Plotly JSON chart: {str(e)}",
                            exc_info=True)
                        st.error(
                            f"Could not display interactive chart: {str(e)}")
                        # Try to show the JSON for debugging
                        with st.expander("Raw Plotly Data", expanded=False):
                            st.json(plotly_json)

                # Handle legacy visualizations as fallback
                elif legacy_vis:
                    logger.debug(
                        f"Displaying legacy visualization: {type(legacy_vis)}")
                    try:
                        if isinstance(legacy_vis, dict):
                            vis_type = legacy_vis.get("type", "")

                            # Handle no_data type explicitly
                            if vis_type == "no_data":
                                st.warning(
                                    f"No data was found for the query: '{legacy_vis.get('data', {}).get('query', 'Unknown query')}'")
                                st.info("""
                                **Try these alternative queries:**
                                - "How many tourists visited Lugano in 2023?"
                                - "Show me tourism trends across Ticino"
                                - "What was the breakdown of spending by industry?"
                                - "Compare Swiss tourists vs foreign tourists in Bellinzona"
                                """)
                            # Handle tables with improved formatting
                            elif vis_type == "table":
                                table_data = legacy_vis.get("data", [])
                                if table_data:
                                    # Check for single value table
                                    if len(table_data) == 1 and len(
                                            table_data[0]) == 1:
                                        # Extract the single key-value pair
                                        key = list(table_data[0].keys())[0]
                                        value = table_data[0][key]
                                        key_readable = key.replace("_", " ")

                                        # Create a custom display for single
                                        # value
                                        st.markdown(
                                            f"### {key_readable.title()}")
                                        st.markdown(
                                            f"<h1 style='text-align: center; color: #1E88E5;'>{value}</h1>",
                                            unsafe_allow_html=True)
                                    else:
                                        # Regular table display
                                        df = pd.DataFrame(table_data)

                                        # Format numeric columns
                                        for col in df.select_dtypes(
                                                include=['float64', 'int64']).columns:
                                            df[col] = df[col].map(
                                                lambda x: f"{x:,.2f}" if isinstance(
                                                    x, float) else x)

                                        st.dataframe(
                                            df,
                                            use_container_width=True,
                                            # Dynamic height based on row count
                                            height=min(400, 50 + 35 * len(df))
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

                            # Handle plotly_json inside visualization dict
                            # (newer format)
                            elif vis_type == "plotly_json":
                                plotly_data = legacy_vis.get("data", "")
                                try:
                                    if isinstance(plotly_data, str):
                                        fig_dict = json.loads(plotly_data)
                                    else:
                                        fig_dict = plotly_data

                                    fig = go.Figure(fig_dict)
                                    st.plotly_chart(
                                        fig, use_container_width=True)
                                except Exception as e:
                                    logger.error(
                                        f"Error displaying embedded Plotly JSON: {str(e)}")
                                    st.error(
                                        "Could not display interactive chart")

                            # Handle unknown visualization types
                            else:
                                logger.warning(
                                    f"Unknown legacy visualization type: {vis_type}")
                                with st.expander("Raw Visualization Data", expanded=False):
                                    st.json(legacy_vis)
                        else:
                            logger.warning(
                                f"Legacy visualization is not a dict: {type(legacy_vis)}")
                            st.write(legacy_vis)
                    except Exception as e:
                        logger.error(
                            f"Error displaying legacy visualization: {str(e)}",
                            exc_info=True)
                        st.error("Could not display visualization.")
                # --- End Enhanced Visualization Handling ---

                # --- Check for missing visualization but has SQL results ---
                elif message.get("sql_query") and not message.get("visualization") and not message.get("plotly_json"):
                    # Only show this if we have a successful response
                    if message.get("content") and len(message.get("content")) > 10:
                        # Show a more helpful message that doesn't imply missing information
                        st.info("This analysis is presented in text form as it's best understood through the analytical insight above.")
                        
                        # Display SQL results table if available directly in the message
                        if message.get("sql_results"):
                            sql_results = message.get("sql_results")
                            st.subheader("SQL Results")
                            # Format the data as a dataframe
                            df = pd.DataFrame(sql_results)
                            
                            # Format any numeric columns
                            for col in df.select_dtypes(include=['float64', 'int64']).columns:
                                df[col] = df[col].map(lambda x: f"{x:,.2f}" if isinstance(x, float) else x)
                            
                            # Display the dataframe with scroll if needed
                            st.dataframe(
                                df,
                                use_container_width=True,
                                height=min(400, 50 + 35 * len(df))  # Dynamic height based on row count
                            )
                            
                            # Add export buttons
                            col1, col2 = st.columns(2)
                            with col1:
                                # CSV download button
                                csv = df.to_csv(index=False)
                                st.download_button(
                                    label="Download CSV",
                                    data=csv,
                                    file_name="sql_results.csv",
                                    mime="text/csv"
                                )
                            with col2:
                                # Excel download button (if possible)
                                try:
                                    buffer = BytesIO()
                                    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                                        df.to_excel(writer, sheet_name='Results', index=False)
                                    st.download_button(
                                        label="Download Excel",
                                        data=buffer.getvalue(),
                                        file_name="sql_results.xlsx",
                                        mime="application/vnd.ms-excel"
                                    )
                                except Exception as e:
                                    logger.error(f"Error creating Excel file: {str(e)}")
                                    # Fallback to CSV if Excel fails
                                    if not col1.button_hooked:
                                        st.download_button(
                                            label="Download CSV (Excel not available)",
                                            data=csv,
                                            file_name="sql_results.csv",
                                            mime="text/csv"
                                        )

            # Display debug info if available
            if message.get("debug_info"):
                with st.expander("Debug Information", expanded=False):
                    # Show steps and processing time
                    debug_display = message.copy()
                    debug_display.pop('visualization', None)
                    debug_display.pop('plotly_json', None)
                    st.json(debug_display)
                    
                    # If raw event data was collected, show it
                    if message.get("raw_events"):
                        with st.expander("Raw Event Data", expanded=False):
                            for i, event in enumerate(message.get("raw_events", [])):
                                with st.expander(f"Event {i+1}: {event.get('type', 'unknown')}", expanded=False):
                                    st.code(json.dumps(event, indent=2), language="json")


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
