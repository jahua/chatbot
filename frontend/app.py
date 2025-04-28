from visualization_service import StreamlitVisualizationService
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
import plotly.express as px

# Add the project root to the Python path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the visualization service

# Set page config first thing
st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded",
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
        margin-bottom: 1rem !important;
    }

    /* Add spacing between messages */
    .stChatMessage {
        margin-bottom: 1.5rem !important;
    }

    /* Make dataframes use full width */
    .stDataFrame {
        width: 100% !important;
    }

    /* Hide View SQL Query in input form */
    [data-testid="stExpander"] {
        display: none !important;
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
st.session_state.api_url = config.API_URL.rstrip(
    '/')  # Remove trailing slash if present
print(f"DEBUG app.py: Ensuring API URL is set to {st.session_state.api_url}")


def get_base_url():
    """Get the base URL"""
    base_url = os.getenv("API_URL", "http://localhost:8081").rstrip('/')
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
            logger.error(
                f"API health check failed with status code: {response.status_code}")
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

# Helper function for displaying visualization data
def display_visualization_data(viz_data):
    # Initial check: If viz_data is None or not a dict, display info and return.
    if not viz_data or not isinstance(viz_data, dict):
        # Log this occurrence for debugging
        logger.warning(f"display_visualization_data called with invalid data: {type(viz_data)}, value: {str(viz_data)[:100]}")
        st.info("No visualization data received or data is in an invalid format.")
        return
        
    # ---> ADDED LOGGING BEFORE THE SUSPECTED ERROR LINE <---    
    logger.debug(f"Inside display_visualization_data. Type(viz_data): {type(viz_data)}, Value: {str(viz_data)[:200]}...")
    # -------------------------------------------------------

    viz_type = viz_data.get("type")
    
    # ---> ADDED LOGGING AFTER .get('type') <---    
    logger.debug(f"Got viz_type: {viz_type} (type: {type(viz_type)})")
    # -------------------------------------------

    if viz_type == "plotly_json":
        # Log entry into plotly block
        logger.debug("Processing viz_type: plotly_json")
        fig_data = viz_data.get("data", {}) # This .get() would fail if viz_data was None
        if fig_data:
            # Check if fig_data itself is a string that needs loading
            if isinstance(fig_data, str):
                try:
                    fig_data = json.loads(fig_data)
                except json.JSONDecodeError:
                    st.error("Failed to parse Plotly JSON string.")
                    return
            # Ensure fig_data is a dict before creating Figure
            if isinstance(fig_data, dict):
                 try:
                    fig = go.Figure(fig_data)
                    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True})
                 except Exception as plot_err:
                     st.error(f"Error rendering Plotly chart: {plot_err}")
                     st.json(fig_data) # Show raw data on error
            else:
                st.error("Invalid format for Plotly data.")
                st.json(fig_data) # Show the problematic data
        else:
            st.info("No data provided for Plotly visualization.")
            
    elif viz_type == "table":
        # Log entry into table block
        logger.debug("Processing viz_type: table")
        table_data = viz_data.get("data", []) # This .get() would fail if viz_data was None
        if table_data:
            df = pd.DataFrame(table_data)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No data available for table visualization.")
            
    elif viz_type == "no_data":
         # Log entry into no_data block
         logger.debug("Processing viz_type: no_data")
         st.info("No data available to create a visualization.")
         
    elif viz_type:
        # Handle other potential simple types or show raw
        logger.warning(f"Processing viz_type: Unsupported type '{viz_type}'")
        st.warning(f"Unsupported or unrecognized visualization type: '{viz_type}'")
        st.json(viz_data) 
    else:
        # If type is missing, show raw data
        logger.warning("Processing viz_type: Type is missing/None")
        st.warning("Visualization type missing.")
        st.json(viz_data)

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

    # Reset processing state from any previous errors
    st.session_state.processing = False

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

    try:
        # Use streaming process if enabled
        if use_streaming:
            process_streaming_query(query, request_id)
        else:
            # Non-streaming process
            api_endpoint = f"{st.session_state.api_url}/chat"
            response = requests.post(
                api_endpoint,
                json={
                    "message": query,
                    "session_id": st.session_state.current_chat_id
                },
                headers={"Content-Type": "application/json"},
                timeout=60
            )
            
            if response.status_code == 200:
                response_data = response.json()
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response_data.get("content", "No response content"),
                    "request_id": request_id
                })
            else:
                st.error(
                    f"Error: Server returned status code {response.status_code}")
            
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        logger.error(f"Error processing query: {str(e)}")
    finally:
        # Reset processing flag
        st.session_state.processing = False

# --- Define streaming process function ---


def process_streaming_query(query: str, request_id: str):
    """Process a query with streaming and update chat interface incrementally."""
    try:
        # Initialize message containers using st.container() for persistence
        content_container = st.empty()
        sql_container = st.empty()
        viz_container = st.empty()  # Single container for visualization
        debug_container = st.empty() if st.session_state.debug_mode else None

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
                raise Exception(
                    f"API returned status code {response.status_code}")

            client = sseclient.SSEClient(response)
                    
            # Process events
            for event in client.events():
                if not event.data:
                    continue
                            
                try:
                    data = json.loads(event.data)
                    event_type = data.get("type", "")

                    if event_type == "status":
                        # Optionally display status updates (e.g., in a temporary area)
                        # st.info(data.get("status", "Processing..."))
                        pass # Often, just logging is enough
                    elif event_type == "content":
                        content = data.get("content", "")
                        current_message["content"] += content
                        with content_container:
                            st.markdown(current_message["content"])
                                
                    elif event_type == "sql_query":
                        sql_query = data.get("sql_query", "")
                        current_message["sql_query"] = sql_query
                        with sql_container:
                            if st.session_state.debug_mode:
                                with st.expander("View SQL Query"):
                                    st.code(sql_query, language="sql")
                                
                    elif event_type == "visualization":
                        # Get data, default to None if missing
                        viz_data = data.get("visualization", None) 
                        # Only store and attempt to display if data is not None
                        if viz_data: 
                            current_message["visualization"] = viz_data # Store for final message
                            try:
                                with viz_container:
                                    st.empty() # Clear previous viz attempts
                                    # Log before calling
                                    logger.debug(f"Calling display_visualization_data (intermediate). Type: {type(viz_data)}, Value: {str(viz_data)[:100]}...") 
                                    display_visualization_data(viz_data)
                            except Exception as e:
                                st.error(f"Error displaying visualization: {str(e)}")
                                logger.error(f"Visualization error: {str(e)}\nData: {str(viz_data)[:500]}") # Log more data on error
                        else:
                             # If viz_data is None from the start, ensure it's None in current_message too
                             current_message["visualization"] = None
                             logger.debug("Intermediate visualization data is None. Skipping display.")

                    elif event_type == "final_response":
                        # This chunk contains the complete final state
                        current_message["content"] = data.get("content", current_message["content"]) # Use final content
                        current_message["sql_query"] = data.get("sql_query", current_message.get("sql_query"))
                        current_message["visualization"] = data.get("visualization", current_message.get("visualization"))
                        current_message["debug_info"] = data.get("debug_info", current_message.get("debug_info"))
                        
                        # Update UI with final content
                        with content_container:
                            st.markdown(current_message["content"])
                        
                        # Update visualization display only if final data exists
                        final_viz_data = data.get("visualization", current_message.get("visualization"))
                        current_message["visualization"] = final_viz_data # Store final state
                        
                        # Update visualization display only if final data exists
                        if final_viz_data: 
                            try:
                                with viz_container:
                                    st.empty() # Clear previous viz attempts
                                    # Log before calling
                                    logger.debug(f"Calling display_visualization_data (final). Type: {type(final_viz_data)}, Value: {str(final_viz_data)[:100]}...")
                                    display_visualization_data(final_viz_data)
                            except Exception as e:
                                st.error(f"Error displaying final visualization: {str(e)}")
                                logger.error(f"Final Visualization error: {str(e)}\nData: {str(final_viz_data)[:500]}") # Log more data on error
                        else:
                            # Ensure viz container is empty if no final visualization
                            with viz_container: 
                                st.empty()
                            logger.debug("Final visualization data is None. Skipping display.")
                        
                        # Update debug info if needed
                        if st.session_state.debug_mode and debug_container and current_message.get("debug_info"):
                             with debug_container:
                                with st.expander("Debug Info (Final)"):
                                    st.json(current_message["debug_info"])
                        
                        # Add the final message to session state
                        st.session_state.messages.append(current_message)
                        logger.info("Final response processed and added to messages.")
                        break # End processing after final response
                                
                    elif event_type == "error":
                        error_content = data.get("content", "An unknown error occurred.")
                        st.error(error_content)
                        current_message["content"] = error_content # Store error in message
                        current_message["status"] = "error"
                        st.session_state.messages.append(current_message)
                        logger.error(f"Received error chunk: {error_content}")
                        break # Stop processing on error
                    
                    # Remove the old 'end' event logic as final_response handles completion
                    # elif event_type == "end":
                    #    if current_message.get("content") or current_message.get("visualization"):
                    #        st.session_state.messages.append(current_message)
                    #    break
                                
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse event data: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    continue

            # Clean up streaming client
            client.close()

    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
        logger.error(
            f"Error in process_streaming_query: {str(e)}\n{traceback.format_exc()}")
    finally:
        st.session_state.processing = False


def create_visualization(data, viz_type="auto"):
    """Create appropriate visualization based on data structure and type"""
    try:
        # Convert data to DataFrame if needed
        if isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            st.error("Invalid data format for visualization")
            return

        # Get column types
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        date_cols = [
            col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        text_cols = df.select_dtypes(include=['object']).columns

        # Create tabs for different visualization options
        tab1, tab2, tab3 = st.tabs(
            ["📈 Main Chart", "📊 Alternative View", "📑 Data Table"])

        with tab1:
            # Determine primary visualization type
            if viz_type == "auto":
                if 'spending' in df.columns.str.lower().tolist(
                ) or 'amount' in df.columns.str.lower().tolist():
                    viz_type = "bar"
                elif 'density' in df.columns.str.lower().tolist():
                    viz_type = "heatmap"
                elif date_cols and numeric_cols.any():
                    viz_type = "line"
                elif len(df.columns) == 2 and len(numeric_cols) == 1:
                    viz_type = "pie"
                else:
                    viz_type = "bar"

            if viz_type == "line":
                date_col = date_cols[0]
                fig = px.line(
                    df,
                    x=date_col,
                    y=numeric_cols,
                    title="Time Series Analysis",
                    template="plotly_dark",
                    height=600  # Increased height
                )

                # Enhanced line chart layout
                fig.update_layout(
                    showlegend=True,
                    # Increased top margin for title
                    margin=dict(l=60, r=40, t=80, b=60),
                    plot_bgcolor='rgba(17, 17, 17, 0.1)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(size=14, color='white'),  # Increased font size
                    title=dict(
                        text="<b>Time Series Analysis</b>",  # Bold title
                        font=dict(size=20, color='white'),  # Larger title
                        x=0.5,
                        xanchor='center'
                    ),
                    legend=dict(
                        yanchor="top",
                        y=0.99,
                        xanchor="left",
                        x=0.01,
                        bgcolor='rgba(0,0,0,0.3)',
                        bordercolor='rgba(255,255,255,0.2)',
                        borderwidth=1,
                        font=dict(size=13)
                    ),
                    xaxis=dict(
                        gridcolor='rgba(255,255,255,0.1)',
                        showgrid=True,
                        title_font=dict(size=16),
                        tickfont=dict(size=13),
                        rangeslider=dict(visible=True),
                        title="<b>Date</b>"  # Bold axis title
                    ),
                    yaxis=dict(
                        gridcolor='rgba(255,255,255,0.1)',
                        showgrid=True,
                        title_font=dict(size=16),
                        tickfont=dict(size=13),
                        tickformat=',.0f',
                        title="<b>Value</b>",  # Bold axis title
                        rangemode='tozero'  # Start y-axis from 0
                    ),
                    updatemenus=[
                        dict(
                            type="buttons",
                            showactive=False,
                            buttons=[
                                dict(
                                    label="Play",
                                    method="animate",
                                    args=[
                                        None, {
                                            "frame": {
                                                "duration": 500, "redraw": True}, "fromcurrent": True}]
                                ),
                                dict(
                                    label="Pause",
                                    method="animate",
                                    args=[
                                        [None], {
                                            "frame": {
                                                "duration": 0, "redraw": False}, "mode": "immediate"}]
                                )
                            ],
                            x=0.1,
                            y=1.1,
                        )
                    ],
                    annotations=[
                        dict(
                            text="Click and drag to zoom, double-click to reset",
                            showarrow=False,
                            x=0.5,
                            y=1.08,
                            xref='paper',
                            yref='paper',
                            font=dict(size=12, color='rgba(255,255,255,0.6)')
                        )
                    ]
                )

                # Enhanced line styling
                for i in range(len(fig.data)):
                    fig.data[i].update(
                        line=dict(
                            width=3,
                            dash=None if i == 0 else ['solid', 'dash', 'dot', 'dashdot'][i % 4]
                        ),
                        mode='lines+markers',
                        marker=dict(
                            size=8,
                            symbol=['circle', 'diamond', 'square', 'triangle-up'][i % 4],
                            line=dict(width=2, color='rgba(255,255,255,0.8)')
                        ),
                        hovertemplate=(
                            f"<b>Date:</b> %{{x}}<br>"
                            f"<b>{fig.data[i].name}:</b> %{{y:,.0f}}<br>"
                            "<extra></extra>"
                        )
                    )

            elif viz_type == "bar":
                if len(df.columns) >= 2:
                    x_col = text_cols[0] if len(
                        text_cols) > 0 else df.columns[0]
                    y_cols = numeric_cols
                else:
                    x_col = df.index
                    y_cols = df.columns[0]

                fig = px.bar(
                    df,
                    x=x_col,
                    y=y_cols,
                    title="Data Distribution",
                    template="plotly_dark",
                    height=500,
                    barmode='group',
                    color_discrete_sequence=px.colors.qualitative.Set3
                )

                fig.update_layout(
                    showlegend=True,
                    margin=dict(l=60, r=40, t=60, b=60),
                    plot_bgcolor='rgba(17, 17, 17, 0.1)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(size=13, color='white'),
                    title=dict(
                        font=dict(size=18),
                        x=0.5,
                        xanchor='center'
                    ),
                    xaxis=dict(
                        categoryorder='total descending',  # Sort bars by value
                        gridcolor='rgba(255,255,255,0.1)',
                        showgrid=True,
                        tickangle=45  # Angle labels for better readability
                    ),
                    yaxis=dict(
                        gridcolor='rgba(255,255,255,0.1)',
                        showgrid=True,
                        tickformat=',.0f'
                    ),
                    bargap=0.15,
                    bargroupgap=0.1
                )

            # Display the plot
            st.plotly_chart(fig, use_container_width=True, config={
                'displayModeBar': True,
                'displaylogo': False,
                'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                'scrollZoom': True
            })

        with tab2:
            # Create alternative visualization
            if viz_type == "line":
                # Show as area chart
                fig2 = px.area(
                    df,
                    x=date_col,
                    y=numeric_cols,
                    title="Area View",
                    template="plotly_dark",
                    height=400
                )
            elif viz_type == "bar":
                # Show as pie if single numeric column
                if len(numeric_cols) == 1:
                    fig2 = px.pie(
                        df,
                        values=numeric_cols[0],
                        names=x_col,
                        title="Distribution View",
                        template="plotly_dark",
                        height=400,
                        hole=0.4
                    )
                else:
                    # Show as stacked bar
                    fig2 = px.bar(
                        df,
                        x=x_col,
                        y=numeric_cols,
                        title="Stacked View",
                        template="plotly_dark",
                        height=400,
                        barmode='stack'
                    )

            fig2.update_layout(
                margin=dict(l=40, r=40, t=40, b=40),
                plot_bgcolor='rgba(17, 17, 17, 0.1)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(size=12, color='white')
            )
            st.plotly_chart(fig2, use_container_width=True)

        with tab3:
            # Show summary statistics
            st.subheader("Summary Statistics")
            if numeric_cols.any():
                cols = st.columns(3)
                for i, col in enumerate(
                        numeric_cols[:3]):  # Show stats for up to 3 numeric columns
                    with cols[i]:
                        st.metric(
                            f"{col}",
                            f"{df[col].mean():,.1f}",
                            f"±{df[col].std():,.1f}"
                        )

            # Show interactive table with formatting
            st.dataframe(
                df.style.format({
                    col: '{:,.2f}' for col in numeric_cols
                }).background_gradient(
                    cmap='Blues',
                    subset=numeric_cols
                ),
                use_container_width=True,
                height=300
            )
                    
    except Exception as e:
        st.error(f"Error creating visualization: {str(e)}")
        st.write("Debug: Full error:", traceback.format_exc())


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

        # For assistant messages, show additional info in a more organized way
        if message["role"] == "assistant":
            
            # --- MODIFIED VISUALIZATION HANDLING --- 
            # Check if key exists AND if the value is not None
            viz_data = message.get("visualization") # Use .get() for safety
            if viz_data is not None:
                logger.debug(f"display_message found visualization data (type: {type(viz_data)}). Preparing to display.")
                try:
                    # Create tabs for visualization and data
                    viz_tab, data_tab = st.tabs(["📈 Visualization", "📊 Data"])
                    
                    with viz_tab:
                        # *** CALL THE HELPER FUNCTION ***
                        display_visualization_data(viz_data)
                            
                    with data_tab:
                        # Display raw data if available within the viz_data dict
                        raw_data = viz_data.get("raw_data")
                        if raw_data and isinstance(raw_data, list):
                            try:
                                df = pd.DataFrame(raw_data)
                                st.dataframe(df, use_container_width=True)
                                logger.debug("Displayed raw_data in data tab.")
                            except Exception as df_err:
                                st.error(f"Failed to display raw data table: {df_err}")
                                logger.error(f"Error creating DataFrame from raw_data: {df_err}")
                                st.json(raw_data) # Show raw data if table fails
                        else:
                            # If no specific raw_data key, maybe show the whole viz_data as fallback?
                            # Or indicate no separate raw data is available.
                            st.info("No separate raw data available for this visualization.")
                            # Optionally display the viz_data dict itself for debugging:
                            # st.json(viz_data)

                except Exception as e:
                    # Catch errors during tab creation or helper call
                    st.error(f"Error displaying visualization section: {str(e)}") 
                    logger.error(f"Error in display_message visualization block: {str(e)}", exc_info=True)
                    # Display raw viz_data if error occurs
                    st.json(viz_data)
            # else: # Optional: Log if visualization key exists but is None
            #    if "visualization" in message:
            #         logger.debug("display_message: 'visualization' key exists but value is None.")
            # --- END MODIFIED VISUALIZATION HANDLING ---

            # Create columns for SQL and Debug toggles
            # Check if key exists before accessing
            sql_query = message.get("sql_query")
            debug_info = message.get("debug_info")
            request_id = message.get("request_id", uuid.uuid4()) # Generate fallback key if needed
            
            if sql_query or (debug_info and st.session_state.debug_mode):
                col1, col2 = st.columns([1, 1])

                # SQL Query toggle
                if sql_query:
                    with col1:
                        # Use unique key based on request_id
                        if st.toggle("Show SQL Query", key=f"sql_{request_id}"):
                            st.code(sql_query, language="sql")

                # Debug info toggle
                if st.session_state.debug_mode and debug_info:
                    with col2:
                         # Use unique key based on request_id
                        if st.toggle("Show Debug Info", key=f"debug_{request_id}"):
                            st.json(debug_info)


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
    # Title and description
    st.title("Tourism Data Insights Chatbot 💬")
    st.markdown("""
        Ask questions about Swiss tourism data to get insights and visualizations.
        """)

    # Create a container for status
    status_container = st.container()
    with status_container:
        st.markdown(
            '<div style="margin-bottom: 1rem;">',
            unsafe_allow_html=True)
        if check_api_connection():
            st.success("API Connected ✅")
        else:
            st.error("API Unavailable ❌")
            st.info(
                "Please check if the backend server is running at: " +
                st.session_state.api_url)
        st.markdown('</div>', unsafe_allow_html=True)

    # Add a separator after status
    st.markdown(
        '<hr style="margin: 0.5rem 0; border-color: #333;">',
        unsafe_allow_html=True)

    # Create a container for the main chat interface
    chat_container = st.container()
        
    # Create a container for the input form
    input_container = st.container()

    # Display chat history in the main container
    with chat_container:
        for message in st.session_state.messages:
            # *** CALL THE CORRECTED HELPER FUNCTION ***
            display_message(message)
            # *** REMOVE ALL THE DUPLICATED LOGIC THAT WAS HERE ***

        # Show loading animation when processing
        if st.session_state.processing:
            with st.chat_message("assistant"):
                st.markdown(
                    '<div style="display: flex; align-items: center; gap: 8px;">🤖 <div class="stSpinner"></div> Analyzing data...</div>',
                    unsafe_allow_html=True)

    # Chat input form at the bottom
    with input_container:
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

    # Add example questions at the bottom
    st.markdown("### Example Questions")
    example_questions = [
        "Show me the daily visitor trend in July 2023 with a line chart",
        "Create a pie chart of spending distribution across different industries",
        "Display weekly visitor trends for the first quarter of 2023",
        "Compare visitor numbers between different regions of Switzerland",
        "Show weekly visitor trends for spring 2023 as a line graph",
        "Which are the top 10 regions by visitor count?"]

    # Create two columns for example questions
    col1, col2 = st.columns(2)

    # Display examples in two columns with better styling
    for i, question in enumerate(example_questions):
        with col1 if i % 2 == 0 else col2:
            if st.button(
                question,
                key=f"example_{i}",
                use_container_width=True,
                help="Click to use this example"
            ):
                # Process the question immediately
                process_query(question, use_streaming=True)


# Entry point - call the main function
if __name__ == "__main__":
    main()
