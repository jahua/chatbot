import streamlit as st
import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import base64
import io
from PIL import Image

# Set page configuration
st.set_page_config(
    page_title="Tourism Analytics Chatbot",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5em;
        color: #1E88E5;
        margin-bottom: 0.5em;
    }
    .query-box {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 1em;
        margin-bottom: 1em;
    }
    .response-box {
        background-color: #e1f5fe;
        border-radius: 10px;
        padding: 1em;
        margin-bottom: 1em;
    }
    .sql-box {
        background-color: #e8eaf6;
        border-radius: 10px;
        padding: 1em;
        font-family: monospace;
        margin-bottom: 1em;
    }
</style>
""", unsafe_allow_html=True)

# Constants
API_URL = "http://localhost:8000"

# Initialize session state
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'session_id' not in st.session_state:
    st.session_state.session_id = "streamlit-session"

# Header
st.markdown("<h1 class='main-header'>Tourism Analytics Chatbot</h1>", unsafe_allow_html=True)

# Sidebar for configuration
with st.sidebar:
    st.header("About")
    st.write("Ask questions about tourism data and get insightful visualizations")
    
    st.header("Examples")
    example_questions = [
        "What is the industry with the highest spending?",
        "What was the busiest week in spring 2023?",
        "Compare domestic vs international visitors in 2023",
        "What are the top 5 regions by visitor count?"
    ]
    
    for q in example_questions:
        if st.button(q):
            st.session_state.messages.append({"role": "user", "content": q})

# Function to decode base64 visualization
def decode_base64_to_image(base64_string):
    try:
        # Remove the data URL prefix if present
        if "base64," in base64_string:
            base64_string = base64_string.split("base64,")[1]
        
        image_bytes = base64.b64decode(base64_string)
        image = Image.open(io.BytesIO(image_bytes))
        return image
    except Exception as e:
        st.error(f"Error decoding visualization: {e}")
        return None

# Function to send message to API
def send_message(message):
    try:
        response = requests.post(
            f"{API_URL}/chat",
            json={
                "message": message,
                "session_id": st.session_state.session_id,
                "is_direct_query": False
            }
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        st.error(f"Connection error: {str(e)}")
        return None

# Function to generate Plotly chart from SQL results
def generate_plotly_chart(data, chart_type="bar"):
    if not data:
        return None
    
    df = pd.DataFrame(data)
    
    # Identify numeric columns for y-axis
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    if not numeric_cols:
        return None
    
    # Identify non-numeric column for x-axis
    non_numeric_cols = df.select_dtypes(exclude=['number']).columns.tolist()
    x_col = non_numeric_cols[0] if non_numeric_cols else df.columns[0]
    y_col = numeric_cols[0]
    
    if chart_type == "bar":
        fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}")
    elif chart_type == "line":
        fig = px.line(df, x=x_col, y=y_col, title=f"{y_col} over {x_col}")
    elif chart_type == "pie":
        fig = px.pie(df, names=x_col, values=y_col, title=f"{y_col} Distribution")
    else:
        fig = px.bar(df, x=x_col, y=y_col, title=f"{y_col} by {x_col}")
    
    fig.update_layout(
        autosize=True,
        height=500,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    
    return fig

# Display chat history
for message in st.session_state.messages:
    if message["role"] == "user":
        st.markdown(f"<div class='query-box'><strong>You:</strong> {message['content']}</div>", unsafe_allow_html=True)
    else:
        st.markdown(f"<div class='response-box'><strong>Assistant:</strong> {message['content']}</div>", unsafe_allow_html=True)
        
        # Display SQL query if available
        if "sql_query" in message:
            st.markdown(f"<div class='sql-box'><strong>SQL Query:</strong><pre>{message['sql_query']}</pre></div>", unsafe_allow_html=True)
        
        # Display visualization if available
        if "visualization" in message:
            st.subheader("Visualization")
            try:
                # First try to use the built-in visualization if result data is available
                if "result_data" in message:
                    fig = generate_plotly_chart(message["result_data"])
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        # Fall back to the base64 image
                        image = decode_base64_to_image(message["visualization"])
                        if image:
                            st.image(image, use_column_width=True)
                else:
                    # Use the base64 image
                    image = decode_base64_to_image(message["visualization"])
                    if image:
                        st.image(image, use_column_width=True)
            except Exception as e:
                st.error(f"Error displaying visualization: {e}")

# Input for new message
with st.container():
    user_input = st.text_input("Ask a question about tourism data:", key="user_input")
    
    col1, col2 = st.columns([1, 5])
    with col1:
        send_button = st.button("Send")
    
    if send_button and user_input:
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": user_input})
        
        # Get response from API
        with st.spinner("Thinking..."):
            response_data = send_message(user_input)
            
        if response_data:
            # Create assistant message
            assistant_message = {
                "role": "assistant", 
                "content": response_data.get("content", "I couldn't generate a response.")
            }
            
            # Add SQL query if available
            if "sql_query" in response_data:
                assistant_message["sql_query"] = response_data["sql_query"]
            
            # Add visualization if available
            if "visualization" in response_data:
                assistant_message["visualization"] = response_data["visualization"]
            
            # Add result data if available
            if "result" in response_data:
                assistant_message["result_data"] = response_data["result"]
            
            # Add to chat history
            st.session_state.messages.append(assistant_message)
            
            # Rerun to update UI
            st.experimental_rerun() 