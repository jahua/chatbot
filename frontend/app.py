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

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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
.chat-message {
    display: flex;
    padding: 1.5rem;
    margin: 0;
    border-bottom: 1px solid #2A2B32;
}

.user-message {
    background-color: #343541;
}

.bot-message {
    background-color: #444654;
}

.message-content {
    max-width: 800px;
    margin: 0 auto;
    color: #ECECF1;
    font-size: 1rem;
    line-height: 1.5;
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

/* Visualization container styling */
.visualization-container {
    max-width: 800px;
    margin: 1rem auto;
    padding: 1rem;
    background-color: #2A2B32;
    border-radius: 0.5rem;
}

/* Input styling */
.input-container {
    max-width: 800px;
    margin: 1rem auto;
    padding: 1rem;
}

.stTextInput > div > div > input {
    color: #ECECF1 !important;
    background-color: #40414F !important;
    border: 1px solid #565869 !important;
    border-radius: 0.5rem !important;
    padding: 1rem !important;
    font-size: 1rem !important;
    line-height: 1.5 !important;
}

.stTextInput > div > div > input:focus {
    border-color: #10A37F !important;
    box-shadow: 0 0 0 2px rgba(16, 163, 127, 0.2) !important;
}

/* Button styling */
.stButton > button {
    background-color: #10A37F !important;
    color: white !important;
    border: none !important;
    padding: 0.5rem 1rem !important;
    border-radius: 0.25rem !important;
    cursor: pointer !important;
    transition: background-color 0.2s !important;
}

.stButton > button:hover {
    background-color: #0D8E6C !important;
}

/* Headers styling */
h1, h2, h3 {
    color: #ECECF1 !important;
    font-weight: 600;
}

/* Code block styling */
pre {
    background-color: #2A2B32 !important;
    padding: 1rem !important;
    border-radius: 0.5rem !important;
    margin: 1rem 0 !important;
}

code {
    color: #10A37F !important;
}

/* Markdown content styling */
.stMarkdown {
    color: #ECECF1;
}

/* Hide Streamlit branding */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

/* Claude-like container */
.claude-container {
    display: flex;
    flex-direction: column;
    height: 100vh;
    max-width: 800px;
    margin: 0 auto;
    padding: 1rem;
}

.messages-container {
    flex-grow: 1;
    overflow-y: auto;
    margin-bottom: 1rem;
}

.input-area {
    position: sticky;
    bottom: 0;
    background-color: #343541;
    padding: 1rem 0;
    border-top: 1px solid #565869;
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
</style>
""", unsafe_allow_html=True)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "conversations" not in st.session_state:
    st.session_state.conversations = []
if "current_conversation_index" not in st.session_state:
    st.session_state.current_conversation_index = 0
if "session_id" not in st.session_state:
    st.session_state.session_id = str(datetime.now().timestamp())
if "model" not in st.session_state:
    st.session_state.model = "claude"
if 'should_clear_input' not in st.session_state:
    st.session_state.should_clear_input = False
if 'user_input' not in st.session_state:
    st.session_state.user_input = ""
if "processing" not in st.session_state:
    st.session_state.processing = False
if "last_request_id" not in st.session_state:
    st.session_state.last_request_id = None
if "processed_queries" not in st.session_state:
    st.session_state.processed_queries = set()

def process_query(query: str):
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
    with st.container():
        if message["role"] == "user":
            st.markdown(f'<div class="chat-message user-message"><div class="message-content">👤 {message["content"]}</div></div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="chat-message bot-message"><div class="message-content">🤖 {message["content"]}</div></div>', unsafe_allow_html=True)
            if "visualization" in message and message["visualization"]:
                try:
                    st.markdown('<div class="visualization-container">', unsafe_allow_html=True)
                    # Check if the visualization is a JSON string (Plotly chart)
                    try:
                        vis_data = json.loads(message["visualization"])
                        # Create a plotly figure from the JSON
                        fig = go.Figure(vis_data)
                        # Ensure the figure has correct layout settings for dark mode
                        fig.update_layout(
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(42,43,50,1)',
                            font=dict(color='#ECECF1'),
                            margin=dict(l=20, r=20, t=40, b=20)
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    except (json.JSONDecodeError, TypeError, ValueError) as e:
                        st.error(f"Error processing visualization: {str(e)}")
                        st.text(f"Raw visualization data type: {type(message['visualization'])}")
                        # If not a valid JSON, try as base64 image
                        try:
                            if isinstance(message["visualization"], str):
                                # Ensure the string is proper base64 by padding if necessary
                                visualization = message["visualization"]
                                # Add padding if needed
                                padding = len(visualization) % 4
                                if padding:
                                    visualization += '=' * (4 - padding)
                                image = Image.open(BytesIO(base64.b64decode(visualization)))
                                st.image(image, use_column_width=True)
                        except Exception as img_error:
                            st.error(f"Could not display visualization: {str(img_error)}")
                    st.markdown('</div>', unsafe_allow_html=True)
                except Exception as e:
                    st.error(f"Error displaying visualization: {str(e)}")

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
st.markdown("Built with Streamlit and LangGraph") 