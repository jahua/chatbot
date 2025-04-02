import streamlit as st
import json
import plotly
import pandas as pd
from typing import Dict, Any, List
import requests
from datetime import datetime
import uuid
import os
import logging
import traceback
import plotly.express as px
import ast
import plotly.graph_objects as go
import time

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configure the page
st.set_page_config(
    page_title="Tourism Data Analysis Chatbot",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "current_visualization" not in st.session_state:
    st.session_state.current_visualization = None
if "session_id" not in st.session_state:
    st.session_state.session_id = str(datetime.now().timestamp())
if "model" not in st.session_state:
    st.session_state.model = "claude"

# Sidebar for model selection and settings
with st.sidebar:
    st.title("Settings")
    st.session_state.model = st.selectbox(
        "Select Model",
        ["claude"],
        index=0
    )
    
    st.markdown("---")
    st.markdown("### About")
    st.markdown("""
    This chatbot helps you explore tourism data in Switzerland.
    You can ask questions about visitor patterns, demographics, and more.
    """)

# Main chat interface
st.title("Tourism Data Analysis Chatbot")
st.markdown("""
Ask questions about:
- Visitor statistics
- Spending patterns
- Seasonal trends
- Industry analysis
""")

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if "data" in message and message["data"]:
            try:
                if isinstance(message["data"], list) and len(message["data"]) > 0:
                    df = pd.DataFrame(message["data"])
                    st.dataframe(df, use_container_width=True)
            except Exception as e:
                logger.error(f"Error displaying data: {str(e)}")
        if "plot" in message and message["plot"]:
            try:
                st.plotly_chart(message["plot"], use_container_width=True)
            except Exception as e:
                logger.error(f"Error displaying plot: {str(e)}")
        if "sql_query" in message and message["sql_query"]:
            with st.expander("View Generated SQL Query"):
                st.code(message["sql_query"], language="sql")

# Accept user input
if prompt := st.chat_input("Type your question here..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.write(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        data_placeholder = st.empty()
        sql_placeholder = st.empty()
        
        # Show initial processing message
        message_placeholder.markdown("_Processing your question..._")
        
        # Process steps with status indicators
        status_container = st.container()
        
        try:
            with status_container:
                step1 = st.empty()
                step2 = st.empty()
                step3 = st.empty()
                
                # Step 1: Generating SQL Query
                step1.markdown("⏳ **Step 1:** Generating SQL query...")
                
                # Send request to backend
                response = requests.post(
                    "http://localhost:8001/api/chat/message",
                    json={
                        "content": prompt,
                        "role": "user",
                        "model": "openai",
                        "session_id": str(datetime.now().timestamp())
                    }
                )
                response.raise_for_status()
                data = response.json()
                
                if data.get("sql_query"):
                    step1.markdown("✅ **Step 1:** Generated SQL query")
                    # Step 2: Executing Database Query
                    step2.markdown("⏳ **Step 2:** Executing database query...")
                    # Short delay to simulate processing time
                    time.sleep(0.5)
                    
                    if data.get("data"):
                        step2.markdown("✅ **Step 2:** Retrieved data from database")
                        
                        # Display data if available
                        if data.get("data") and len(data.get("data")) > 0:
                            df = pd.DataFrame(data.get("data"))
                            data_placeholder.dataframe(df, use_container_width=True)
                        
                        # Step 3: Generating Response
                        step3.markdown("⏳ **Step 3:** Analyzing data and generating response...")
                        # Short delay to simulate processing time
                        time.sleep(0.5)
                        
                        if data.get("response"):
                            step3.markdown("✅ **Step 3:** Analysis complete")
                            # Remove the temporary processing message and show the final response
                            message_placeholder.markdown(data.get("response", "No response received"))
                            
                            # Show SQL query in expander
                            if data.get("sql_query"):
                                with sql_placeholder.expander("View Generated SQL Query"):
                                    st.code(data.get("sql_query"), language="sql")
                    else:
                        step2.markdown("❌ **Step 2:** Error executing database query")
                        if data.get("error"):
                            message_placeholder.markdown(f"**Error:** {data.get('error')}")
                        else:
                            message_placeholder.markdown("**Error:** Failed to execute database query")
                else:
                    step1.markdown("❌ **Step 1:** Could not generate SQL query")
                    message_placeholder.markdown(data.get("response", "I couldn't generate a SQL query for your question."))
                
            # Add assistant response to chat history
            st.session_state.messages.append({
                "role": "assistant",
                "content": data.get("response", "No response received"),
                "data": data.get("data", []),
                "sql_query": data.get("sql_query", None)
            })
            
            # Remove status indicators after successful processing
            status_container.empty()
            
        except requests.exceptions.RequestException as e:
            message_placeholder.markdown(f"**Error:** {str(e)}")
            logger.error(f"Error sending message: {str(e)}")

# Add a clear chat button in the sidebar
with st.sidebar:
    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.session_state.chat_history = []
        st.session_state.current_visualization = None
        st.session_state.session_id = str(datetime.now().timestamp())
        st.session_state.model = "claude"
        st.rerun()

# Footer
st.markdown("---")
st.markdown("Built with Streamlit and LangGraph") 