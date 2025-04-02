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

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configure the page
st.set_page_config(
    page_title="Tourism Data Chatbot",
    page_icon="🏔️",
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
    st.session_state.session_id = str(uuid.uuid4())
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
st.title("🏔️ Tourism Data Chatbot")

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Display visualization if available
        if "plot" in message:
            st.plotly_chart(message["plot"], use_container_width=True)
        if "statistics" in message:
            st.json(message["statistics"])
        
        # Display SQL query if available
        if "sql_query" in message and message["sql_query"]:
            with st.expander("View SQL Query"):
                st.code(message["sql_query"], language="sql")

# Accept user input
if prompt := st.chat_input("What would you like to know about tourism in Switzerland?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        try:
            # Prepare the request data
            request_data = {
                "message": prompt,
                "session_id": st.session_state.get("session_id", "default_session")
            }

            # Send request to backend
            response = requests.post(
                "http://localhost:8001/api/chat/message",
                json=request_data
            )
            
            # Process the response
            if response.status_code == 200:
                response_data = response.json()
                
                # Display the main response message
                st.markdown(response_data.get("message", "No response available"))
                
                # Display SQL query if available
                if sql_query := response_data.get("sql_query"):
                    with st.expander("View SQL Query"):
                        st.code(sql_query, language="sql")
                
                # Display results if available
                if results := response_data.get("results"):
                    if isinstance(results, dict) and "data" in results:
                        data = results["data"]
                        if data and len(data) > 0:
                            try:
                                # Convert the string representation of tuples to actual data
                                if isinstance(data, str):
                                    data = ast.literal_eval(data)
                                
                                # Convert the tuple data to list of dictionaries
                                processed_data = []
                                for row in data:
                                    # Convert Decimal strings to integers
                                    week = int(float(str(row[0])))
                                    swiss = int(float(str(row[1])))
                                    foreign = int(float(str(row[2])))
                                    total = int(float(str(row[3])))
                                    
                                    processed_data.append({
                                        "Week": week,
                                        "Swiss Tourists": swiss,
                                        "Foreign Tourists": foreign,
                                        "Total Visitors": total
                                    })
                                
                                # Convert to DataFrame
                                df = pd.DataFrame(processed_data)
                                
                                # Display the data in a table
                                st.write("### Weekly Visitor Statistics")
                                st.dataframe(df, use_container_width=True)
                                
                                # Create and display visualization
                                fig = px.line(df, 
                                            x="Week", 
                                            y=["Swiss Tourists", "Foreign Tourists", "Total Visitors"],
                                            title="Weekly Visitor Patterns in Spring 2023",
                                            labels={"value": "Number of Visitors", 
                                                   "Week": "Week Number",
                                                   "variable": "Visitor Type"})
                                fig.update_layout(
                                    xaxis_title="Week Number",
                                    yaxis_title="Number of Visitors",
                                    hovermode='x unified'
                                )
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Display key statistics
                                st.write("### Key Statistics")
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.metric("Peak Week", f"Week {df['Week'][df['Total Visitors'].idxmax()]}")
                                    st.metric("Peak Total Visitors", f"{df['Total Visitors'].max():,}")
                                    st.metric("Average Total Visitors", f"{int(df['Total Visitors'].mean()):,}")
                                
                                with col2:
                                    st.metric("Peak Swiss Tourists", f"{df['Swiss Tourists'].max():,}")
                                    st.metric("Peak Foreign Tourists", f"{df['Foreign Tourists'].max():,}")
                                    st.metric("Foreign Tourist Ratio", f"{(df['Foreign Tourists'].sum() / df['Total Visitors'].sum() * 100):.1f}%")
                            
                            except Exception as e:
                                st.error(f"Error processing data: {str(e)}")
                                st.write("Raw data:", data)
                
                # Add assistant response to chat history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response_data.get("message", "No response available")
                })
            else:
                error_message = f"Error: {response.status_code} - {response.text}"
                st.error(error_message)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_message
                })
                
        except Exception as e:
            error_message = f"Error: {str(e)}"
            st.error(error_message)
            st.session_state.messages.append({
                "role": "assistant",
                "content": error_message
            })

# Add a clear chat button in the sidebar
with st.sidebar:
    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.session_state.chat_history = []
        st.session_state.current_visualization = None
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.model = "claude"
        st.rerun()

# Footer
st.markdown("---")
st.markdown("Built with Streamlit and LangGraph") 