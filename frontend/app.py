import streamlit as st
import json
import pandas as pd
from typing import Dict, Any, List
import requests
from datetime import datetime
import uuid
import os
import logging
import traceback
import time
import matplotlib.pyplot as plt

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
        if "data" in message and message["data"]:
            try:
                if isinstance(message["data"], list) and len(message["data"]) > 0:
                    df = pd.DataFrame(message["data"])
                    st.markdown("### Data Results")
                    st.dataframe(df, use_container_width=True)
            except Exception as e:
                logger.error(f"Error displaying data: {str(e)}")
        
        if "visualization" in message and message["visualization"]:
            try:
                st.markdown("### Visualization")
                viz_data = message["visualization"]
                df = pd.DataFrame(viz_data["data"])
                
                if viz_data["type"] == "line":
                    df[viz_data["y_axis"]] = df[viz_data["y_axis"]].astype(float)
                    st.line_chart(
                        data=df,
                        x=viz_data["x_axis"],
                        y=viz_data["y_axis"]
                    )
                elif viz_data["type"] == "area":
                    df[viz_data["y_axis"]] = df[viz_data["y_axis"]].astype(float)
                    st.area_chart(
                        data=df,
                        x=viz_data["x_axis"],
                        y=viz_data["y_axis"]
                    )
                elif viz_data["type"] == "bar":
                    st.bar_chart(df, x="category", y="value")
                elif viz_data["type"] == "scatter":
                    df[viz_data["x_axis"]] = df[viz_data["x_axis"]].astype(float)
                    df[viz_data["y_axis"]] = df[viz_data["y_axis"]].astype(float)
                    st.scatter_chart(
                        data=df,
                        x=viz_data["x_axis"],
                        y=viz_data["y_axis"]
                    )
                elif viz_data["type"] == "pie":
                    # For pie charts, we need to calculate percentages
                    total = sum(item["value"] for item in viz_data["data"])
                    labels = [item["category"] for item in viz_data["data"]]
                    sizes = [item["value"] for item in viz_data["data"]]
                    
                    # Create a figure and axis
                    fig, ax = plt.subplots()
                    ax.pie(sizes, labels=labels, autopct='%1.1f%%')
                    ax.axis('equal')
                    st.pyplot(fig)
                    plt.close(fig)
            except Exception as e:
                logger.error(f"Error displaying visualization: {str(e)}")
                st.error("Could not display visualization")
        
        st.markdown("### Analysis")
        st.write(message["content"])
        
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
        viz_placeholder = st.empty()
        analysis_placeholder = st.empty()
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
                step4 = st.empty()
                
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
                    
                    if data.get("data"):
                        step2.markdown("✅ **Step 2:** Retrieved data from database")
                        
                        # Display data if available
                        if data.get("data") and len(data.get("data")) > 0:
                            df = pd.DataFrame(data.get("data"))
                            data_placeholder.markdown("### Data Results")
                            data_placeholder.dataframe(df, use_container_width=True)
                        
                        # Step 3: Generating Visualization
                        step3.markdown("⏳ **Step 3:** Generating visualization...")
                        if data.get("visualization"):
                            try:
                                viz_data = data.get("visualization")
                                viz_placeholder.markdown("### Visualization")
                                if viz_data["type"] == "line":
                                    df = pd.DataFrame(viz_data["data"])
                                    df[viz_data["y_axis"]] = df[viz_data["y_axis"]].astype(float)
                                    viz_placeholder.line_chart(
                                        data=df,
                                        x=viz_data["x_axis"],
                                        y=viz_data["y_axis"]
                                    )
                                elif viz_data["type"] == "area":
                                    df = pd.DataFrame(viz_data["data"])
                                    df[viz_data["y_axis"]] = df[viz_data["y_axis"]].astype(float)
                                    viz_placeholder.area_chart(
                                        data=df,
                                        x=viz_data["x_axis"],
                                        y=viz_data["y_axis"]
                                    )
                                elif viz_data["type"] == "bar":
                                    df = pd.DataFrame(viz_data["data"])
                                    viz_placeholder.bar_chart(df, x="category", y="value")
                                elif viz_data["type"] == "scatter":
                                    df = pd.DataFrame(viz_data["data"])
                                    df[viz_data["x_axis"]] = df[viz_data["x_axis"]].astype(float)
                                    df[viz_data["y_axis"]] = df[viz_data["y_axis"]].astype(float)
                                    viz_placeholder.scatter_chart(
                                        data=df,
                                        x=viz_data["x_axis"],
                                        y=viz_data["y_axis"]
                                    )
                                elif viz_data["type"] == "pie":
                                    # For pie charts, we need to calculate percentages
                                    total = sum(item["value"] for item in viz_data["data"])
                                    labels = [item["category"] for item in viz_data["data"]]
                                    sizes = [item["value"] for item in viz_data["data"]]
                                    
                                    # Create a figure and axis
                                    fig, ax = plt.subplots()
                                    ax.pie(sizes, labels=labels, autopct='%1.1f%%')
                                    ax.axis('equal')
                                    viz_placeholder.pyplot(fig)
                                    plt.close(fig)
                                step3.markdown("✅ **Step 3:** Visualization generated")
                            except Exception as e:
                                logger.error(f"Error displaying visualization: {str(e)}")
                                step3.markdown("❌ **Step 3:** Error generating visualization")
                        else:
                            step3.markdown("ℹ️ **Step 3:** No visualization available for this query")
                        
                        # Step 4: Analyzing Data and Generating Summary
                        step4.markdown("⏳ **Step 4:** Analyzing data and generating summary...")
                        
                        if data.get("response"):
                            step4.markdown("✅ **Step 4:** Analysis complete")
                            # Show the final response
                            analysis_placeholder.markdown("### Analysis")
                            analysis_placeholder.markdown(data.get("response", "No response received"))
                            
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
                "sql_query": data.get("sql_query", None),
                "visualization": data.get("visualization", None)
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
        st.session_state.session_id = str(datetime.now().timestamp())
        st.session_state.model = "claude"
        st.rerun()

# Footer
st.markdown("---")
st.markdown("Built with Streamlit and LangGraph") 