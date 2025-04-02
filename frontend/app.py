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
st.title("🏔️ Tourism Data Chatbot")

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "data" in message:
            display_data = message["data"]
            if isinstance(display_data, pd.DataFrame):
                st.dataframe(display_data, use_container_width=True)
        if "plot" in message:
            st.plotly_chart(message["plot"], use_container_width=True)
        if "summary" in message:
            with st.expander("View Analysis Summary"):
                st.markdown(message["summary"])

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
                "content": prompt,
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
                if response_data.get("sql_query"):
                    with st.expander("View SQL Query", expanded=True):
                        st.code(response_data["sql_query"], language="sql")
                
                # Display visualization if available
                if response_data.get("visualization"):
                    viz_data = response_data["visualization"]
                    if viz_data["type"] == "plotly":
                        try:
                            fig = go.Figure(viz_data["data"])
                            st.plotly_chart(
                                fig, 
                                use_container_width=True,
                                config=viz_data.get("config", {
                                    'displayModeBar': True,
                                    'responsive': True
                                })
                            )
                        except Exception as e:
                            st.error(f"Error displaying visualization: {str(e)}")
                
                # Display analysis summary if available
                if response_data.get("analysis_summary"):
                    with st.expander("View Analysis Summary", expanded=True):
                        st.markdown(response_data["analysis_summary"])
                
                # Display raw data if available
                if response_data.get("data"):
                    with st.expander("View Raw Data", expanded=True):
                        df = pd.DataFrame(response_data["data"])
                        st.dataframe(
                            df.style.format({
                                'foreign_tourists': '{:,.0f}',
                                'swiss_tourists': '{:,.0f}',
                                'total_visitors': '{:,.0f}'
                            }),
                            use_container_width=True
                        )
                
                # Initialize message content for chat history
                chat_message = {
                    "role": "assistant",
                    "content": response_data.get("message", "No response available")
                }
                
                # Process and display results if available
                if results := response_data.get("results"):
                    if isinstance(results, dict) and "data" in results:
                        data = results["data"]
                        if data and len(data) > 0:
                            try:
                                # Convert to DataFrame
                                df = pd.DataFrame(data)
                                
                                # Display the data in a table
                                st.write("### Weekly Visitor Statistics")
                                st.dataframe(df.style.format(thousands=","), use_container_width=True)
                                
                                # Create visualization tabs
                                viz_tab1, viz_tab2 = st.tabs(["📈 Interactive Plot", "📊 Simple Plot"])
                                
                                with viz_tab1:
                                    # Create interactive Plotly visualization
                                    fig = go.Figure()
                                    
                                    # Add traces for each visitor type
                                    fig.add_trace(go.Scatter(
                                        x=df['Week'],
                                        y=df['Swiss Tourists'],
                                        name='Swiss Tourists',
                                        mode='lines+markers',
                                        line=dict(width=2, color='#1f77b4'),
                                        marker=dict(size=8)
                                    ))
                                    fig.add_trace(go.Scatter(
                                        x=df['Week'],
                                        y=df['Foreign Tourists'],
                                        name='Foreign Tourists',
                                        mode='lines+markers',
                                        line=dict(width=2, color='#ff7f0e'),
                                        marker=dict(size=8)
                                    ))
                                    fig.add_trace(go.Scatter(
                                        x=df['Week'],
                                        y=df['Total Visitors'],
                                        name='Total Visitors',
                                        mode='lines+markers',
                                        line=dict(dash='dash', width=2, color='#2ca02c'),
                                        marker=dict(size=8)
                                    ))
                                    
                                    fig.update_layout(
                                        title={
                                            'text': 'Weekly Visitor Patterns in Spring 2023',
                                            'y': 0.95,
                                            'x': 0.5,
                                            'xanchor': 'center',
                                            'yanchor': 'top',
                                            'font': dict(size=20)
                                        },
                                        xaxis_title='Week Number',
                                        yaxis_title='Number of Visitors',
                                        hovermode='x unified',
                                        showlegend=True,
                                        height=600,
                                        template='plotly_dark',
                                        legend=dict(
                                            yanchor="top",
                                            y=0.99,
                                            xanchor="left",
                                            x=0.01,
                                            bgcolor='rgba(0,0,0,0.5)',
                                            bordercolor='rgba(255,255,255,0.2)',
                                            borderwidth=1
                                        ),
                                        plot_bgcolor='rgba(0,0,0,0)',
                                        paper_bgcolor='rgba(0,0,0,0)'
                                    )
                                    
                                    fig.update_xaxes(
                                        tickmode='linear',
                                        dtick=1,
                                        gridcolor='rgba(128, 128, 128, 0.2)',
                                        title_font=dict(size=14),
                                        tickfont=dict(size=12),
                                        showgrid=True,
                                        zeroline=False
                                    )
                                    fig.update_yaxes(
                                        gridcolor='rgba(128, 128, 128, 0.2)',
                                        title_font=dict(size=14),
                                        tickfont=dict(size=12),
                                        tickformat=',d',
                                        showgrid=True,
                                        zeroline=False
                                    )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                                
                                with viz_tab2:
                                    # Create simple Streamlit line chart
                                    st.line_chart(
                                        df.set_index('Week')[['Swiss Tourists', 'Foreign Tourists', 'Total Visitors']],
                                        use_container_width=True
                                    )
                                
                                # Display insights if available
                                if insights := results.get("insights"):
                                    with st.expander("📊 View Analysis", expanded=True):
                                        st.markdown(insights)
                                
                                # Calculate and display key metrics
                                col1, col2, col3 = st.columns(3)
                                
                                with col1:
                                    peak_total = df['Total Visitors'].max()
                                    peak_week = df.loc[df['Total Visitors'].idxmax(), 'Week']
                                    st.metric(
                                        "Peak Visitors",
                                        f"{peak_total:,}",
                                        f"Week {int(peak_week)}"
                                    )
                                
                                with col2:
                                    avg_total = df['Total Visitors'].mean()
                                    st.metric(
                                        "Average Weekly Visitors",
                                        f"{int(avg_total):,}"
                                    )
                                
                                with col3:
                                    swiss_share = (df['Swiss Tourists'].sum() / df['Total Visitors'].sum() * 100)
                                    st.metric(
                                        "Swiss Tourist Share",
                                        f"{swiss_share:.1f}%"
                                    )
                                
                            except Exception as e:
                                st.error(f"Error processing data: {str(e)}")
                                st.write("Raw data:", data)
                
                # Display summary if available
                if response_data.get("summary"):
                    with st.expander("View Data Summary"):
                        st.markdown(response_data["summary"])
                
                # Add message to chat history
                st.session_state.messages.append(chat_message)
                
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
        st.session_state.session_id = str(datetime.now().timestamp())
        st.session_state.model = "claude"
        st.rerun()

# Footer
st.markdown("---")
st.markdown("Built with Streamlit and LangGraph") 