import streamlit as st
import pandas as pd
import json
import uuid
import os
import sys
from typing import List, Dict, Any, Optional

# Add the parent directory to sys.path to import langchain_integration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from langchain_integration import get_langchain_helper

# Page configuration
st.set_page_config(
    page_title="SQL Explorer - Tourism Analytics",
    page_icon="🔍",
    layout="wide"
)

# Initialize session state for SQL Explorer
if "sql_messages" not in st.session_state:
    st.session_state.sql_messages = []
if "sql_history" not in st.session_state:
    st.session_state.sql_history = []
if "sql_processing" not in st.session_state:
    st.session_state.sql_processing = False
if "sql_query_mode" not in st.session_state:
    st.session_state.sql_query_mode = "natural_language"  # "natural_language" or "sql"

# Add Tailwind CSS styling
st.markdown("""
<link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
<style>
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        display: flex;
        flex-direction: column;
    }
    .user-message {
        background-color: #e2f0ff;
        border-left: 5px solid #1E88E5;
    }
    .assistant-message {
        background-color: #f0f2f6;
        border-left: 5px solid #7E57C2;
    }
    .sql-block {
        background-color: #2d2d2d;
        color: #f8f8f2;
        padding: 1rem;
        border-radius: 0.5rem;
        font-family: monospace;
        white-space: pre-wrap;
        margin-top: 0.5rem;
    }
    .code-header {
        background-color: #1e1e1e;
        color: #f8f8f2;
        padding: 0.5rem 1rem;
        border-top-left-radius: 0.5rem;
        border-top-right-radius: 0.5rem;
        font-family: monospace;
    }
    .stButton > button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)


def process_sql_query(query: str, is_sql: bool = False):
    """Process a query using LangChain and update the chat interface"""
    if not query:
        return
    
    # Add user message
    st.session_state.sql_messages.append({
        "role": "user", 
        "content": query,
        "id": str(uuid.uuid4())
    })
    
    # Set processing flag
    st.session_state.sql_processing = True
    
    # Get LangChain helper
    helper = get_langchain_helper()
    
    try:
        # Process the query
        if is_sql or st.session_state.sql_query_mode == "sql":
            result = helper.execute_sql_query(query)
        else:
            result = helper.process_natural_language_query(query)
        
        # Add assistant message with result
        content = result.get("content", "Sorry, I couldn't process your query.")
        st.session_state.sql_messages.append({
            "role": "assistant",
            "content": content,
            "result": result.get("result"),
            "success": result.get("success", False),
            "id": str(uuid.uuid4())
        })
        
        # Add to history
        st.session_state.sql_history.append({
            "query": query,
            "is_sql": is_sql,
            "timestamp": pd.Timestamp.now().isoformat()
        })
        
    except Exception as e:
        # Add error message
        st.session_state.sql_messages.append({
            "role": "assistant",
            "content": f"Error: {str(e)}",
            "success": False,
            "id": str(uuid.uuid4())
        })
    
    finally:
        # Clear processing flag
        st.session_state.sql_processing = False


def clear_chat():
    """Clear the chat history"""
    st.session_state.sql_messages = []


def display_sql_message(message: Dict[str, Any]):
    """Display a SQL chat message"""
    message_id = message.get("id", str(uuid.uuid4()))
    
    if message["role"] == "user":
        st.markdown(f"""
        <div class="chat-message user-message">
            <div class="font-bold">You</div>
            <div>{message["content"]}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="chat-message assistant-message">
            <div class="font-bold">SQL Assistant</div>
            <div>{message["content"]}</div>
        </div>
        """, unsafe_allow_html=True)
        
        # If there's a SQL result with a DataFrame, display it
        if message.get("result") and isinstance(message["result"], dict) and "output" in message["result"]:
            try:
                # Try to extract any SQL queries from the response
                if "intermediate_steps" in message["result"]:
                    for step in message["result"]["intermediate_steps"]:
                        if isinstance(step, tuple) and len(step) >= 2:
                            tool_input = step[0]
                            if "name" in tool_input and tool_input["name"] == "sql_db_query":
                                if "args" in tool_input and "query" in tool_input["args"]:
                                    sql_query = tool_input["args"]["query"]
                                    st.markdown("""
                                    <div class="code-header">SQL Query</div>
                                    """, unsafe_allow_html=True)
                                    st.code(sql_query, language="sql")
                
                # Try to extract any data from the response
                output = message["result"]["output"]
                if isinstance(output, str) and "[" in output and "]" in output:
                    # Try to find and parse table-like data in square brackets
                    try:
                        start_idx = output.find("[")
                        end_idx = output.rfind("]") + 1
                        if start_idx >= 0 and end_idx > start_idx:
                            data_str = output[start_idx:end_idx]
                            # Try to parse as JSON
                            try:
                                data = json.loads(data_str)
                                if isinstance(data, list) and len(data) > 0:
                                    df = pd.DataFrame(data)
                                    st.dataframe(df)
                            except:
                                pass
                    except:
                        pass
            except Exception as e:
                st.error(f"Error displaying results: {e}")


def render_sql_explorer():
    """Render the SQL Explorer page"""
    st.title("🔍 SQL Explorer powered by LangChain")
    
    # Create a two-column layout
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.subheader("Options")
        
        # Query mode selector
        query_mode = st.radio(
            "Query Mode",
            ["Natural Language", "SQL"],
            index=0 if st.session_state.sql_query_mode == "natural_language" else 1
        )
        st.session_state.sql_query_mode = "natural_language" if query_mode == "Natural Language" else "sql"
        
        # Clear chat button
        st.button("Clear Chat", on_click=clear_chat)
        
        # Query history
        if st.session_state.sql_history:
            st.subheader("Query History")
            for i, item in enumerate(reversed(st.session_state.sql_history[-10:])):
                query_text = item["query"]
                if len(query_text) > 30:
                    query_text = query_text[:27] + "..."
                
                query_type = "SQL" if item["is_sql"] else "NL"
                if st.button(f"{query_type}: {query_text}", key=f"history_{i}"):
                    process_sql_query(item["query"], item["is_sql"])
    
    with col1:
        # Chat display area
        st.subheader("Chat")
        
        # Display messages
        chat_container = st.container()
        with chat_container:
            for message in st.session_state.sql_messages:
                display_sql_message(message)
        
        # Query input area
        st.subheader("Ask a question or run SQL")
        query_placeholder = "Enter a SQL query..." if st.session_state.sql_query_mode == "sql" else "Ask a question about your data..."
        query = st.text_area(
            "Query",
            height=100,
            placeholder=query_placeholder,
            label_visibility="collapsed"
        )
        
        col1, col2 = st.columns([4, 1])
        with col2:
            submit_button = st.button("Submit")
        
        if submit_button and query:
            process_sql_query(query, st.session_state.sql_query_mode == "sql")
            st.rerun()


# Render the page
render_sql_explorer() 