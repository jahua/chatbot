#!/usr/bin/env python3
"""
Tourism Data Chatbot Frontend Runner

This script runs the Streamlit frontend with uv dependency management.
"""

import os
import subprocess
import sys
import time

def check_dependencies():
    """Ensure all frontend dependencies are installed using uv."""
    try:
        # Check if streamlit is installed using uv
        subprocess.run(
            ["uv", "pip", "freeze"],
            check=True,
            capture_output=True,
            text=True
        )
        print("Dependencies verified.")
    except subprocess.CalledProcessError:
        print("Warning: Failed to verify dependencies with uv.")
        print("Installing streamlit and other dependencies...")
        try:
            subprocess.run(
                ["uv", "pip", "install", "streamlit", "plotly", "pandas", "requests"],
                check=True
            )
        except Exception as e:
            print(f"Error installing dependencies: {e}")
            sys.exit(1)

def run_streamlit():
    """Run the Streamlit frontend application."""
    frontend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
    
    if not os.path.exists(os.path.join(frontend_dir, "app.py")):
        print(f"Error: Frontend app not found at {frontend_dir}/app.py")
        sys.exit(1)
    
    print(f"Starting Streamlit frontend from {frontend_dir}...")
    
    # Kill any existing Streamlit processes
    try:
        subprocess.run(["pkill", "-f", "streamlit run"], check=False)
        # Small delay to ensure processes are terminated
        time.sleep(1)
    except Exception:
        pass
    
    # Change to the frontend directory
    os.chdir(frontend_dir)
    
    # Run Streamlit
    streamlit_process = subprocess.Popen(
        ["streamlit", "run", "app.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True
    )
    
    # Print URL information
    for line in streamlit_process.stdout:
        print(line, end="")
        # Once we see the URL is ready, we can stop capturing output
        if "You can now view your Streamlit app in your browser." in line:
            break
    
    print("\nStreamlit frontend is running!")
    print("Press Ctrl+C to stop the frontend.")
    
    try:
        # Keep the script running
        streamlit_process.wait()
    except KeyboardInterrupt:
        print("\nStopping Streamlit frontend...")
        streamlit_process.terminate()
        print("Streamlit frontend stopped.")

if __name__ == "__main__":
    # Check dependencies
    check_dependencies()
    
    # Run Streamlit
    run_streamlit() 