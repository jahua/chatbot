#!/bin/bash
# Tourism Data Chatbot - Startup Script
# This script starts both the backend and frontend services

# Set strict mode
set -e

echo "🚀 Starting Tourism Data Chatbot..."

# Check if Python and uv are available
if ! command -v python &> /dev/null; then
    echo "❌ Python is not installed. Please install Python 3.11+ and try again."
    exit 1
fi

if ! command -v uv &> /dev/null; then
    echo "🔄 Installing uv package manager..."
    pip install uv
fi

# Ensure we're in a virtual environment
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "⚠️ Not running in a virtual environment."
    echo "Please activate the virtual environment first:"
    echo "    source venv311/bin/activate"
    echo "Or run the setup script to create one:"
    echo "    python setup_env.py"
    exit 1
fi

# Start the backend server in the background
echo "🔄 Starting backend server on port 8000..."
python run_server.py &
BACKEND_PID=$!

# Wait for the backend to start up
echo "⏳ Waiting for backend to start..."
sleep 3

# Start the frontend
echo "🔄 Starting Streamlit frontend..."
python run_frontend.py &
FRONTEND_PID=$!

# Trap Ctrl+C to clean up processes
trap cleanup INT
cleanup() {
    echo -e "\n🛑 Stopping all services..."
    kill $BACKEND_PID 2>/dev/null || true
    kill $FRONTEND_PID 2>/dev/null || true
    pkill -f "streamlit run" 2>/dev/null || true
    pkill -f "uvicorn" 2>/dev/null || true
    echo "✅ All services stopped"
    exit 0
}

# Keep script running until Ctrl+C
echo -e "\n✨ Tourism Data Chatbot is running!"
echo "📊 Frontend: http://localhost:8501"
echo "🔌 Backend API: http://localhost:8000"
echo "Press Ctrl+C to stop all services"

# Wait for children processes
wait 

