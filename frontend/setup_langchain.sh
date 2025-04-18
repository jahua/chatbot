#!/bin/bash

echo "Setting up LangChain integration for Tourism Analytics Chatbot..."

# Check if pip is installed
if ! command -v pip &> /dev/null; then
    echo "Error: pip is not installed. Please install Python and pip first."
    exit 1
fi

# Check if requirements_langchain.txt exists
if [ ! -f "requirements_langchain.txt" ]; then
    echo "Error: requirements_langchain.txt not found in the current directory."
    echo "Please run this script from the frontend directory."
    exit 1
fi

# Install dependencies
echo "Installing LangChain dependencies..."
pip install -r requirements_langchain.txt

# Check for .env file
if [ ! -f "../.env" ]; then
    echo "Warning: .env file not found in the parent directory."
    echo "Creating a template .env file. Please fill in your actual values."
    
    cat > "../.env" << EOL
# Database Connection
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=5432
POSTGRES_DB=your_database_name
POSTGRES_USER=your_database_user
POSTGRES_PASSWORD=your_database_password

# OpenAI API Key (required for LangChain)
OPENAI_API_KEY=your_openai_api_key
EOL
    
    echo "A template .env file has been created in the parent directory."
fi

echo "Setup complete! You can now run the application with: streamlit run app.py"
echo "Navigate to the SQL Explorer page to use the LangChain integration." 