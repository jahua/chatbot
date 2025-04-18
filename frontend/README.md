# Tourism Analytics Chatbot - LangChain Integration

This project extends the Tourism Analytics Chatbot with LangChain integration for advanced SQL queries and natural language to SQL capabilities.

## Features

- Original chatbot capabilities for tourism data analysis
- New LangChain-powered SQL Explorer
- Natural language to SQL conversion
- Interactive SQL query execution
- Query history tracking

## Setup Instructions

### 1. Install Dependencies

First, install the required dependencies:

```bash
# Install base requirements
pip install -r requirements.txt

# Install LangChain-specific dependencies
pip install -r requirements_langchain.txt
```

### 2. Environment Variables

Make sure your `.env` file includes the following variables:

```
# Database Connection
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=5432
POSTGRES_DB=your_database_name
POSTGRES_USER=your_database_user
POSTGRES_PASSWORD=your_database_password

# OpenAI API Key (required for LangChain)
OPENAI_API_KEY=your_openai_api_key
```

### 3. Running the Application

Start the Streamlit application:

```bash
cd frontend
streamlit run app.py
```

The application will be available at `http://localhost:8501`.

## Pages

- **Chat Bot**: The main chatbot interface for tourism data analysis
- **Map Dashboard**: Geographic visualization of tourism data
- **Swisscom Insights**: Analytics using Swisscom data
- **SQL Explorer**: LangChain-powered SQL exploration interface

## Using the SQL Explorer

The SQL Explorer page allows you to:

1. Ask natural language questions about your data
2. Write and execute SQL queries directly
3. View query results in a tabular format
4. Access your query history

Toggle between "Natural Language" and "SQL" modes using the radio buttons in the Options panel.

## Development

### Adding New LangChain Capabilities

To add new LangChain capabilities:

1. Update the `langchain_integration.py` file
2. Extend the `LangChainSQLHelper` class with new methods
3. Call these methods from your Streamlit pages

### Customizing the SQL Explorer

To customize the SQL Explorer:

1. Modify `pages/3_🔍_SQL_Explorer.py`
2. Add new visualization options
3. Extend query processing capabilities

## Troubleshooting

- **Database Connection Issues**: Check your database connection parameters in the `.env` file
- **OpenAI API Key**: Ensure your OpenAI API key is valid and has sufficient credits
- **LangChain Errors**: Check the console output for detailed error messages

## License

[Insert your license information here] 