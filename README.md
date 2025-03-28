# Tourism SQL RAG System

A RAG-enhanced Text-to-SQL system for tourism data analysis, featuring natural language querying of tourism data stored in PostgreSQL.

## Features

- Natural language to SQL query conversion
- RAG-enhanced schema understanding
- Real-time query execution
- Interactive chat interface
- Beautiful data visualization
- Local LLM support via Ollama

## Prerequisites

- Python 3.8+
- Node.js 18+
- PostgreSQL
- Ollama (for local LLM)
- ChromaDB (for vector storage)

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd tourism-sql-rag
```

2. Set up the backend:
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration
```

3. Set up the frontend:
```bash
cd frontend
npm install
```

4. Start Ollama and pull the model:
```bash
# Start Ollama service
ollama serve

# Pull the SQLCoder model
ollama pull sqlcoder:7b
```

5. Start ChromaDB:
```bash
# Start ChromaDB service
docker run -p 8000:8000 chromadb/chroma
```

## Running the Application

1. Start the backend:
```bash
# From the root directory
uvicorn main:app --reload
```

2. Start the frontend:
```bash
# From the frontend directory
npm run dev
```

3. Access the application:
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

## Usage

1. Open the chat interface in your browser
2. Type your question about tourism data in natural language
3. The system will:
   - Convert your question to SQL
   - Execute the query
   - Display the results
   - Provide a natural language explanation

## Example Queries

- "Show me the total number of visitors by region"
- "What are the top 5 tourist destinations?"
- "Compare visitor numbers between 2022 and 2023"
- "Which regions had the highest growth in tourism revenue?"

## Architecture

The system consists of several key components:

1. **Frontend**: Next.js application with a modern chat interface
2. **Backend**: FastAPI service handling:
   - Natural language processing
   - SQL query generation
   - Database interactions
   - RAG-enhanced context
3. **LLM**: Local SQLCoder model via Ollama
4. **Vector Store**: ChromaDB for schema understanding
5. **Database**: PostgreSQL for tourism data

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 