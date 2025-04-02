# Tourism Data Chatbot

An intelligent chatbot system for analyzing tourism data in Switzerland, powered by Claude AI.

## Features

- Interactive chat interface built with Streamlit
- Backend API powered by FastAPI
- Data analysis and visualization capabilities
- Support for querying tourism statistics
- Real-time data visualization with Plotly
- Session management and chat history

## Tech Stack

- Python 3.11+
- FastAPI
- Streamlit
- PostgreSQL
- Claude AI
- Plotly
- Pandas

## Setup

1. Clone the repository:
```bash
git clone https://github.com/jahua/chatbot.git
cd chatbot
```

2. Create and activate virtual environment:
```bash
python -m venv venv311
source venv311/bin/activate  # On Windows: venv311\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Initialize the database:
```bash
python initialize_db.py
```

## Running the Application

1. Start the backend server:
```bash
cd chatbot
python -m uvicorn app.main:app --port 8001 --reload
```

2. Start the frontend application:
```bash
cd frontend
streamlit run app.py
```

The application will be available at:
- Frontend: http://localhost:8501
- Backend API: http://localhost:8001

## Project Structure

```
chatbot/
├── app/                    # Backend application
│   ├── core/              # Core functionality
│   ├── db/                # Database models and connection
│   ├── llm/              # LLM integration
│   ├── models/           # Data models
│   ├── schemas/          # Pydantic schemas
│   └── services/         # Business logic
├── frontend/             # Streamlit frontend
├── tests/               # Test suite
├── .env                 # Environment variables
├── requirements.txt     # Python dependencies
└── README.md           # Project documentation
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
