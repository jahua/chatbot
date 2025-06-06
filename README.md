# Tourism Data Chatbot

An intelligent chatbot system for analyzing tourism data in Switzerland, powered by OpenAI.

## Features

- Interactive chat interface built with Streamlit
- Backend API powered by FastAPI
- Data analysis and visualization capabilities
- Support for querying tourism statistics
- Real-time data visualization with Plotly
- Session management and chat history
- Fast dependency management with uv
- Docker containerization for easy deployment

## Tech Stack

- Python 3.11+
- FastAPI
- Streamlit
- PostgreSQL
- OpenAI
- Plotly
- Pandas
- uv (fast Python package installer)
- Docker & Docker Compose

## Setup

### Docker Setup (Recommended)

The easiest way to get started with the Tourism Data Chatbot is using Docker:

1. Clone the repository:
```bash
git clone https://github.com/jahua/chatbot.git
cd chatbot
```

2. Create a `.env` file:
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. Build and start the containers:
```bash
docker-compose up -d
```

This will start all required services:
- Backend API server
- Frontend Streamlit application
- PostgreSQL database

The application will be available at:
- Frontend: http://localhost:8501
- Backend API: http://localhost:8080

### Local Setup with uv (Alternative)

1. Clone the repository:
```bash
git clone https://github.com/jahua/chatbot.git
cd chatbot
```

2. Run the setup script:
```bash
python setup_env.py
```

3. Activate the virtual environment:
```bash
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

4. Run the setup script again (inside the virtual environment):
```bash
python setup_env.py
```

5. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

### Traditional Setup (without uv)

1. Clone the repository:
```bash
git clone https://github.com/jahua/chatbot.git
cd chatbot
```

2. Create and activate virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
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

## Running the Application

### Docker (Recommended)

If you've set up with Docker, the application is already running. To stop it:

```bash
docker-compose down
```

To update and restart:

```bash
git pull
docker-compose up -d --build
```

### Local Development

#### Easy Start (Single Command)

To start both the backend and frontend with a single command:

```bash
./start.sh
```

#### Manual Start

1. Start the backend server:
```bash
./run_server.py
# or
python run_server.py
```

2. Start the frontend application:
```bash
./run_frontend.py
# or
python run_frontend.py
```

The application will be available at:
- Frontend: http://localhost:8501
- Backend API: http://localhost:8080

## Using uv for Dependency Management

This project uses [uv](https://github.com/astral-sh/uv), a fast Python package installer and resolver. Benefits include:

- Up to 10-100x faster package installations
- Reliable dependency resolution
- Optimized for CI/CD pipelines
- Compatible with pip and requirements.txt

To use uv manually:

```bash
# Install a package
uv pip install package-name

# Install from requirements.txt
uv pip install -r requirements.txt

# Create a virtual environment
uv venv .venv
```

# Run backend server with uv
uv run uvicorn app.main:app

# Run frontend with uv
uv run streamlit run app.py

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
├── docker/               # Docker configuration
├── tests/               # Test suite
├── .env                 # Environment variables
├── docker-compose.yml   # Docker compose configuration
├── Dockerfile          # Docker image definition
├── pyproject.toml      # Project configuration
├── requirements.txt     # Python dependencies
├── setup_env.py        # Environment setup script
├── run_server.py       # Backend server runner
├── run_frontend.py     # Frontend runner
├── start.sh            # Complete startup script
└── README.md           # Project documentation
```

## Docker Implementation

The application is containerized using Docker for easy deployment and consistent environments across different systems.

### Docker Components

- **Backend Container**: Runs the FastAPI server
- **Frontend Container**: Runs the Streamlit interface
- **Database Container**: PostgreSQL for data storage

### Docker Configuration

The `docker-compose.yml` file defines the multi-container setup with proper networking, volume mounting, and environment variable passing. Key features include:

- Volume mounts for persistent data
- Automatic database initialization
- Health checks for service dependencies
- Environment variable configuration
- Port mapping for service access

### Building Custom Images

To build the images separately:

```bash
# Build backend
docker build -f docker/backend.Dockerfile -t tourism-chatbot-backend .

# Build frontend
docker build -f docker/frontend.Dockerfile -t tourism-chatbot-frontend .
```

### Production Deployment

For production deployment, additional security measures are recommended:

1. Use proper secrets management instead of environment files
2. Set up SSL/TLS termination
3. Configure proper database credentials
4. Implement authentication for the API

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
