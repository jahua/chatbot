#!/bin/bash
# Script to rebuild the backend Docker image with the latest code changes

echo "Building new backend Docker image..."
docker build -t jahua/chatbot-backend:latest .

echo "Stopping and removing existing backend container..."
docker-compose stop backend
docker-compose rm -f backend

echo "Starting new backend container..."
docker-compose up -d backend

echo "Backend container rebuilt and started."
echo "You can test with: curl -X POST http://localhost:8080/chat -H \"Content-Type: application/json\" -d '{\"message\": \"Can you visualise the Swiss tourists and international tourists per month in a bar chart?\"}'" 