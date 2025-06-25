#!/bin/bash

# Function to handle the termination of the script
cleanup() {
  echo "Stopping redis container..."
  docker compose down redis
  exit
}

# Trap SIGINT (Ctrl+C) to call the cleanup function
trap cleanup SIGINT

# Start the redis container
docker compose up -d redis
echo "redis container started."

# Navigate to the 'next' directory and run the development server
cd server && npm run dev &

# Wait until the script is interrupted
wait