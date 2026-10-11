#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

MODEL_NAME="qwen3.5:9b" # Note: replace with qwen3.5:9b if available on Ollama registry

echo "🚀 Starting Docker Compose stack..."
docker compose up -d

echo "⏳ Waiting for Ollama container to be healthy..."
until docker inspect --format='{{.State.Health.Status}}' local_ollama 2>/dev/null | grep -q "healthy"; do
  sleep 2
  echo -n "."
done

echo -e "\n✅ Ollama is up and running!"

echo "📥 Pulling model: $MODEL_NAME (this may take a few minutes depending on your internet speed)..."
docker exec -it local_ollama ollama pull $MODEL_NAME

echo "🎉 Infrastructure setup complete! MongoDB is on port 27017 and Ollama is ready on port 11434."