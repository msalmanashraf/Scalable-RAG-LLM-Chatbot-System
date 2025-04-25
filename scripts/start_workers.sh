#!/bin/bash

# Get the directory of the current script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Define the app folder adjacent to the script folder
APP_DIR="$SCRIPT_DIR/../app"

# Create logs folder if it doesn't exist
LOG_DIR="$APP_DIR/logs"
mkdir -p "$LOG_DIR"

# Run the Python scripts from the app folder and save output to log files
python "$APP_DIR/workers/minio_event_worker.py" > "$LOG_DIR/minio_event_worker.log" 2>&1 &
python "$APP_DIR/workers/embedding_worker.py" > "$LOG_DIR/embedding_worker.log" 2>&1 &
python "$APP_DIR/workers/llm_worker.py" > "$LOG_DIR/llm_worker.log" 2>&1 &

