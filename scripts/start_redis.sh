#!/bin/bash

# Function to check if Redis is running
is_redis_running() {
    pgrep redis-server > /dev/null
}

 Define directories and config paths
SYNC_CACHE_PYTHON_SCRIPT="../app/services/sync_cache_to_redis.py"  # Path to the Python script

# Check if Redis is installed
if dpkg -l | grep -q redis-server; then
    echo "Redis is already installed."
else
    echo "Redis is not installed. Installing Redis..."
    
    # Update package list and install Redis
    sudo apt update && sudo apt install -y redis-server

    # Enable Redis to start on boot
    # sudo systemctl enable redis
fi

if is_redis_running; then
    echo "Redis is already running."
else
	# Start Redis service (Commented out for regular Ubuntu)
	# echo "Starting Redis..."
	# sudo systemctl start redis

	# Start Redis in WSL
	echo "Starting Redis in WSL..."
	redis-server --daemonize yes
fi

# Run the Python script for caching response in redis from duckdb
if [ -f "$SYNC_CACHE_PYTHON_SCRIPT" ]; then
    echo "Running Python script for syncing cache..."
    python3 "$SYNC_CACHE_PYTHON_SCRIPT" "../app/embeddings/embeddings.duckdb"
else
    echo "Python script not found at $SYNC_CACHE_PYTHON_SCRIPT"
    exit 1
fi

# Check Redis status
if pgrep redis-server > /dev/null; then
    echo "Redis is running successfully in WSL."
else
    echo "Failed to start Redis in WSL."
fi
