#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Directory containing the service scripts
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Log file
LOG_FILE="service_startup.log"

# Function to log messages with timestamps
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check if a service started successfully
check_service() {
    local service_name="$1"
    local check_command="$2"
    
    if eval "$check_command"; then
        log_message "$service_name started successfully"
        return 0
    else
        log_message "ERROR: $service_name failed to start"
        return 1
    fi
}

# Clear previous log file
> "$LOG_FILE"

log_message "Starting services..."

# 1. Start RabbitMQ first since MinIO depends on it
log_message "Starting RabbitMQ..."
if ! bash "$SCRIPT_DIR/start_rabbitmq.sh"; then
    log_message "Failed to start RabbitMQ. Aborting startup sequence."
    exit 1
fi

# Wait for RabbitMQ to be fully operational
log_message "Waiting for RabbitMQ to be ready..."
sleep 10

# 2. Start Redis
log_message "Starting Redis..."
if ! bash "$SCRIPT_DIR/start_redis.sh"; then
    log_message "Failed to start Redis. Aborting startup sequence."
    exit 1
fi

# Wait for Redis to be ready
log_message "Waiting for Redis to be ready..."
sleep 5

# 3. Start MinIO (which depends on RabbitMQ)
log_message "Starting MinIO..."
if ! bash "$SCRIPT_DIR/start_minio.sh"; then
    log_message "Failed to start MinIO."
    exit 1
fi

# Verify all services are running
log_message "Verifying services..."

# Check RabbitMQ
if ! check_service "RabbitMQ" "sudo rabbitmqctl status >/dev/null 2>&1"; then
    log_message "RabbitMQ verification failed"
    exit 1
fi

# Check Redis
if ! check_service "Redis" "pgrep redis-server >/dev/null"; then
    log_message "Redis verification failed"
    exit 1
fi

# Check MinIO
if ! check_service "MinIO" "pgrep minio >/dev/null"; then
    log_message "MinIO verification failed"
    exit 1
fi

log_message "All services started successfully!"
echo "Services are ready to use. Check $LOG_FILE for detailed startup logs."
