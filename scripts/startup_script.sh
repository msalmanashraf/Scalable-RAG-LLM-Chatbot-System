#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the directory of the current script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$SCRIPT_DIR/../app"
LOG_DIR="$APP_DIR/logs"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log files
SERVICE_LOG_FILE="$LOG_DIR/service_startup.log"
RABBITMQ_LOG="$LOG_DIR/rabbitmq.log"
REDIS_LOG="$LOG_DIR/redis.log"
MINIO_LOG="$LOG_DIR/minio.log"

# Function to log messages with timestamps
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$SERVICE_LOG_FILE"
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

# Function to start a worker
start_worker() {
    local worker_name="$1"
    local worker_script="$2"
    local log_file="$3"
    
    log_message "Starting $worker_name worker..."
    python "$worker_script" > "$log_file" 2>&1 &
    local worker_pid=$!
    sleep 2
    
    if kill -0 $worker_pid 2>/dev/null; then
        log_message "$worker_name worker started successfully (PID: $worker_pid)"
    else
        log_message "ERROR: $worker_name worker failed to start"
        return 1
    fi
}

# Clear previous service log file
> "$SERVICE_LOG_FILE"

log_message "Starting services and workers..."
log_message "All logs will be stored in: $LOG_DIR"

# 1. Start RabbitMQ
log_message "Starting RabbitMQ..."
if ! bash "$SCRIPT_DIR/start_rabbitmq.sh" > "$RABBITMQ_LOG" 2>&1; then
    log_message "Failed to start RabbitMQ. Aborting startup sequence."
    exit 1
fi

# Wait for RabbitMQ to be fully operational
log_message "Waiting for RabbitMQ to be ready..."
sleep 10

# 2. Start Redis
log_message "Starting Redis..."
if ! bash "$SCRIPT_DIR/start_redis.sh" > "$REDIS_LOG" 2>&1; then
    log_message "Failed to start Redis. Aborting startup sequence."
    exit 1
fi

# Wait for Redis to be ready
log_message "Waiting for Redis to be ready..."
sleep 5

# 3. Start MinIO
log_message "Starting MinIO..."
if ! bash "$SCRIPT_DIR/start_minio.sh" > "$MINIO_LOG" 2>&1; then
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

log_message "All services started successfully! Starting workers..."

# Start workers
start_worker "MinIO Event" "$APP_DIR/workers/minio_event_worker.py" "$LOG_DIR/minio_event_worker.log"
start_worker "Embedding" "$APP_DIR/workers/embedding_worker.py" "$LOG_DIR/embedding_worker.log"
start_worker "LLM" "$APP_DIR/workers/llm_worker.log" "$LOG_DIR/llm_worker.log"

log_message "Startup sequence completed!"
echo "Services and workers are ready to use."
echo "All logs are stored in: $LOG_DIR"
echo "Available logs:"
echo "- Service startup: $SERVICE_LOG_FILE"
echo "- RabbitMQ: $RABBITMQ_LOG"
echo "- Redis: $REDIS_LOG"
echo "- MinIO: $MINIO_LOG"
echo "- Worker logs: minio_event_worker.log, embedding_worker.log, llm_worker.log"