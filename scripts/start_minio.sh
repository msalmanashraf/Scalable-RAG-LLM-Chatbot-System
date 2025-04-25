#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define directories and config paths
MINIO_DATA_DIR="/mnt/data/minio"
MINIO_CONFIG_FILE="$(pwd)/../app/config/minio_config.json"  # Path to MinIO config file
RABBITMQ_CONFIG_FILE="$(pwd)/../app/config/rabbitmq_config.json"  # Path to RabbitMQ config file
MINIO_PYTHON_SCRIPT="../app/services/minio_bucket_decleration.py"  # Path to the Python script
LOG_DIR="$(pwd)/../app/logs"

# Ensure the MinIO config file exists
if [ ! -f "$MINIO_CONFIG_FILE" ]; then
    echo "Configuration file not found at $MINIO_CONFIG_FILE"
    exit 1
fi

# Ensure the RabbitMQ config file exists
if [ ! -f "$RABBITMQ_CONFIG_FILE" ]; then
    echo "Configuration file not found at $RABBITMQ_CONFIG_FILE"
    exit 1
fi

# Function to check if a command exists and is executable
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Read values from the MinIO configuration file using jq
ACCESS_KEY=$(jq -r '.access_key' "$MINIO_CONFIG_FILE")
SECRET_KEY=$(jq -r '.secret_key' "$MINIO_CONFIG_FILE")
USERNAME=$(jq -r '.username' "$MINIO_CONFIG_FILE")

# Read values from the RabbitMQ configuration file using jq
RABBITMQ_USER=$(jq -r '.rabbitmq.username' "$RABBITMQ_CONFIG_FILE")
RABBITMQ_PASSWORD=$(jq -r '.rabbitmq.password' "$RABBITMQ_CONFIG_FILE")

# Check if MinIO server is already installed
if ! command_exists minio; then
    echo "Installing MinIO Server..."
    wget https://dl.min.io/server/minio/release/linux-amd64/minio
    chmod +x minio
    sudo mv minio /usr/local/bin/
else
    echo "MinIO Server is already installed."
fi

# Check if MinIO Client (mc) is already installed
if ! command_exists mc; then
    echo "Installing MinIO Client..."
    wget https://dl.min.io/client/mc/release/linux-amd64/mc
    chmod +x mc
    sudo mv mc /usr/local/bin/
else
    echo "MinIO Client is already installed."
fi

# Start MinIO server
echo "Starting MinIO Server..."
mkdir -p "$MINIO_DATA_DIR"
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="minioadmin"
nohup minio server "$MINIO_DATA_DIR" --console-address ":9001" > "$LOG_DIR/nohup.out" 2>&1 &

# Wait for the server to start
sleep 10

# Configure the alias for MinIO client
echo "Configuring MinIO client..."
mc alias set myminio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# # Change the root user password
# echo "Changing root password to 'minioadminmaster'..."
# mc admin user add myminio minioadmin minioadminmaster

# # Update alias with the new password
# mc alias set myminio http://localhost:9000 minioadmin minioadminmaster

# # Create a new user with username, access key, and secret key from config
# echo "Creating new user $USERNAME with access key and secret key..."
# mc admin user add myminio "$USERNAME" "$ACCESS_KEY" "$SECRET_KEY"

# Apply AMQP notification settings using RabbitMQ user and password from config
echo "Configuring AMQP notification..."
mc admin config set myminio notify_amqp:primary \
    url="amqp://$RABBITMQ_USER:$RABBITMQ_PASSWORD@localhost:5672" \
    exchange="minio_exchange" \
    exchange_type="direct" \
    routing_key="minio_key" \
    durable="on"

# Restart MinIO service and wait for it to restart
echo "Restarting MinIO service..."
mc admin service restart myminio
sleep 5

# Run the Python script for bucket declaration
if [ -f "$MINIO_PYTHON_SCRIPT" ]; then
    echo "Running Python script for bucket declaration..."
    python3 "$MINIO_PYTHON_SCRIPT"
else
    echo "Python script not found at $MINIO_PYTHON_SCRIPT"
    exit 1
fi

# Output success message
echo "MinIO setup and configuration completed!"
