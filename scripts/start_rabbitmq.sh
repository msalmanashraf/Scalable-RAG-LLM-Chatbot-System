#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define directories and config paths
QUEUE_DECLERATION_PYTHON_SCRIPT="../app/services/rabbitmq_queue_decleration.py"  # Path to the Python script

# Function to check if RabbitMQ is installed
is_rabbitmq_installed() {
    dpkg -l | grep -q rabbitmq-server
}

# Function to check if RabbitMQ is already running
is_rabbitmq_running() {
    sudo rabbitmqctl status >/dev/null 2>&1
}

# Function to start RabbitMQ
start_rabbitmq() {
	#echo "Enabling RabbitMQ service..."
	#sudo systemctl enable rabbitmq-server
    #echo "Starting RabbitMQ server..."
    #sudo systemctl start rabbitmq-server
	
	# Start RabbitMQ server manually in WSL
	echo "Starting RabbitMQ server manually..."
	sudo rabbitmq-server -detached
	
	# Enable RabbitMQ Management Plugin (optional, provides UI)
	echo "Enabling RabbitMQ Management Plugin..."
	sudo rabbitmq-plugins enable rabbitmq_management
}

if is_rabbitmq_installed; then
    if is_rabbitmq_running; then
        echo "RabbitMQ is already running. Skipping start."
    else
        echo "RabbitMQ is not running. Starting the service..."
        start_rabbitmq
	fi
	
	# Run the Python script for queue declaration
	if [ -f "$QUEUE_DECLERATION_PYTHON_SCRIPT" ]; then
		echo "Running Python script for queue declaration..."
		python3 "$QUEUE_DECLERATION_PYTHON_SCRIPT"
	else
		echo "Python script not found at $QUEUE_DECLERATION_PYTHON_SCRIPT"
		exit 1
	fi
	
else
    echo "RabbitMQ is not installed. Installing now..."

	# Update the system
	echo "Updating the system..."
	sudo apt update -y
	sudo apt upgrade -y

	# Install dependencies
	echo "Installing necessary dependencies..."
	sudo apt install -y curl gnupg apt-transport-https software-properties-common

	# Add RabbitMQ repository signing key
	echo "Adding RabbitMQ repository signing key..."
	curl -fsSL https://packagecloud.io/rabbitmq/rabbitmq-server/gpgkey | sudo apt-key add -

	# Add RabbitMQ repository
	echo "Adding RabbitMQ repository..."
	sudo tee /etc/apt/sources.list.d/rabbitmq.list <<EOF
deb https://packagecloud.io/rabbitmq/rabbitmq-server/ubuntu/ $(lsb_release -cs) main
EOF

	# Add Erlang (dependency for RabbitMQ) repository signing key
	echo "Adding Erlang repository signing key..."
	curl -fsSL https://packages.erlang-solutions.com/ubuntu/erlang_solutions.asc | sudo apt-key add -

	# Add Erlang repository
	echo "Adding Erlang repository..."
	sudo tee /etc/apt/sources.list.d/erlang.list <<EOF
deb https://packages.erlang-solutions.com/ubuntu $(lsb_release -cs) contrib
EOF

	# Update the repositories
	echo "Updating repositories..."
	sudo apt update -y

	# Install Erlang
	echo "Installing Erlang..."
	sudo apt install -y erlang

	# Install RabbitMQ server
	echo "Installing RabbitMQ server..."
	sudo apt install -y rabbitmq-server

	# Enable RabbitMQ service to start on boot
	# echo "Enabling RabbitMQ service..."
	# sudo systemctl enable rabbitmq-server

	# # Start RabbitMQ service
	# echo "Starting RabbitMQ service..."
	# sudo systemctl start rabbitmq-server

	# Start RabbitMQ server manually in WSL
	echo "Starting RabbitMQ server manually..."
	sudo rabbitmq-server -detached

	# Enable RabbitMQ Management Plugin (optional, provides UI)
	echo "Enabling RabbitMQ Management Plugin..."
	sudo rabbitmq-plugins enable rabbitmq_management

	# Provide RabbitMQ server details
	echo "RabbitMQ installed successfully on WSL."
	echo "You can access the RabbitMQ management console at: http://localhost:15672/"
	echo "Default username and password are 'guest'. Please change them in production."

	# Print info on how to stop RabbitMQ server manually
	echo "To stop RabbitMQ in WSL, use the following command:"
	echo "    rabbitmqctl stop"
	
	# Run the Python script for queue declaration
	if [ -f "$QUEUE_DECLERATION_PYTHON_SCRIPT" ]; then
		echo "Running Python script for queue declaration..."
		python3 "$QUEUE_DECLERATION_PYTHON_SCRIPT"
	else
		echo "Python script not found at $QUEUE_DECLERATION_PYTHON_SCRIPT"
		exit 1
	fi
fi