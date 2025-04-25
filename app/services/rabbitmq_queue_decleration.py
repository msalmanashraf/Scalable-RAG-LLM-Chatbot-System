import pika
import json
import os
import requests
from pika.exceptions import ChannelClosedByBroker

def load_config(file_path='config.json'):
    """
    Load configuration from the JSON file.
    """
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(parent_dir, '..', 'config', 'rabbitmq_config.json')

    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def change_rabbitmq_password_and_set_permissions(host, old_user, old_pass, new_user, new_pass):
    """
    Change the RabbitMQ user's password and assign permissions to the default virtual host '/'.
    """
    management_api_url = f"http://{host}:15672/api/users/{new_user}"
    auth = (old_user, old_pass)  # Credentials to access the Management API

    # Data for the user - changing the password
    data = {
        "password": new_pass,
        "tags": "administrator"  # Give the new user administrative privileges
    }

    try:
        # Make an HTTP PUT request to change the password
        response = requests.put(management_api_url, auth=auth, json=data)

        if response.status_code in [201, 204]:
            print(f"Password for user '{new_user}' changed successfully.")
        else:
            print(f"Failed to change password. Status code: {response.status_code}, Response: {response.text}")

        # Now assign permissions to the virtual host "/"
        permissions_url = f"http://{host}:15672/api/permissions/%2F/{new_user}"
        permissions_data = {
            "configure": ".*",
            "write": ".*",
            "read": ".*"
        }

        # Make an HTTP PUT request to assign permissions to the user
        response = requests.put(permissions_url, auth=(new_user, new_pass), json=permissions_data)
        if response.status_code in [201, 204]:
            print(f"Permissions for user '{new_user}' set successfully.")
        else:
            print(f"Failed to set permissions for user '{new_user}'. Status code: {response.status_code}, Response: {response.text}")

    except Exception as e:
        print(f"Error while changing password or setting permissions: {e}")

def delete_guest_user(host, admin_user, admin_pass):
    """
    Delete the 'guest' user to disallow access after creating a new user.
    """
    management_api_url = f"http://{host}:15672/api/users/guest"
    auth = (admin_user, admin_pass)  # Use the new admin credentials

    try:
        # Make an HTTP DELETE request to delete the guest user
        response = requests.delete(management_api_url, auth=auth)
        if response.status_code == 204:
            print("Successfully deleted the 'guest' user.")
        else:
            print(f"Failed to delete the 'guest' user. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error while deleting 'guest' user: {e}")

def queue_exists(channel, queue_name):
    """
    Check if a queue exists by declaring it passively.
    If it exists, no exception is raised; otherwise, an exception is caught.
    """
    try:
        channel.queue_declare(queue=queue_name, passive=True)
        print(f"Queue '{queue_name}' already exists.")
        return True
    except ChannelClosedByBroker as e:
        if e.reply_code == 404:
            print(f"Queue '{queue_name}' does not exist.")
        return False
    except Exception as e:
        print(f"Error checking existence of queue '{queue_name}': {e}")
        return False

def declare_queues():
    try:
        # Load configuration values
        config = load_config()

        rabbitmq_host = config['rabbitmq']['host']
        rabbitmq_port = config['rabbitmq']['port']
        rabbitmq_user = config['rabbitmq']['username']
        rabbitmq_pass = config['rabbitmq']['password']

        # Attempt to connect with guest credentials
        try:
            credentials = pika.PlainCredentials('guest', 'guest')
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=rabbitmq_host, port=rabbitmq_port, credentials=credentials))
            print("Connected with 'guest/guest'.")

            # Change password and delete guest
            change_rabbitmq_password_and_set_permissions(rabbitmq_host, 'guest', 'guest', rabbitmq_user, rabbitmq_pass)
            connection.close()
        except Exception as e:
            print("Failed with 'guest/guest', using config credentials.")
        
        # Now use new credentials
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=rabbitmq_host, port=rabbitmq_port, credentials=credentials))
        channel = connection.channel()

        # Declare required queues from config
        for queue_config in config['rabbitmq']['queues']:
            queue_name = queue_config['name']
            try:
                # Attempt to declare the queue with specified properties
                channel.queue_declare(queue=queue_name, durable=True, auto_delete=False, passive=True)
                print(f"Declared queue: {queue_name}")
            except ChannelClosedByBroker:
                print(f"Channel closed while declaring {queue_name}. Check permissions.")
                return
            except Exception as e:
                print(f"Failed to declare queue {queue_name}: {e}")

        connection.close()
        
    except Exception as general_error:
        print(f"An error occurred during RabbitMQ setup: {general_error}")

if __name__ == "__main__":
    declare_queues()
