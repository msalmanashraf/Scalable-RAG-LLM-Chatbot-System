import pika
import json
import os
from minio import Minio
from minio.error import S3Error

def load_config(file_path='config.json'):
    """
    Load configuration from the JSON file.
    """

    parent_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(parent_dir, '..', 'config', 'minio_config.json')

    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def create_bucket():
    """
    Declare Minio Bucket using the settings from the configuration file.
    Adds error handling and logging.
    """
    try:
        # Load configuration values
        config = load_config()

        client = Minio(
        config['endpoint'],
        access_key=config['username'],
        secret_key=config['password'],
        secure=False  # Set to True if using HTTPS
        )       

        # Check if the bucket already exists
        if not client.bucket_exists(config['bucket_name']):
            client.make_bucket(config['bucket_name'])
            print(f"Bucket '{config['bucket_name']}' created successfully.")
        else:
            print(f"Bucket '{config['bucket_name']}' already exists.")
    except S3Error as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    create_bucket()
