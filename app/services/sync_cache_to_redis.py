import duckdb
import redis
import sys
import os
import json

def load_config(file_path='config.json'):
    """
    Load configuration from the JSON file.
    """
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(parent_dir, '..', 'config', 'redis_config.json')

    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def sync_duckdb_to_redis(duckdb_file):
    try:
        # Load Redis configuration
        config = load_config()
        redis_host = config.get('host', 'localhost')
        redis_port = config.get('port', 6379)
        redis_db = config.get('db', 0)

        # Connect to the DuckDB database
        conn = duckdb.connect(duckdb_file)

        # Query the `cached_responses` table
        query = "SELECT question_hash, answer FROM cached_responses;"
        rows = conn.execute(query).fetchall()

        if not rows:
            print("No data found in the cached_responses table.")
            return

        # Connect to the Redis database
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)

        # Write each row to Redis
        for question_hashed, answer in rows:
            redis_client.setex(f"cache:{question_hashed}", 604800, answer)  # 7 days expiration

        print(f"Successfully synced {len(rows)} entries from DuckDB to Redis.")

        # Close the DuckDB connection
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <duckdb_database_file>")
        sys.exit(1)

    duckdb_file = sys.argv[1]
    sync_duckdb_to_redis(duckdb_file)
