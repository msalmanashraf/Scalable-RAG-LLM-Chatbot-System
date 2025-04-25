import pika
import json
from llama_cpp import Llama
import numpy as np
import duckdb
import os
import time
from typing import List, Dict
from datetime import datetime
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmbeddingWorker:
    """
    Worker class that generates and manages text embeddings using a Llama model.
    
    This class handles embedding generation, storage, and similarity search operations.
    It connects to a RabbitMQ message broker to process document chunks and queries.
    """
    def __init__(self, model_path: str, similarity_threshold: float = 0.95):
        """
        Initialize the worker with model and database connections.
        
        Args:
            model_path: Path to the LLM model
            similarity_threshold: Threshold for considering embeddings as similar (0 to 1)
        """
        # Load model once at startup
        print("Loading Llama model...")
        self.llm = Llama(model_path=model_path, embedding=True)
        print("Model loaded successfully")

        self.similarity_threshold = similarity_threshold

        # Setup DuckDB
        parent_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(parent_dir, '..', 'embeddings', 'embeddings.duckdb')
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.conn = duckdb.connect(db_path)
        self._initialize_database()

    def calculate_similarity(self, v1: List[float], v2: List[float]) -> float:
        """
        Calculate cosine similarity between two vectors.
        
        Args:
            v1: First embedding vector
            v2: Second embedding vector
            
        Returns:
            Cosine similarity score between 0 and 1 (higher means more similar)
        """
        v1_array = np.array(v1)
        v2_array = np.array(v2)
        
        norm1 = np.linalg.norm(v1_array)
        norm2 = np.linalg.norm(v2_array)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
            
        return float(np.dot(v1_array, v2_array) / (norm1 * norm2))

    def _initialize_database(self):
        """
        Initialize the database schema with tables and indexes for embeddings.
        Creates the embeddings table if it doesn't exist and sets up appropriate indexes.
        """
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                chunk_id VARCHAR PRIMARY KEY,
                text TEXT NOT NULL,
                embedding REAL[] NOT NULL,
                source VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_chunk_id ON embeddings(chunk_id);
            CREATE INDEX IF NOT EXISTS idx_source ON embeddings(source);
        """)
        self.conn.commit()

    def generate_embeddings(self, text: str) -> List[float]:
        """
        Generate embeddings using the pre-loaded model.
        
        Args:
            text: The text to generate embeddings for
            
        Returns:
            A list of float values representing the embedding vector
        """
        print(text)
        embedding = self.llm.embed(text)
        
        # Ensure we have a flat list of floats
        if isinstance(embedding, np.ndarray):
            embedding = embedding.flatten().tolist()
        elif isinstance(embedding, list):
            embedding = [float(x) for x in (embedding[0] if isinstance(embedding[0], list) else embedding)]
            
        return embedding

    def embedding_exists(self, chunk_id: str) -> bool:
        """
        Check if an embedding already exists for the given chunk_id.
        
        Args:
            chunk_id: The unique identifier for the text chunk
            
        Returns:
            True if the embedding exists, False otherwise
        """
        result = self.conn.execute(
            "SELECT 1 FROM embeddings WHERE chunk_id = ?",
            [chunk_id]
        ).fetchone()
        return result is not None

    def store_embedding(self, chunk_id: str, text: str, embedding: List[float], source: str):
        """
        Store the embedding in DuckDB.
        
        Args:
            chunk_id: Unique identifier for the text chunk
            text: The original text content
            embedding: The generated embedding vector
            source: Source identifier for the text
        
        Raises:
            Exception: If database operations fail
        """
        try:
            # First try to delete any existing record
            self.conn.execute(
                "DELETE FROM embeddings WHERE chunk_id = ?",
                [chunk_id]
            )
            
            # Then insert the new record
            self.conn.execute("""
                INSERT INTO embeddings (chunk_id, text, embedding, source, created_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [chunk_id, text, embedding, source])
            
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

    def search_similar(self, query_embedding: List[float], limit: int = 5) -> List[Dict]:
        """
        Search for similar embeddings using cosine similarity.
        
        Args:
            query_embedding: The embedding vector to compare against
            limit: Maximum number of results to return
            
        Returns:
            List of similar documents sorted by similarity score (higher is more similar)
        """
        # Fetch all embeddings and calculate similarity in Python
        results = self.conn.execute("""
            SELECT chunk_id, text, embedding, source
            FROM embeddings
        """).fetchall()
        
        # Calculate similarities
        similarities = []
        for r in results:
            similarity = self.calculate_similarity(query_embedding, r[2])
            if similarity >= self.similarity_threshold:
                similarities.append({
                    'chunk_id': r[0],
                    'text': r[1],
                    'source': r[3],
                    'similarity': similarity
                })
        
        # Sort by similarity and return top results
        similarities.sort(key=lambda x: x['similarity'], reverse=True)
        return similarities[:limit]

    def process_message(self, document_chunk: dict) -> bool:
        """
        Process a single message and store its embedding.
        
        Args:
            document_chunk: Dictionary containing text data with required fields:
                          - text: The content to embed
                          - chunk_id: Unique identifier
                          - source: Origin of the content
                          
        Returns:
            True if processing succeeded, False otherwise
        """
        try:
            # Validate required fields
            required_fields = ["text", "chunk_id", "source"]
            if not all(field in document_chunk for field in required_fields):
                print(f"Message missing required fields: {document_chunk}")
                return False
            
            # Generate embeddings for the new text
            embedding = self.generate_embeddings(document_chunk["text"])

            # Search for similar existing embeddings
            similar_docs = self.search_similar(embedding)
            
            if similar_docs:
                print("\nFound similar existing documents:")
                for doc in similar_docs:
                    print(f"- Chunk ID: {doc['chunk_id']}")
                    print(f"  Similarity: {doc['similarity']:.4f}")
                    print(f"  Text: {doc['text'][:100]}...")
                    print(f"  Source: {doc['source']}\n")

                # If very similar document exists, you might want to skip storage
                if similar_docs[0]['similarity'] > self.similarity_threshold:
                    print(f"Found very similar document (similarity: {similar_docs[0]['similarity']:.4f})")
                    print(f"Existing chunk_id: {similar_docs[0]['chunk_id']}")
                    return True

            # Store in DuckDB
            self.store_embedding(
                chunk_id=document_chunk["chunk_id"],
                text=document_chunk["text"],
                embedding=embedding,
                source=document_chunk["source"]
            )
            
            print(f"Successfully stored embedding for chunk {document_chunk['chunk_id']}")
            return True

        except Exception as e:
            print(f"Error processing message: {str(e)}")
            return False
    
    def process_query(self, ch, method, properties, body):
        """
        Process questions from query_queue and generate embeddings for them.
        
        Args:
            ch: RabbitMQ channel
            method: Delivery method details
            properties: Message properties
            body: Message body containing query details
            
        This method generates an embedding for a question and publishes it to the
        query_embedding_queue for further processing.
        """
        try:
            data = json.loads(body)
            logger.info(f"Processing query: {data['task_id']}")
            
            # Generate embedding
            embedding = self.generate_embeddings(data['question'])
            
            # Make sure embedding is properly formatted
            embedding_list = embedding.tolist() if isinstance(embedding, np.ndarray) else embedding
            
            # Pass to next queue
            ch.basic_publish(
                exchange='',
                routing_key='query_embedding_queue',
                body=json.dumps({
                    'task_id': data['task_id'],
                    'question': data['question'],
                    'embedding': embedding_list,
                    'top_k': data['top_k'],
                    'similarity_threshold': data['similarity_threshold'],
                    'temperature': data['temperature'],
                    'max_tokens': data['max_tokens']
                }),
                properties=pika.BasicProperties(
                    delivery_mode=2  # Make message persistent
                )
            )
            # Explicitly acknowledge the message was processed
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Embedding generated for task {data['task_id']}")
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            # Don't requeue if it's a formatting issue
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()

# Rest of the code remains unchanged (create_channel and main functions)
def create_channel(parameters):
    """
    Create a new RabbitMQ channel with error handling and retries.
    
    Args:
        parameters: RabbitMQ connection parameters
        
    Returns:
        Tuple containing the connection and channel objects
        
    This function attempts to establish a connection repeatedly until successful.
    """
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            print("Failed to connect to RabbitMQ. Retrying in 5 seconds...")
            time.sleep(5)

def main():
    """
    Main entry point for the embedding worker service.
    
    This function:
    1. Loads configuration and model
    2. Sets up the embedding worker
    3. Establishes RabbitMQ connections
    4. Starts consuming messages from document_chunks_queue and query_queue
    5. Handles reconnection on errors
    """
    # Load configuration
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(parent_dir, '..', 'config', 'rabbitmq_config.json')
    model_path = os.path.join(parent_dir, '..', 'models/sentence', 'all-MiniLM-L6-v2.F16.gguf')
    
    with open(config_path) as f:
        config = json.load(f)

    # Initialize the worker with the model
    worker = EmbeddingWorker(model_path, similarity_threshold=0.95)

    # Setup RabbitMQ connection parameters
    credentials = pika.PlainCredentials(
        config['rabbitmq']['username'],
        config['rabbitmq']['password']
    )
    parameters = pika.ConnectionParameters(
        host=config['rabbitmq']['host'],
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )

    def on_message(channel, method_frame, header_frame, body):
        """
        Callback function for processing RabbitMQ messages.
        
        Args:
            channel: RabbitMQ channel
            method_frame: Delivery method details
            header_frame: Message header information
            body: Message body containing document chunk data
            
        This function handles incoming document chunks and processes them
        for embedding generation and storage.
        """
        try:
            # Decode and parse message
            document_chunk = json.loads(body.decode('utf-8', errors='replace'))
            print("Received message:", document_chunk)

            # Process the message using the worker instance
            success = worker.process_message(document_chunk)

        except json.JSONDecodeError as e:
            print(f"JSON decoding failed: {e}. Raw message: {body}")
            success = False
        except Exception as e:
            print(f"Unexpected error processing message: {str(e)}")
            success = False
        
        finally:
            try:
                # Acknowledge message
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except Exception as e:
                print(f"Error acknowledging message: {str(e)}")

    try:
        while True:
            try:
                # Create channel
                connection, channel = create_channel(parameters)
                
                # Setup consumer
                channel.basic_consume(
                    queue="document_chunks_queue",
                    on_message_callback=on_message,
                )

                channel.basic_consume(
                    queue="query_queue",
                    on_message_callback=worker.process_query,
                )

                print("Waiting for messages... Press CTRL+C to exit.")
                channel.start_consuming()

            except pika.exceptions.ConnectionClosedError:
                print("Connection was closed. Reconnecting...")
            except pika.exceptions.AMQPConnectionError:
                print("Lost connection to RabbitMQ. Reconnecting...")
            except KeyboardInterrupt:
                print("Shutting down...")
                break
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                print("Restarting consumer...")
            
            # Wait before reconnecting
            time.sleep(5)

    finally:
        # Clean up
        try:
            connection.close()
        except:
            pass
        worker.close()

if __name__ == "__main__":
    main()