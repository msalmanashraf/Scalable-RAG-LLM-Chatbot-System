import json
import numpy as np
import logging
from llama_cpp import Llama
from typing import List, Dict, Union
import sys
import os
import duckdb
from hashlib import sha256
from utilities import ConfigLoader, RabbitMQClient, RedisClient
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LLMWorker:
    """
    Worker class for processing LLM inference requests using a RAG pattern.
    
    This worker:
    1. Consumes query embedding messages from RabbitMQ
    2. Checks Redis cache for existing responses
    3. Finds similar context from the embeddings database
    4. Generates responses using the LLaMA model
    5. Caches results in Redis and DuckDB
    
    Attributes:
        rabbitmq: RabbitMQ client for message queue operations
        redis: Redis client for caching responses
        db_conn: DuckDB connection for persistent storage
        similarity_threshold: Minimum similarity score for context retrieval
        top_k: Maximum number of similar documents to include in context
        max_tokens: Maximum tokens to generate in response
        temperature: Temperature parameter for LLM response generation
    """
    def __init__(self, config: dict):
        """
        Initialize the LLM worker with configuration parameters.
        
        Args:
            config: Dictionary containing configuration parameters:
                - model_path: Path to the LLaMA model file
                - rabbitmq: RabbitMQ connection configuration
                - redis: Redis connection configuration
                - db_path: Path to the DuckDB database
                - similarity_threshold: Threshold for context similarity
                - top_k: Number of similar documents to retrieve
                - max_tokens: Maximum tokens in generated response
                - temperature: Sampling temperature for LLM
        """
        # self.llm = Llama(model_path = config['model_path'], n_ctx=4096, n_gpu_layers=0)
        self.rabbitmq = RabbitMQClient(config['rabbitmq'])
        self.redis = RedisClient(config['redis'])
        self.db_conn = duckdb.connect(config['db_path'])
        self.similarity_threshold = config.get('similarity_threshold', 0.3)
        self.top_k = config.get('top_k', 3)
        self.max_tokens = config.get('max_tokens', 4096)
        self.temperature = config.get('temperature', 0.7)
        
        self._init_database()
        # self._setup_queues()

    def _init_database(self):
        """
        Initialize the DuckDB database schema if not already present.
        Creates cached_responses table for storing previous query results.
        """
        self.db_conn.execute("""
            CREATE TABLE IF NOT EXISTS cached_responses (
                question_hash VARCHAR PRIMARY KEY,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_accessed TIMESTAMP
            )
        """)

    def _setup_queues(self):
        """
        Set up RabbitMQ queues for message processing.
        Ensures the query_embedding_queue exists with appropriate parameters.
        """
        self.rabbitmq.channel.queue_declare(
            queue='query_embedding_queue',
            durable=True,
            arguments={'x-max-priority': 1}
        )
    
    def _calculate_similarity(self, v1: Union[List[float], np.ndarray], v2: Union[List[float], np.ndarray]) -> float:
            """
            Calculate cosine similarity between two vectors.
            
            Args:
                v1: First vector (as list or numpy array)
                v2: Second vector (as list or numpy array)
                
            Returns:
                float: Cosine similarity score between -1.0 and 1.0
                
            Raises:
                ValueError: If vector dimensions don't match
            """
            try:
                v1_array = np.array(v1)
                v2_array = np.array(v2)
                
                if v1_array.size != v2_array.size:
                    raise ValueError(f"Vector dimensions don't match: {v1_array.size} vs {v2_array.size}")
                
                norm1 = np.linalg.norm(v1_array)
                norm2 = np.linalg.norm(v2_array)
                
                if norm1 == 0 or norm2 == 0:
                    return 0.0
                
                similarity = np.dot(v1_array, v2_array) / (norm1 * norm2)
                
                return float(np.clip(similarity, -1.0, 1.0))
                
            except Exception as e:
                logger.error(f"Error calculating similarity: {str(e)}")
                raise

    def _find_similar_context(self, similarity_threshold, top_k, embedding: list) -> str:
        """
        Find context documents similar to the provided embedding.
        
        Args:
            similarity_threshold: Minimum similarity score to consider
            top_k: Maximum number of results to return
            embedding: Query embedding vector
            
        Returns:
            List of dictionaries containing similar documents, with fields:
            - chunk_id: Unique identifier for the document chunk
            - text: Document text content
            - source: Source reference for the document
            - similarity: Similarity score to the query
        """
        results = self.db_conn.execute("""
            SELECT chunk_id, text, embedding, source
            FROM embeddings
        """).fetchall()

        if not results:
            logger.warning("No embeddings found in database")
            return []
        
        similarities = []
        for r in results:
            try:
                db_embedding = np.array(r[2])
                similarity = self._calculate_similarity(embedding, db_embedding)
                logger.info(f"Similarity score: {similarity} for chunk {r[0]}")
                
                if similarity >= similarity_threshold:
                    similarities.append({
                        'chunk_id': r[0],
                        'text': r[1],
                        'source': r[3],
                        'similarity': similarity
                    })
            except Exception as e:
                logger.warning(f"Skipping document {r[0]} due to error: {str(e)}")
                continue
            
        logger.info(f"Found {len(similarities)} matches above threshold {similarity_threshold}")
        similarities.sort(key=lambda x: x['similarity'], reverse=True)
        return similarities[:top_k]
    
    def estimate_tokens(self, text: str) -> int:
        """
        Estimate the number of tokens in a text string.
        
        Uses a simple heuristic of 1 token ≈ 4 characters.
        
        Args:
            text: Input text string
            
        Returns:
            int: Estimated token count
        """
        # Rough estimation: 1 token ≈ 4 characters
        return len(text) // 4

    def _process_message(self, ch, method, properties, body):
        """
        Process incoming RabbitMQ messages containing query requests.
        
        This callback function:
        1. Parses the incoming message
        2. Updates task status to 'processing'
        3. Checks cache for existing responses
        4. Finds relevant context from embeddings database
        5. Generates response using the LLM
        6. Caches the result and updates task status
        
        Args:
            ch: RabbitMQ channel
            method: RabbitMQ method frame
            properties: RabbitMQ properties
            body: Message body as bytes
        """
        try:
            # Parse message data
            data = json.loads(body)
            task_id = data['task_id']
            question = data['question']
            embedding = data['embedding']
            question_hash = sha256(question.encode()).hexdigest()
            top_k = data['top_k']
            similarity_threshold = data['similarity_threshold']
            temperature = data['temperature']
            max_tokens = data['max_tokens']

            # Update task status to 'processing'
            self.db_conn.execute("""
                UPDATE tasks SET status = 'processing'
                WHERE task_id = ?
            """, [task_id])

            # Check cache for existing response
            if cached := self.redis.get(f"cache:{question_hash}"):
                self.db_conn.execute("""
                    UPDATE tasks SET status = 'completed', result = ?
                    WHERE task_id = ?
                """, [cached, task_id])
                return

            # Find similar context documents
            context = self._find_similar_context(similarity_threshold, top_k, embedding)
            # print(context)

            # Token management for context assembly
            total_tokens = 0
            context_parts = []
        
            # Reserve tokens for the template and question
            template_tokens = 100  # Approximate tokens for prompt template
            reserved_tokens = template_tokens + self.estimate_tokens(question)
            available_tokens = max_tokens - reserved_tokens
            
            # Build context up to the available token limit
            for i, doc in enumerate(context):
                doc_text = f"Document {i+1} (similarity: {doc['similarity']:.3f}):\n{doc['text']}"
                doc_tokens = self.estimate_tokens(doc_text)
                
                if total_tokens + doc_tokens > available_tokens:
                    logger.info(f"Stopping at document {i} to respect token limit")
                    break
                    
                context_parts.append(doc_text)
                total_tokens += doc_tokens
            
            context_str = "\n\n".join(context_parts)
            logger.info(f"Final context estimated tokens: {self.estimate_tokens(context_str)}")

            # Construct the prompt with context and question
            prompt = f"""Use the following context to answer the question. If you cannot find an answer in the context, say "I don't have enough information to answer this question."

            Context:
            {context_str}

            Question: {question}

            Answer:"""
            
            # Generate response using LLM
            response = self.llm.create_completion(
                prompt=prompt,
                max_tokens = max_tokens,
                temperature= temperature,
                stop=["Question:"],
                echo=False
            )

            # Extract answer from response
            if response and 'choices' in response and len(response['choices']) > 0:
                answer = response['choices'][0]['text'].strip()
                print(answer)
            else:
                logger.error(f"Unexpected response format: {response}")
                return "Error generating response"
                
            # Cache the response in Redis (1 week expiry)
            self.redis.setex(f"cache:{question_hash}", 604800, answer)
            
            # Store response in DuckDB cache
            self.db_conn.execute("""
                INSERT OR REPLACE INTO cached_responses 
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, [question_hash, question, answer])
            
            # Update task status to 'completed' with result
            self.db_conn.execute("""
                UPDATE tasks SET status = 'completed', result = ?
                WHERE task_id = ?
            """, [answer, task_id])

            # Acknowledge message processing
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            # print(e)
            logger.error(f"Processing failed: {str(e)}")
            # Update task status to 'failed'
            self.db_conn.execute("""
                UPDATE tasks SET status = 'failed'
                WHERE task_id = ?
            """, [task_id])
            # Negative acknowledgment with requeue
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


    def start(self):
        """
        Start the worker and begin consuming messages from the queue.
        Sets up consumer callback and starts the event loop.
        """
        self.rabbitmq.channel.basic_consume(
            queue='query_embedding_queue',
            on_message_callback=self._process_message,
            auto_ack= False
        )
        logger.info("LLM Worker started. Waiting for messages...")
        self.rabbitmq.channel.start_consuming()

if __name__ == "__main__":
    # Determine file paths based on script location
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    rabbitmq_config_path = os.path.join(parent_dir, '..', 'config', 'rabbitmq_config.json')
    redis_config_path = os.path.join(parent_dir, '..', 'config', 'redis_config.json')
    model_path = os.path.join(parent_dir, '..', 'models/mistral', 'mistral-7b-v0.1.Q2_K.gguf')
    db_path = os.path.join(parent_dir, '..', 'embeddings', 'embeddings.duckdb')
    
    # Create worker configuration
    config = {
        "model_path": model_path,
        "db_path": db_path,
        "rabbitmq": ConfigLoader.load_rabbitmq_config(rabbitmq_config_path),
        "redis": ConfigLoader.load_redis_config(redis_config_path),
        "similarity_threshold": 0.3,
        "top_k": 3,
        "max_tokens": 4096,
        "temperature": 0.7
    }
    
    # Initialize and start the worker
    worker = LLMWorker(config)
    worker.start()