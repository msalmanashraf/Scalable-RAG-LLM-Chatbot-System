from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import logging
from datetime import datetime
import uuid
import json
from hashlib import sha256
from typing import List, Dict, Optional, Union
import duckdb
from pika import BasicProperties
from utilities import ConfigLoader, RabbitMQClient, RedisClient


# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variable for query service
query_service = None

class QuestionRequest(BaseModel):
    question: str
    top_k: int = 3
    similarity_threshold: float = 0.3
    temperature: float = 0.7
    max_tokens: int = 4096

class DocumentResponse(BaseModel):
    text: str
    source: str
    similarity: float

class QuestionResponse(BaseModel):
    answer: str
    similar_documents: List[DocumentResponse]

class TaskResponse(BaseModel):
    task_id: str
    status: str
    cache_hit: bool = False
    result: str = None

class QueryService:
    def __init__(self, rabbitmq_config: Dict, redis_config: Dict):
        self.rabbitmq = RabbitMQClient(rabbitmq_config)
        self.redis = RedisClient(redis_config)
        self.db_conn = self._init_database()
        
    def _init_database(self):
        """Initialize the database with tasks table if it doesn't exist"""
        parent_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(parent_dir, 'embeddings', 'embeddings.duckdb')
        
        conn = duckdb.connect(db_path)
        
        # Check if tasks table exists
        table_exists = conn.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='tasks'
        """).fetchone()
        
        if not table_exists:
            # Create the table with all needed columns
            conn.execute("""
                CREATE TABLE tasks (
                    task_id VARCHAR PRIMARY KEY,
                    status VARCHAR NOT NULL,
                    question TEXT,
                    parameters TEXT,  /* Using 'parameters' instead of 'params' */
                    result TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("Created tasks table")
        else:
            # Check if 'parameters' column exists, add it if it doesn't
            columns = conn.execute("""
                PRAGMA table_info(tasks)
            """).fetchall()
            
            column_names = [col[1] for col in columns]
            
            if 'parameters' not in column_names:
                conn.execute("""
                    ALTER TABLE tasks ADD COLUMN parameters TEXT
                """)
                logger.info("Added 'parameters' column to tasks table")
        
        return conn

    async def submit_query(self, params: QuestionRequest) -> Dict:
        """Submit a query to RabbitMQ and return a task ID"""
        task_id = str(uuid.uuid4())
        question_hash = sha256(params.question.encode()).hexdigest()
        
        # Check cache first
        if cached := self.redis.get(f"cache:{question_hash}"):
            print(type(cached))
            logger.info("Returning cached response")
            return {
                "task_id": task_id,
                "status": "completed",
                "result": cached,
                "cache_hit": True
                }

        try:
            # Check if RabbitMQ channel exists
            if not self.rabbitmq or not self.rabbitmq.channel:
                logger.error("RabbitMQ connection or channel is None")
                raise Exception("RabbitMQ connection not established")
                
            # Publish to RabbitMQ
            channel = self.rabbitmq.channel
            channel.basic_publish(
                exchange='',
                routing_key='query_queue',
                body=json.dumps({
                    'task_id': task_id,
                    'question': params.question,
                    'top_k': params.top_k,
                    'similarity_threshold': params.similarity_threshold,
                    'temperature': params.temperature,
                    'max_tokens': params.max_tokens
                }),
                properties=BasicProperties(
                    delivery_mode=2,  # Persistent
                    priority=1
                )
            )

            # Convert parameters to JSON
            parameters_json = json.dumps({
                'top_k': params.top_k,
                'similarity_threshold': params.similarity_threshold,
                'temperature': params.temperature,
                'max_tokens': params.max_tokens
            })

            # Store task metadata - using 'parameters' column name
            self.db_conn.execute("""
                INSERT INTO tasks (task_id, status, question, parameters)
                VALUES (?, ?, ?, ?)
            """, [
                task_id, 
                'queued', 
                params.question, 
                parameters_json
            ])

            return {
                "task_id": task_id,
                "status": "queued",
                "cache_hit": False
            }

        except Exception as e:
            logger.error(f"Submission failed: {str(e)}")
            raise HTTPException(500, f"Query submission failed: {str(e)}")

    def get_task_status(self, task_id: str) -> Dict:
        """Get the status of a task by task ID"""
        try:
            result = self.db_conn.execute("""
                SELECT status, result FROM tasks WHERE task_id = ?
            """, [task_id]).fetchone()
            
            if not result:
                return {
                    "task_id": task_id,
                    "status": "not_found",
                    "result": None
                }
                
            return {
                "task_id": task_id,
                "status": result[0],
                "result": json.loads(result[1]) if result[1] else None
            }
        except Exception as e:
            logger.error(f"Error retrieving task status: {str(e)}")
            return {
                "task_id": task_id,
                "status": "error",
                "result": None
            }
    
    def close(self):
        """Close connections"""
        if self.db_conn:
            self.db_conn.close()
            logger.info("Database connection closed")
        if self.rabbitmq:
            self.rabbitmq.close()
            logger.info("RabbitMQ connection closed")
        if self.redis:
            self.redis.close()
            logger.info("Redis connection closed")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.
    Handles startup and shutdown events.
    """
    # Startup
    global query_service
    
    try:
        logger.info("Initializing QueryService...")
        config_path = os.path.join(os.path.dirname(__file__), 'config')
        query_service = QueryService(
            rabbitmq_config=ConfigLoader.load_rabbitmq_config(f"{config_path}/rabbitmq_config.json"),
            redis_config=ConfigLoader.load_redis_config(f"{config_path}/redis_config.json")
        )
        
        # Verify connections
        if not query_service.rabbitmq or not query_service.rabbitmq.channel:
            logger.warning("RabbitMQ connection not established, attempting reconnect...")
            query_service.rabbitmq.reconnect()
        
        logger.info("QueryService initialized successfully")
        
        # This yield is required for the context manager
        yield
    except Exception as e:
        logger.error(f"Failed to initialize QueryService: {str(e)}")
        raise
    finally:
        # Shutdown - should only happen after the yield
        if 'query_service' in globals() and query_service:
            query_service.close()
            logger.info("Resources cleaned up")

# Initialize FastAPI with lifespan
app = FastAPI(
    title="Embedding Search API",
    description="API for semantic search and question answering using embeddings",
    lifespan=lifespan
)

@app.post("/ask", response_model=TaskResponse)
async def ask_question(request: QuestionRequest):
    """
    Submit a question for processing and return a task ID.
    
    Parameters:
    - question: The question to answer
    - top_k: Number of similar documents to retrieve (default: 3)
    - similarity_threshold: Minimum similarity score (default: 0.3)
    - temperature: Temperature for response generation (default: 0.7)
    - max_tokens: Maximum tokens for response (default: 4096)
    """
    if not query_service:
        raise HTTPException(status_code=500, detail="Service not initialized")
    
    try:
        return await query_service.submit_query(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tasks/{task_id}", response_model=TaskResponse)
def get_task(task_id: str):
    """
    Get the status and result of a task by task ID.
    """
    if not query_service:
        raise HTTPException(status_code=500, detail="Service not initialized")
        
    return query_service.get_task_status(task_id)

@app.get("/health")
async def health_check():
    """Check if the service is healthy."""
    if not query_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    # Check database connection
    try:
        query_service.db_conn.execute("SELECT 1").fetchone()
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    
    # Check RabbitMQ connection
    rabbitmq_status = "connected" if (query_service.rabbitmq and query_service.rabbitmq.channel) else "disconnected"
    
    # Check Redis connection
    redis_status = "connected" if query_service.redis.ping() else "disconnected"
    
    return {
        "status": "healthy" if all(s == "connected" for s in [db_status, rabbitmq_status, redis_status]) else "degraded",
        "connections": {
            "database": db_status,
            "rabbitmq": rabbitmq_status,
            "redis": redis_status
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)