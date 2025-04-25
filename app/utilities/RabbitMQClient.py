import pika
import json
import logging
from typing import Dict, Any, Optional, Callable
import time

logger = logging.getLogger(__name__)

class RabbitMQClient:
    """
    Client for interacting with RabbitMQ message broker.
    Handles connection, channel management, and basic operations.
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize a RabbitMQ client with provided configuration.
        
        Args:
            config: Dictionary containing connection parameters:
                - host: RabbitMQ server hostname
                - port: RabbitMQ server port
                - username: Authentication username
                - password: Authentication password
                - virtual_host: Virtual host to use
                - connection_attempts: Number of connection attempts
                - retry_delay: Delay between connection attempts in seconds
        """
        self.config = config
        self.connection = None
        self.channel = None
        self.connect()
        
    def connect(self) -> bool:
        """
        Establish connection to RabbitMQ and create a channel.
        
        Returns:
            bool: True if connection and channel were successfully established, False otherwise.
        """
        try:
            # Create connection parameters from config
            credentials = pika.PlainCredentials(
                username=self.config.get('username', 'guest'),
                password=self.config.get('password', 'guest')
            )
            
            parameters = pika.ConnectionParameters(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 5672),
                virtual_host=self.config.get('virtual_host', '/'),
                credentials=credentials,
                connection_attempts=self.config.get('connection_attempts', 5),
                retry_delay=self.config.get('retry_delay', 2),
                heartbeat=1200 
            )
            
            # Establish connection
            self.connection = pika.BlockingConnection(parameters)
            
            # Create channel
            self.channel = self.connection.channel()
            
            logger.info(f"Successfully connected to RabbitMQ at {self.config.get('host')}:{self.config.get('port')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            self.connection = None
            self.channel = None
            return False
    
    def reconnect(self, max_attempts: int = 3) -> bool:
        """
        Attempt to reconnect to RabbitMQ if connection was lost.
        
        Args:
            max_attempts: Maximum number of reconnection attempts
        
        Returns:
            bool: True if reconnection was successful, False otherwise
        """
        for attempt in range(1, max_attempts + 1):
            logger.info(f"Reconnection attempt {attempt}/{max_attempts}")
            
            # Close existing connection if it exists
            if self.connection and self.connection.is_open:
                try:
                    self.connection.close()
                except Exception as e:
                    logger.warning(f"Error closing existing connection: {str(e)}")
            
            # Attempt to connect
            if self.connect():
                return True
                
            # Wait before next attempt
            time.sleep(self.config.get('retry_delay', 2))
        
        logger.error(f"Failed to reconnect after {max_attempts} attempts")
        return False
    
    def declare_queue(self, queue_name: str, durable: bool = True, 
                     exclusive: bool = False, auto_delete: bool = False) -> bool:
        """
        Declare a queue on the RabbitMQ server.
        
        Args:
            queue_name: Name of the queue to declare
            durable: Whether the queue should survive broker restarts
            exclusive: Whether the queue is exclusive to this connection
            auto_delete: Whether the queue should be deleted when no longer used
            
        Returns:
            bool: True if queue was successfully declared, False otherwise
        """
        if not self.channel:
            logger.error("Cannot declare queue: No channel available")
            return False
            
        try:
            self.channel.queue_declare(
                queue=queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete
            )
            logger.info(f"Queue '{queue_name}' declared successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to declare queue '{queue_name}': {str(e)}")
            return False
    
    def publish_message(self, queue_name: str, message: Dict[str, Any], 
                       persistent: bool = True) -> bool:
        """
        Publish a message to a specified queue.
        
        Args:
            queue_name: Name of the queue to publish to
            message: Dictionary containing the message to publish
            persistent: Whether the message should persist in case of broker restart
            
        Returns:
            bool: True if message was successfully published, False otherwise
        """
        if not self.channel:
            logger.error("Cannot publish message: No channel available")
            return False
            
        try:
            # Ensure queue exists
            self.declare_queue(queue_name)
            
            # Convert message to JSON
            message_body = json.dumps(message)
            
            # Set message properties
            properties = pika.BasicProperties(
                delivery_mode=2 if persistent else 1,  # 2 means persistent
                content_type='application/json'
            )
            
            # Publish message
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=properties
            )
            
            logger.debug(f"Message published to queue '{queue_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to publish message to queue '{queue_name}': {str(e)}")
            return False
    
    def consume_messages(self, queue_name: str, callback: Callable, 
                        auto_ack: bool = False) -> None:
        """
        Begin consuming messages from a specified queue.
        
        Args:
            queue_name: Name of the queue to consume from
            callback: Function to call when a message is received
            auto_ack: Whether to automatically acknowledge receipt of messages
        """
        if not self.channel:
            logger.error("Cannot consume messages: No channel available")
            return
            
        try:
            # Ensure queue exists
            self.declare_queue(queue_name)
            
            # Start consuming
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=auto_ack
            )
            
            logger.info(f"Started consuming messages from queue '{queue_name}'")
            self.channel.start_consuming()
        except Exception as e:
            logger.error(f"Failed to consume messages from queue '{queue_name}': {str(e)}")
    
    def close(self) -> None:
        """
        Close the RabbitMQ connection and channel.
        This method should be called when shutting down to properly release resources.
        """
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
                logger.info("RabbitMQ channel closed")
            
            if self.connection and self.connection.is_open:
                self.connection.close()
                logger.info("RabbitMQ connection closed")
                
            self.channel = None
            self.connection = None
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {str(e)}")