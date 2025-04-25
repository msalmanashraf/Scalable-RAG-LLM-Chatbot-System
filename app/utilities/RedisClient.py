import redis
import json
import logging
from typing import Dict, Any, Optional, Union, List, Tuple

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self, config: dict):
        """
        Initialize Redis client with provided configuration.
        
        Args:
            config (dict): Redis configuration dictionary with host, port, db, etc.
        """
        self.config = config
        self.client = redis.Redis(
            host=config['host'],
            port=config['port'],
            db=config['db'],
            password=config.get('password', None),
            decode_responses=config.get('decode_responses', True),
            socket_timeout=config.get('socket_timeout', 5),
            socket_keepalive=config.get('socket_keepalive', True),
            socket_connect_timeout=config.get('socket_connect_timeout', 10),
            retry_on_timeout=config.get('retry_on_timeout', True),
            health_check_interval=config.get('health_check_interval', 30)
        )
        self.namespace = config.get('namespace', '')
        self.default_ttl = config.get('default_ttl', 604800)  # 1 week default
        logger.info(f"Redis client initialized with host={config['host']}, port={config['port']}, db={config['db']}")
        
    def _format_key(self, key: str) -> str:
        """Add namespace prefix to key if namespace is set"""
        if self.namespace and not key.startswith(f"{self.namespace}:"):
            return f"{self.namespace}:{key}"
        return key
    
    def ping(self) -> bool:
        """Test connection to Redis server"""
        try:
            return self.client.ping()
        except redis.ConnectionError as e:
            logger.error(f"Redis connection error: {str(e)}")
            return False
    
    def get(self, key: str) -> Optional[str]:
        """
        Get a value from Redis by key.
        
        Args:
            key (str): The key to retrieve
            
        Returns:
            Optional[str]: The value associated with the key, or None if key doesn't exist
        """
        try:
            formatted_key = self._format_key(key)
            value = self.client.get(formatted_key)
            if value:
                logger.debug(f"Retrieved value for key: {formatted_key}")
            else:
                logger.debug(f"No value found for key: {formatted_key}")
            return value
        except Exception as e:
            logger.error(f"Error retrieving key {key} from Redis: {str(e)}")
            return None
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """
        Set a key-value pair in Redis with optional expiration.
        
        Args:
            key (str): The key to set
            value (str): The value to set
            ex (Optional[int]): Expiration time in seconds
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            formatted_key = self._format_key(key)
            self.client.set(formatted_key, value, ex=ex if ex is not None else self.default_ttl)
            logger.debug(f"Set value for key: {formatted_key}" + (f" with expiry: {ex}s" if ex else f" with default expiry: {self.default_ttl}s"))
            return True
        except Exception as e:
            logger.error(f"Error setting key {key} in Redis: {str(e)}")
            return False
    
    def setex(self, key: str, time: int, value: str) -> bool:
        """
        Set a key-value pair in Redis with expiration.
        Convenience alias matching Redis native command name.
        
        Args:
            key (str): The key to set
            time (int): Expiration time in seconds
            value (str): The value to set
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.set(key, value, ex=time)
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from Redis.
        
        Args:
            key (str): The key to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            formatted_key = self._format_key(key)
            result = self.client.delete(formatted_key)
            if result:
                logger.debug(f"Deleted key: {formatted_key}")
            return bool(result)
        except Exception as e:
            logger.error(f"Error deleting key {key} from Redis: {str(e)}")
            return False
    
    def exists(self, key: str) -> bool:
        """
        Check if a key exists in Redis.
        
        Args:
            key (str): The key to check
            
        Returns:
            bool: True if the key exists, False otherwise
        """
        try:
            formatted_key = self._format_key(key)
            return bool(self.client.exists(formatted_key))
        except Exception as e:
            logger.error(f"Error checking existence of key {key} in Redis: {str(e)}")
            return False
    
    def set_json(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """
        Set a JSON value for a key in Redis.
        
        Args:
            key (str): The key to set
            value (Any): The value to serialize as JSON
            ex (Optional[int]): Expiration time in seconds
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            json_value = json.dumps(value)
            return self.set(key, json_value, ex=ex)
        except Exception as e:
            logger.error(f"Error setting JSON for key {key} in Redis: {str(e)}")
            return False
    
    def get_json(self, key: str) -> Optional[Any]:
        """
        Get a JSON value from Redis by key and deserialize it.
        
        Args:
            key (str): The key to retrieve
            
        Returns:
            Optional[Any]: The deserialized JSON value, or None if not found or error
        """
        try:
            value = self.get(key)
            if value:
                return json.loads(value)
            return None
        except json.JSONDecodeError:
            logger.error(f"Value for key {key} is not valid JSON")
            return None
        except Exception as e:
            logger.error(f"Error getting JSON for key {key} from Redis: {str(e)}")
            return None
    
    def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """
        Increment the integer value of a key by the given amount.
        
        Args:
            key (str): The key to increment
            amount (int): The amount to increment by (default: 1)
            
        Returns:
            Optional[int]: The value after increment, or None on error
        """
        try:
            formatted_key = self._format_key(key)
            return self.client.incrby(formatted_key, amount)
        except Exception as e:
            logger.error(f"Error incrementing key {key} in Redis: {str(e)}")
            return None
    
    def expire(self, key: str, seconds: int) -> bool:
        """
        Set a key's time to live in seconds.
        
        Args:
            key (str): The key to expire
            seconds (int): Expiration time in seconds
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            formatted_key = self._format_key(key)
            return bool(self.client.expire(formatted_key, seconds))
        except Exception as e:
            logger.error(f"Error setting expiry for key {key} in Redis: {str(e)}")
            return False
    
    def ttl(self, key: str) -> int:
        """
        Get the time to live for a key in seconds.
        
        Args:
            key (str): The key to get TTL for
            
        Returns:
            int: TTL in seconds, -1 if no expiry, -2 if key doesn't exist
        """
        try:
            formatted_key = self._format_key(key)
            return self.client.ttl(formatted_key)
        except Exception as e:
            logger.error(f"Error getting TTL for key {key} in Redis: {str(e)}")
            return -2
    
    def pipeline(self) -> redis.client.Pipeline:
        """
        Get a Redis pipeline object for batch operations.
        
        Returns:
            redis.client.Pipeline: A Redis pipeline object
        """
        return self.client.pipeline()
    
    def scan_iter(self, match: str = "*", count: int = 100) -> List[str]:
        """
        Iterate through the keys in the Redis database.
        
        Args:
            match (str): Pattern to match keys against
            count (int): Number of keys to scan per internal call
            
        Returns:
            List[str]: A list of keys matching the pattern
        """
        try:
            formatted_match = self._format_key(match)
            return list(self.client.scan_iter(match=formatted_match, count=count))
        except Exception as e:
            logger.error(f"Error scanning keys in Redis: {str(e)}")
            return []
    
    def mget(self, keys: List[str]) -> List[Optional[str]]:
        """
        Get multiple values from Redis by keys.
        
        Args:
            keys (List[str]): The keys to retrieve
            
        Returns:
            List[Optional[str]]: The values associated with the keys
        """
        try:
            formatted_keys = [self._format_key(key) for key in keys]
            return self.client.mget(formatted_keys)
        except Exception as e:
            logger.error(f"Error retrieving multiple keys from Redis: {str(e)}")
            return [None] * len(keys)
    
    def mset(self, mapping: Dict[str, str]) -> bool:
        """
        Set multiple key-value pairs in Redis.
        
        Args:
            mapping (Dict[str, str]): Dictionary of key-value pairs to set
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            formatted_mapping = {self._format_key(k): v for k, v in mapping.items()}
            return self.client.mset(formatted_mapping)
        except Exception as e:
            logger.error(f"Error setting multiple keys in Redis: {str(e)}")
            return False
    
    def reconnect(self) -> bool:
        """
        Reconnect to Redis server with the same configuration.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.close()
            self.client = redis.Redis(
                host=self.config['host'],
                port=self.config['port'],
                db=self.config['db'],
                password=self.config.get('password', None),
                decode_responses=self.config.get('decode_responses', True),
                socket_timeout=self.config.get('socket_timeout', 5),
                socket_keepalive=self.config.get('socket_keepalive', True),
                socket_connect_timeout=self.config.get('socket_connect_timeout', 10),
                retry_on_timeout=self.config.get('retry_on_timeout', True),
                health_check_interval=self.config.get('health_check_interval', 30)
            )
            logger.info("Redis connection reestablished")
            return self.ping()
        except Exception as e:
            logger.error(f"Error reconnecting to Redis: {str(e)}")
            return False
    
    def close(self):
        """Close the Redis connection"""
        try:
            self.client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {str(e)}")