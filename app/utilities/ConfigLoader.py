import json

class ConfigLoader:
    @staticmethod
    def load_rabbitmq_config(config_path: str) -> dict:
        with open(config_path) as f:
            config = json.load(f)
        return config['rabbitmq']
    
    @staticmethod
    def load_redis_config(config_path: str) -> dict:
        with open(config_path) as f:
            config = json.load(f)
        return config['redis']