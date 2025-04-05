import os
from dotenv import load_dotenv
import yaml
from typing import Dict, Any

class ConfigManager:
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            # Load environment variables from .env file
            load_dotenv()
            self.load_config()

    def load_config(self) -> None:
        """Load configuration from YAML file."""
        config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        # Resolve environment variables
        self._config = self._resolve_env_vars(config)

    def _resolve_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively resolve environment variables in config values."""
        resolved_config = {}
        
        for key, value in config.items():
            if isinstance(value, dict):
                resolved_config[key] = self._resolve_env_vars(value)
            elif isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                env_var = value[2:-1]
                env_value = os.getenv(env_var)
                if env_value is None:
                    raise ValueError(f"Environment variable {env_var} not set")
                resolved_config[key] = env_value
            else:
                resolved_config[key] = value
                
        return resolved_config

    def get_config(self) -> Dict[str, Any]:
        """Get the full configuration dictionary."""
        return self._config

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by key."""
        return self._config.get(key, default)

    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self._config.get('database', {})

    def get_databricks_config(self) -> Dict[str, Any]:
        """Get Databricks configuration."""
        return self._config.get('databricks', {}) 