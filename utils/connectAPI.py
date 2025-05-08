import threading
import configparser
from pathlib import Path

class Get_API_URL_Singleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(Get_API_URL_Singleton, cls).__new__(cls)
                    cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        try:
            project_root = Path(__file__).resolve().parents[1]
            config_path = project_root / 'config' / 'core_config.ini'  # Ensure the filename matches exactly
            if not config_path.exists():
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
            self.config = configparser.ConfigParser()
            self.config.read(str(config_path))
            
            if 'environment' not in self.config or 'current' not in self.config['environment']:
                raise KeyError("Missing 'environment' section or 'current' key in config file.")
            
            self.environment = self.config['environment']['current'].lower()
        except Exception as e:
            print(f"Error loading configuration: {e}")
            self.config = None
            self.environment = None

    def get_api_url(self):
        if self.config is None:
            return None
        try:
            section = f'api_urls_{self.environment}'
            if section not in self.config:
                raise KeyError(f"Missing section [{section}] in config file.")
            api_url = self.config[section].get('API_URL', '').strip()
            if not api_url:
                raise ValueError(f"API URL not found in section [{section}]")
            return api_url
        except Exception as e:
            print(f"Error getting API URL: {e}")
            return None

    def get_value(self, section, key, default=None):
        if self.config is None:
            return default
        try:
            return self.config.get(section, key, fallback=default)
        except Exception:
            return default
