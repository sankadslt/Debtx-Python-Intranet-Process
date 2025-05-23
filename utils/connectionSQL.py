import threading
import configparser
from pathlib import Path
import mysql.connector
from mysql.connector import Error
from utils.logger import SingletonLogger

class DBConnectionSingleton:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DBConnectionSingleton, cls).__new__(cls)
                    cls._instance._initialize_connection()
        return cls._instance

    def _initialize_connection(self):
        self.logger = SingletonLogger.get_logger('dbLogger')
        try:
            # Load connection details from config
            project_root = Path(__file__).resolve().parents[1]
            config_path = project_root / 'config' / 'core_config.ini'
            config = configparser.ConfigParser()
            config.read(str(config_path))

            if 'environment' not in config or 'current' not in config['environment']:
                raise KeyError("Missing [environment] section or 'current' key in core_config.ini")

            env = config['environment']['current'].lower()
            section = f'database_{env}'

            if section not in config:
                raise KeyError(f"Missing section [{section}] in config file.")

            self.db_config = {
                'host': config[section].get('MYSQL_HOST', 'localhost'),
                'database': config[section].get('MYSQL_DATABASE', ''),
                'user': config[section].get('MYSQL_USER', ''),
                'password': config[section].get('MYSQL_PASSWORD', '')
            }

            if not self.db_config['database'] or not self.db_config['user']:
                raise ValueError(f"Database name or user missing in [{section}] configuration.")

            self.connection = mysql.connector.connect(**self.db_config)

            if self.connection.is_connected():
                self.logger.info("MySQL connection established successfully.")
            else:
                self.logger.error("Failed to establish MySQL connection.")
                self.connection = None

        except KeyError as key_err:
            self.logger.error(f"Configuration error: {key_err}")
            self.connection = None
        except Exception as err:
            self.logger.error(f"Error connecting to MySQL: {err}")
            self.connection = None

    def get_connection(self):
        return self.connection

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            try:
                self.connection.close()
                self.logger.info("MySQL connection closed.")
                self.connection = None
                DBConnectionSingleton._instance = None
            except Exception as err:
                self.logger.error(f"Error closing MySQL connection: {err}")

    def __enter__(self):
        return self  # Return the instance itself

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()