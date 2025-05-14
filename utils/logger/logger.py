import logging
from logging import config

from utils.get_root_paths.get_root_paths import get_logger_filePath


def setup_logging():
    config_file = get_logger_filePath()  # Get the path to the logger config file

    try:
        config.fileConfig(config_file)
    except Exception as e:
        print(f"Error setting up logging: {e}")

def get_logger(logger_name):
    """Retrieve a logger by name."""
    return logging.getLogger(logger_name)

# Setup logging on module import
setup_logging()