import configparser
from urllib.parse import urlparse
from pathlib import Path
from utils.logger.logger import get_logger

logger = get_logger("API_Config")

def read_api_config() -> str:
    """Directly reads config with fallback paths"""
    config_paths = [
        Path(r"D:\SLT_DRS\Git_DRS\request_log\Config\databaseConfig.ini"),  # Primary path
        Path(__file__).parent.parent.parent / "Config" / "databaseConfig.ini"  # Fallback
    ]

    config = configparser.ConfigParser()
    
    for path in config_paths:
        try:
            if path.exists():
                config.read(str(path))
                if 'API' in config and config['API'].get('api_url'):
                    url = config['API']['api_url'].strip()
                    if url:
                        parsed = urlparse(url)
                        if parsed.scheme and parsed.netloc:
                            logger.info(f"Using API URL: {url}")
                            return url
        except Exception as e:
            logger.warning(f"Failed to read {path}: {e}")

    logger.error("No valid API configuration found in any path")
    raise ValueError("API URL not configured")