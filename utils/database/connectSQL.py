import pymysql
import configparser
from utils.logger.logger import get_logger
from utils.custom_exceptions.customize_exceptions import DatabaseConnectionError
from utils.get_root_paths.get_root_paths import get_config_filePath

logger = get_logger("connectSQL")

def get_mysql_connection():
    
    """
    Establishes a MySQL connection using the configuration from DB_Config.ini.
    :return: A MySQL connection object.
    """
    config = configparser.ConfigParser()
    # config_path = get_filePath("databaseConfig")
    config_path = get_config_filePath()

    try:
        config.read(config_path)
        if 'DATABASE' not in config:
            raise KeyError(f"'DATABASE' section missing in {config_path}")

        connection = pymysql.connect(
            host=config['DATABASE']['MYSQL_HOST'],
            database=config['DATABASE']['MYSQL_DATABASE'],
            user=config['DATABASE']['MYSQL_USER'],
            password=config['DATABASE']['MYSQL_PASSWORD']
        )
        logger.info("Successfully connected to MySQL.")
        return connection
    except KeyError as e:
        logger.error(f"Configuration error: {e}")
        raise DatabaseConnectionError(f"Configuration error: {e}")
    except Exception as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise DatabaseConnectionError(f"Error connecting to MySQL: {e}")
    return None
