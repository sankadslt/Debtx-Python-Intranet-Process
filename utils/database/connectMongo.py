import configparser
from pymongo import MongoClient
import os
from utils.logger.logger import get_logger
from utils.custom_exceptions.customize_exceptions import DatabaseConnectionError
from utils.get_root_paths.get_root_paths import get_config_filePath #import get_project_root drs_config.ini

# Initialize logger
logger = get_logger("database_logger")

# Read configuration from databaseConfig.ini file
def get_db_connection():
    """
    Establishes a connection to the MongoDB database using the configuration file.

    - Reads the MongoDB URI and database name from the `databaseConfig.ini` file.
    - Validates the presence of the configuration file and required fields.
    - Establishes a connection to the MongoDB database and returns the database object.

    :return: MongoDB database object.
    :raises Exception: If the configuration file is missing or connection fails.
    """
    try:
        config_path = get_config_filePath()  # Get the path to the config file
        logger.info(f"Reading configuration file from: {config_path}")  # Debugging log
        if not os.path.exists(config_path):
            logger.error(f"Configuration file '{config_path}' not found.")
            raise FileNotFoundError(f"Configuration file '{config_path}' not found.")

        config = configparser.ConfigParser()
        config.read(config_path)

        if 'MONGODB' not in config:
            logger.error("'MONGODB' section not found in databaseConfig.ini")
            raise ValueError("'MONGODB' section not found in databaseConfig.ini")

        mongo_uri = config['MONGODB'].get('MONGO_URI', '').strip()
        db_name = config['MONGODB'].get('DATABASE_NAME', '').strip()

        if not mongo_uri or not db_name:
            logger.error("Missing MONGO_URI or DATABASE_NAME in databaseConfig.ini")
            raise ValueError("Missing MONGO_URI or DATABASE_NAME in databaseConfig.ini")

        client = MongoClient(mongo_uri)
        db = client[db_name]
        return db
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise DatabaseConnectionError(f"Error connecting to MongoDB: {e}")

# Get a specific collection
def get_mongo_collection(collection_name):
    """
    Retrieves a specific MongoDB collection.

    - Uses the `get_db_connection` function to establish a connection to the database.
    - Fetches the specified collection from the connected database.

    :param collection_name: Name of the MongoDB collection to fetch.
    :return: MongoDB collection object.
    :raises Exception: If the database connection fails.
    """
    try:
        # Use the get_db_connection function to establish the database connection
        db = get_db_connection()
        logger.info(f"Connected to MongoDB database: {db.name}")
        return db[collection_name]
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise DatabaseConnectionError(f"Failed to connect to MongoDB: {e}")
