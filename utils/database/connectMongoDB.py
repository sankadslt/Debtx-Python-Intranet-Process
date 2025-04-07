import pymongo
from pymongo import MongoClient
import configparser
from utils.logger.logger import get_logger
from utils.filePath.filePath import get_filePath

logger = get_logger("task_status_logger")

def get_mongo_config():
    """
    Returns MongoDB configuration as a dictionary (hash map)
    without authentication parameters
    """
    config = configparser.ConfigParser()
    config_file = get_filePath("databaseConfig")
    
    config_map = {
        'mongo_uri': 'mongodb://localhost:27017/',
        'db_name': 'DRS',
        'collection_name': 'Request_Progress_Log'
    }

    try:
        config.read(config_file)
        if 'MONGODB' in config:
            config_map.update({
                'mongo_uri': config['MONGODB'].get('MONGO_URI', config_map['mongo_uri']),
                'db_name': config['MONGODB'].get('DRS_DATABASE', config_map['db_name']),
                'collection_name': config['MONGODB'].get('REQUEST_PROGRESS_LOG_COLLECTION', 
                                      config_map['collection_name'])
            })
        return config_map
    except Exception as e:
        logger.error(f"Error reading MongoDB config: {e}")
        return config_map  # Return defaults if error occurs

def get_mongo_connection():
    """
    Establishes MongoDB connection without authentication
    Returns: {
        'config': {configuration hash map},
        'client': MongoClient,
        'db': Database,
        'collection': Collection
    }
    """
    try:
        config = get_mongo_config()
        
        # Establish connection (no authentication)
        client = MongoClient(config['mongo_uri'])
        db = client[config['db_name']]
        collection = db[config['collection_name']]
        
        # Verify connection
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        
        return {
            'config': config,
            'client': client,
            'db': db,
            'collection': collection
        }

    except pymongo.errors.ConnectionFailure as e:
        logger.error(f"MongoDB connection failed: {e}")
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
    
    return None

def get_mongo_collection():
    """
    Convenience function that returns just the collection object
    Returns: MongoDB collection object or None if connection fails
    """
    connection = get_mongo_connection()
    return connection['collection'] if connection else None