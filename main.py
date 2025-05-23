'''
 Main.py file is as follows:
    Purpose: This is the main run point of the program.
    Created Date: 
    Created By: Dulhan Perera
    Modified By: Dulhan Perera
    Version: Python 3.9
    Notes:
'''

# Import the main processing class for handling orders
from process.Process_request import Process_request

# Import Singleton utilities for logging
from utils.logger import SingletonLogger

# Initialize the logger using SingletonLogger
SingletonLogger.configure()
logger = SingletonLogger.get_logger("appLogger")

# Entry point of the script
if __name__ == "__main__":
    try:
        # Log the start of the order processing system
        logger.info("Starting Order Processing System")
        
        # Create an instance of the order processing class
        execute_process = Process_request()
        
        # Run the run_process method to start processing orders
        execute_process.run_process()
    
    # Catch any unexpected errors and log them as critical
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
    
    # Log the system shutdown message regardless of success or failure
    finally:
        logger.info("System shutdown")