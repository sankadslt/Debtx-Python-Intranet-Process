# Import the main processing class for handling orders
from orderManipulator.OrderMani import Process_request

# Import a custom logger to track application events
from utils.logger.logger import get_logger

# Initialize the logger for tracking the status of the task
logger = get_logger("task_status_logger")

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
