from orderManipulator.OrderMani import Process_request
from utils.logger.logger import get_logger

logger = get_logger("task_status_logger")

if __name__ == "__main__":
    try:
        processor = Process_request()
        processor.run_process()
    except Exception as e:
        logger.critical(f"Failed to start OrderProcessor: {e}")