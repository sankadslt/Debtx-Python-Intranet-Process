from orderManipulator.OrderMani import OrderProcessor
from utils.logger.logger import get_logger

logger = get_logger("OrderProcessor")

if __name__ == "__main__":
    try:
        processor = OrderProcessor()
        processor.run()
    except Exception as e:
        logger.critical(f"Failed to start OrderProcessor: {e}")