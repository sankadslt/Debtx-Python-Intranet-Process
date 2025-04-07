import time
from utils.database.connectMongoDB import get_mongo_collection
from .caseRegistration import IncidentProcessor
from utils.logger.logger import get_logger

# Initialize logger for order processing tasks
logger = get_logger("task_status_logger")

class OrderProcessor:
    """
    Main class for processing customer orders and managing case registration workflows.
    Handles MongoDB interactions, order processing, and provides a user menu interface.
    """
    
    def __init__(self):
        """Initialize MongoDB connection and verify collection access"""
        self.collection = get_mongo_collection()
        if self.collection is None:
            raise ConnectionError("Failed to connect to MongoDB collection")
        logger.info("MongoDB connection established successfully")

    def process_case(self, account_number, incident_id):
        """
        Process customer details for case registration and update MongoDB document on success.
        
        Args:
            account_number (str): Customer account number to process
            incident_id (int): Associated incident ID for the case
            
        Returns:
            bool: True if processing and update were successful, False otherwise
        """
        logger.info(f"Processing case for account: {account_number}, incident: {incident_id}")
        
        # Initialize incident processor with account details
        processor = IncidentProcessor(
            account_num=account_number,
            incident_id=incident_id,
            mongo_collection=self.collection
        )
        
        # Process the incident (retrieve data, format, send to API)
        success, response = processor.process_incident()
        
        if success:
            # Update MongoDB document to mark as completed
            update_result = self.collection.update_one(
                {
                    "$or": [
                        {"account_number": account_number},
                        {"account_num": account_number}  # Handle different field names
                    ],
                    "parameters.incident_id": incident_id,
                    "request_status": "Open"  # Only update open requests
                },
                {
                    "$set": {
                        "request_status": "Completed",
                        "completed_at": time.time(),  # Current timestamp
                        "api_response": response  # Store API response
                    }
                }
            )
            
            if update_result.modified_count == 1:
                logger.info(f"Successfully updated document for account {account_number}")
                return True
            else:
                logger.warning(f"Failed to update document for account {account_number}")
                return False
        return False

    def get_open_orders(self):
        """
        Retrieve all open orders from MongoDB collection.
        
        Returns:
            list: Collection of documents with request_status="Open"
        """
        return list(self.collection.find({"request_status": "Open"}))

    def process_option_1(self, documents):
        """
        Process documents for Option 1 (Customer Details for Case Registration).
        Filters documents by order_id=1 and processes valid cases.
        
        Args:
            documents (list): MongoDB documents to process
            
        Returns:
            tuple: (processed_count, error_count) tracking successful and failed operations
        """
        processed_count = 0
        error_count = 0
        
        for doc in documents:
            try:
                doc_id = doc.get('_id', 'NO_ID')  # Get document ID or default
                
                # Skip documents not matching option 1 criteria
                if doc.get('order_id') != 1:
                    continue
                    
                # Extract required fields with fallbacks
                account_number = doc.get('account_number') or doc.get('account_num')
                parameters = doc.get('parameters', {})
                incident_id = parameters.get('incident_id')
                
                # Validate required fields
                if not account_number:
                    logger.warning(f"Missing 'account_number' in document: {doc_id}")
                    error_count += 1
                    continue
                if not incident_id:
                    logger.warning(f"Missing 'incident_id' in document: {doc_id}")
                    error_count += 1
                    continue
                    
                # Process valid case and track results
                if self.process_case(account_number, incident_id):
                    processed_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing document {doc_id}: {str(e)}")
                continue
                
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def show_menu(self):
        """
        Display interactive menu to user and capture selection.
        
        Returns:
            int: User-selected option (1-4) or None for invalid input
        """
        print("\nSelect an option:")
        print("1: Cust Details for Case Registration")
        print("2: Monitor Payment")
        print("3: Monitor Payment Cancel")
        print("4: Close_Monitor_If_No_Transaction")
        try:
            return int(input("Enter option (1-4): "))
        except ValueError:
            logger.warning("Invalid menu input - expected number 1-4")
            return None

    def process_selected_option(self, option, documents):
        """
        Route processing to the appropriate handler based on user selection.
        
        Args:
            option (int): User-selected menu option
            documents (list): MongoDB documents to process
        """
        match option:
            case 1:
                self.process_option_1(documents)  # Case registration
            case 2:
                logger.info("Option 2 selected - Monitor Payment")
                # Future implementation
            case 3:
                logger.info("Option 3 selected - Monitor Payment Cancel")
                # Future implementation
            case 4:
                logger.info("Option 4 selected - Close_Monitor_If_No_Transaction")
                # Future implementation
            case _:
                logger.warning(f"Invalid option selected: {option}")

    def run(self):
        """
        Main processing loop that continuously:
        1. Checks for open orders
        2. Displays menu
        3. Processes selected option
        Handles user interrupts and unexpected errors gracefully.
        """
        logger.info("Starting Order Processor")
        while True:
            try:
                # Check for open orders periodically
                open_orders = self.get_open_orders()
                
                if not open_orders:
                    logger.info("No open orders found. Waiting...")
                    time.sleep(5)  # Wait before checking again
                    continue
                    
                logger.info(f"Found {len(open_orders)} open orders")
                
                # Get user input and validate
                option = self.show_menu()
                if option is None:
                    print("Invalid input. Please enter a number between 1-4.")
                    continue
                
                # Process the selected option
                self.process_selected_option(option, open_orders)
                time.sleep(1)  # Brief pause between operations
                
            except KeyboardInterrupt:
                logger.info("Program terminated by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                time.sleep(5)  # Wait after error before retrying

if __name__ == "__main__":
    try:
        # Start the order processor
        processor = OrderProcessor()
        processor.run()
    except Exception as e:
        logger.critical(f"Failed to start OrderProcessor: {e}")