import time
from concurrent.futures import ThreadPoolExecutor
from utils.database.connectMongoDB import get_mongo_collection
from .caseRegistration import IncidentProcessor
from utils.logger.logger import get_logger

logger = get_logger("task_status_logger")
if len(logger.handlers) > 1:
    logger.handlers = [logger.handlers[0]]
logger.propagate = False

class Process_request:
    
    def __init__(self):
        self.collection = get_mongo_collection()
        if self.collection is None:
            raise ConnectionError("Failed to connect to MongoDB collection")
        logger.info("MongoDB connection established successfully")

    def process_case(self, account_number, incident_id):
        logger.info(f"Processing case for account: {account_number}, incident: {incident_id}")
        
        try:
            processor = IncidentProcessor(
                account_num=account_number,
                incident_id=incident_id,
                mongo_collection=self.collection
            )
            
            success, response = processor.process_incident()
            
            status = "Completed" if success else "Failed"
            update_data = {
                "request_status": status,
                f"{status.lower()}_at": time.time(),
                "api_response": response if success else str(response)
            }
            
            update_result = self.collection.update_one(
                {
                    "$or": [
                        {"account_number": account_number},
                        {"account_num": account_number}
                    ],
                    "parameters.incident_id": incident_id,
                    "request_status": "Open"
                },
                {"$set": update_data}
            )
            
            if update_result.modified_count == 1:
                logger.info(f"Marked document as {status} for account {account_number}")
                return True
            logger.warning(f"Failed to update document for account {account_number}")
            return False
            
        except Exception as e:
            logger.error(f"Error processing case for account {account_number}: {str(e)}")
            return False

    def process_single_document(self, doc):
        try:
            if doc.get('order_id') != 1:
                return False
                
            account_number = doc.get('account_number') or doc.get('account_num')
            incident_id = doc.get('parameters', {}).get('incident_id')
            
            if not account_number or not incident_id:
                logger.warning(f"Skipping document {doc.get('_id')} - missing required fields")
                return False
                
            return self.process_case(account_number, incident_id)
            
        except Exception as e:
            logger.error(f"Error processing document {doc.get('_id')}: {str(e)}")
            return False

    def process_option_1(self, documents):
        processed_count = 0
        error_count = 0
        
        # Sequential processing (safe)
        for doc in documents:
            if doc.get('order_id') == 1:
                if self.process_single_document(doc):
                    processed_count += 1
                else:
                    error_count += 1
                time.sleep(1)  # Rate limiting
        
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def get_open_orders(self):
        return list(self.collection.find({"request_status": "Open"}))

    def show_menu(self):
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
        match option:
            case 1:
                self.process_option_1(documents)
            case 2:
                logger.info("Option 2 selected - Monitor Payment")
            case 3:
                logger.info("Option 3 selected - Monitor Payment Cancel")
            case 4:
                logger.info("Option 4 selected - Close_Monitor_If_No_Transaction")
            case _:
                logger.warning(f"Invalid option selected: {option}")

    def run_process(self):
        logger.info("Starting Order Processor")
        while True:
            try:
                open_orders = self.get_open_orders()
                
                if not open_orders:
                    logger.info("No open orders found. Waiting...")
                    time.sleep(5)
                    continue
                    
                logger.info(f"Found {len(open_orders)} open orders")
                
                option = self.show_menu()
                if option is None:
                    print("Invalid input. Please enter a number between 1-4.")
                    continue
                
                self.process_selected_option(option, open_orders)
                time.sleep(1)
                
            except KeyboardInterrupt:
                logger.info("Program terminated by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                time.sleep(5)