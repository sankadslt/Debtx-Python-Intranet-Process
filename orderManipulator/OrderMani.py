# Import time module for any required time-based operations (e.g., delays, timestamps)
import time
# Import ThreadPoolExecutor to enable concurrent execution of tasks using threads
from concurrent.futures import ThreadPoolExecutor
# Import a utility function to connect to a MongoDB collection
from utils.database.connectMongoDB import get_mongo_collection
# Import the main incident processing class from the current package
from .caseRegistration import Process_Incident
# Import a custom logger utility to log messages for debugging, tracking, and auditing
from utils.logger.logger import get_logger


# Initialize a logger with the name "task_status_logger"
logger = get_logger("task_status_logger")

# If the logger has more than one handler attached (which can happen if this module is re-imported),
# retain only the first handler to prevent duplicate logs
if len(logger.handlers) > 1:
    logger.handlers = [logger.handlers[0]]
# which also helps avoid duplicate logging outputs
logger.propagate = False

class Process_request:
    
    def __init__(self):
        """
        Constructor for initializing the class with a MongoDB collection connection.

        Inputs:
            - None

        Outputs:
            - None (initializes instance variables)

        Raises:
            - ConnectionError: If the MongoDB collection could not be retrieved.

        Description:
            This constructor attempts to establish a connection to a MongoDB collection 
            using the `get_mongo_collection()` utility. If the connection fails 
            (i.e., the returned value is None), it raises a ConnectionError. 
            On success, it logs a message indicating that the connection has been established.
        """
        # Attempt to retrieve the MongoDB collection
        self.collection = get_mongo_collection()
        
        # Check if the connection was successful
        if self.collection is None:
            # Raise an error if the connection failed
            raise ConnectionError("Failed to connect to MongoDB collection")
        
        # Log a success message upon successful connection
        logger.info("MongoDB connection established successfully")


    def process_case(self, account_number, incident_id):
        """
        Processes an incident case for a given account.

        Inputs:
            account_number (str): The account number associated with the case.
            incident_id (str): The unique identifier of the incident.

        Outputs:
            None

        Returns:
            bool: 
                - True if the case was processed and the database was successfully updated.
                - False if processing failed or database update was unsuccessful.

        Description:
            This method handles processing of an incident by:
            1. Logging the start of the case processing.
            2. Initializing the Process_Incident object with required parameters.
            3. Running the incident processing logic and capturing the result.
            4. Preparing the update payload based on processing success or failure.
            5. Updating the corresponding document in the MongoDB collection.
            6. Logging the outcome of the update operation.
            7. Handling any exceptions that occur during the process.
        """

        # Log the initiation of case processing
        logger.info(f"Processing case for account: {account_number}, incident: {incident_id}")
        
        try:
            # Create a Process_Incident instance with necessary parameters
            process = Process_Incident(
                account_num=account_number,
                incident_id=incident_id,
                mongo_collection=self.collection
            )
            
            # Execute the incident processing logic
            success, response = process.process_incident()
            
            # Determine the request status based on the processing outcome
            status = "Completed" if success else "Failed"
            
            # Prepare data for updating the MongoDB document
            update_data = {
                "request_status": status,
                f"{status.lower()}_at": time.time(),  # Record timestamp of completion or failure
                "api_response": response if success else str(response)  # Store API response or error message
            }
            
            # Update the document in the MongoDB collection
            update_result = self.collection.update_one(
                {
                    "$or": [
                        {"account_number": account_number},
                        {"account_num": account_number}
                    ],
                    "parameters.incident_id": incident_id,
                    "request_status": "Open"  # Only update if the case is still open
                },
                {"$set": update_data}
            )
            
            # Check if the document was successfully modified
            if update_result.modified_count == 1:
                logger.info(f"Marked document as {status} for account {account_number}")
                return True
            
            # Log a warning if the document update did not occur
            logger.warning(f"Failed to update document for account {account_number}")
            return False

        except Exception as e:
            # Log any errors encountered during case processing
            logger.error(f"Error processing case for account {account_number}: {str(e)}")
            return False


    def process_single_document(self, doc):
        """
        Processes a single MongoDB document representing an incident case.

        Inputs:
            doc (dict): A dictionary representing a document fetched from the MongoDB collection.
                        Expected to contain at least an account number (`account_number` or `account_num`)
                        and a nested incident ID under `parameters.incident_id`.

        Outputs:
            None

        Returns:
            bool:
                - True if the document was successfully processed.
                - False if required fields are missing or an error occurred.

        Description:
            This method extracts the `account_number` and `incident_id` from the input document.
            If either field is missing, it logs a warning and skips processing.
            Otherwise, it forwards the data to `process_case()` for further handling.
            All exceptions are caught and logged, and `False` is returned in case of failure.
        """

        try:
            # Attempt to extract the account number (with fallback key options)
            account_number = doc.get('account_number') or doc.get('account_num')
            
            # Attempt to extract the incident ID from the nested 'parameters' field
            incident_id = doc.get('parameters', {}).get('incident_id')
            
            # Skip processing if either key piece of data is missing
            if not account_number or not incident_id:
                logger.warning(f"Skipping document {doc.get('_id')} - missing required fields")
                return False
            
            # Proceed with case processing if all required fields are present
            return self.process_case(account_number, incident_id)
        
        except Exception as e:
            # Log any errors that occur during document processing
            logger.error(f"Error processing document {doc.get('_id')}: {str(e)}")
            return False


    def process_option_1(self, documents): # name this in case creation 
        """
        Processes documents with `order_id` equal to 1.

        Inputs:
            documents (list): A list of MongoDB document dictionaries to be processed.

        Outputs:
            None

        Returns:
            tuple:
                - processed_count (int): Number of documents successfully processed.
                - error_count (int): Number of documents that failed to process.

        Description:
            Iterates through the list of input documents, and for each document:
            - Checks if its 'order_id' field equals 1.
            - If so, processes it using `process_single_document`.
            - Keeps track of how many documents were processed successfully vs. failed.
            - Waits for 1 second between each document to apply rate limiting.
            - Logs a summary of the processing outcome.
        """

        # Initialize counters for success and failure
        processed_count = 0
        error_count = 0
        
        # Iterate through each document in the list
        for doc in documents:
            # Check if the document should be processed based on its order_id
            if doc.get('order_id') == 1:
                # Process the document and update success/error counters
                if self.process_single_document(doc):
                    processed_count += 1
                else:
                    error_count += 1
                
                # Sleep to prevent rapid-fire processing (rate limiting)
                time.sleep(1)
        
        # Log the final result of the processing batch
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        
        # Return the counts as a tuple
        return processed_count, error_count


    def process_option_2(self, documents):
        logger.info("Processing option 2 - Monitor Payment")
        # Implement logic for option 2
        processed_count = 0
        error_count = 0
        for doc in documents:
            if doc.get('order_id') == 2:
                # Add your logic for Monitor Payment
                logger.info(f"Monitoring payment for document {doc.get('_id')}")
                processed_count += 1
                time.sleep(1)  # Rate limiting
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def process_option_3(self, documents):
        logger.info("Processing option 3 - Monitor Payment Cancel")
        # Implement logic for option 3
        processed_count = 0
        error_count = 0
        for doc in documents:
            if doc.get('order_id') == 3:
                # Add your logic for Monitor Payment Cancel
                logger.info(f"Canceling payment monitoring for document {doc.get('_id')}")
                processed_count += 1
                time.sleep(1)  # Rate limiting
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def process_option_4(self, documents):
        logger.info("Processing option 4 - Close_Monitor_If_No_Transaction")
        # Implement logic for option 4
        processed_count = 0
        error_count = 0
        for doc in documents:
            if doc.get('order_id') == 4:
                # Add your logic for Close_Monitor_If_No_Transaction
                logger.info(f"Closing monitor for document {doc.get('_id')}")
                processed_count += 1
                time.sleep(1)  # Rate limiting
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def get_open_orders(self):
        """
        Fetches all documents from the MongoDB collection where the request status is 'Open'.

        Inputs:
            None

        Outputs:
            None

        Returns:
            list: A list of documents (dictionaries) representing open orders.

        Description:
            This method queries the MongoDB collection for all records where the field
            'request_status' is set to 'Open'. The result is returned as a list of documents.
        """
        # Query the MongoDB collection for all documents with 'request_status' = 'Open'
        return list(self.collection.find({"request_status": "Open"}))


    def process_selected_option(self, option, documents):
        """
        Routes processing to the appropriate method based on the selected option.

        Inputs:
            option (int): The selected processing mode (expected values: 1, 2, 3, 4).
            documents (list): A list of MongoDB documents to process.

        Outputs:
            None

        Returns:
            None

        Description:
            Uses a match-case control structure to route the given documents to the
            corresponding processing function based on the user's selected option.
            - Option 1: Calls `process_option_1()` to handle documents with order_id == 1.
            - Options 2–4: Reserved for future functionality; currently placeholders.
            - Default: Logs a warning if an invalid option is selected.
        """

        match option:
            case 1:
                # Process documents using logic defined in process_option_1
                self.process_option_1(documents)
            
            case 2:
                # Placeholder for future implementation of option 2
                print()
                # self.process_option_2(documents)
            
            case 3:
                # Placeholder for future implementation of option 3
                print()
                # self.process_option_3(documents)
            
            case 4:
                # Placeholder for future implementation of option 4
                print()
                # self.process_option_4(documents)
            
            case _:
                # Log a warning for unsupported options
                logger.warning(f"Invalid option selected: {option}")


    def run_process(self):
        """
        Main method to continuously process open orders in a loop.

        Inputs:
            None

        Outputs:
            None

        Returns:
            None

        Description:
            This method is the core of the order processing system. It runs in an infinite loop
            to continuously fetch and process open orders. For each batch of open orders:
            1. It checks if there are any orders to process.
            2. If no open orders are found, it waits for 5 seconds before checking again.
            3. If open orders exist, it groups them by `order_id` (from 1 to 4) and processes each group.
            4. It ensures that documents with the same `order_id` are processed together to optimize processing.
            5. If any error occurs, it logs the error and waits 5 seconds before retrying.
            6. If the user interrupts the process (via keyboard), the loop terminates gracefully.
        """

        # Log the start of the order processing cycle
        logger.info("Starting Order Processor")
        
        # Infinite loop to keep processing orders until termination
        while True:
            try:
                # Retrieve open orders from the database
                open_orders = self.get_open_orders()
                
                # If no open orders, log and wait for a while before checking again
                if not open_orders:
                    logger.info("No open orders found. Waiting...")
                    time.sleep(5)
                    continue
                
                # Log the number of open orders found
                logger.info(f"Found {len(open_orders)} open orders")
                
                # Group documents by order_id (assumed range 1 to 4) for efficient processing
                optionCount = 4
                for option in range(1, optionCount):  # Processing options 1 to 4
                    # Filter orders by their 'order_id'
                    relevant_docs = [doc for doc in open_orders if doc.get('order_id') == option]
                    
                    # If there are relevant documents for the current option, process them
                    if relevant_docs:
                        logger.info(f"Processing {len(relevant_docs)} documents for order_id {option}")
                        self.process_selected_option(option, relevant_docs)
                
                # Sleep for a short time to prevent excessive resource usage
                time.sleep(1)
            
            # Gracefully handle user interruption (Ctrl+C)
            except KeyboardInterrupt:
                logger.info("Program terminated by user")
                break
            
            # Catch any other unexpected exceptions and log them
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                time.sleep(5)  # Wait before retrying
