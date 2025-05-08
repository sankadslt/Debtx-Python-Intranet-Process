'''
 jsonMapping.py file is as follows:
    Purpose: This script handles database connections and incident creation for debt collection.
    Created Date: 
    Created By: Dulhan Perera
    Modified By: Dulhan Perera
    Version: Python 3.9
    Dependencies: json, datetime, decimal, requests, mysql.connector, utils.logger, utils.connectionSQL, utils.connectAPI, utils.custom_exceptions
    Notes:
'''

# Import time module for any required time-based operations (e.g., delays, timestamps)
import time
# Import ThreadPoolExecutor to enable concurrent execution of tasks using threads
from concurrent.futures import ThreadPoolExecutor
# Import mysql.connector for MySQL database interactions
import mysql.connector
# Import the main incident processing class
from process.MonitorPayment import MonitorPayment
from process.jsonMapping import CreateIncident
# Import Singleton utilities for logging and MySQL connection
from utils.logger import SingletonLogger
from utils.connectionSQL import MySQLConnectionSingleton
# Import custom exceptions for error handling
from utils.custom_exceptions.customize_exceptions import DatabaseConnectionError

# Initialize the logger using SingletonLogger
SingletonLogger.configure()
logger = SingletonLogger.get_logger("appLogger")

class Process_request:
    
    def __init__(self):
        """
        Constructor for initializing the class with a MySQL connection.

        Inputs:
            - None

        Outputs:
            - None (initializes instance variables)

        Raises:
            - DatabaseConnectionError: If the MySQL connection could not be established.

        Description:
            This constructor initializes the MySQL connection using MySQLConnectionSingleton.
            If the connection fails, it raises a DatabaseConnectionError.
            On success, it logs a message indicating that the connection has been established.
        """
        # Log a success message upon successful connection
        logger.info("MySQL connection initialization attempted")
        # No need to store connection here, as MySQLConnectionSingleton manages it

    def process_case(self, account_number, incident_id, request_id):
        """
        Processes an incident case for a given account.

        Inputs:
            account_number (str): The account number associated with the case.
            incident_id (str): The unique identifier of the incident (from para_1 in request_log_details).
            request_id (int): The request ID to correlate updates across tables.

        Outputs:
            None

        Returns:
            bool: 
                - True if the case was processed and the database was successfully updated.
                - False if processing failed or database update was unsuccessful.

        Description:
            This method handles processing of an incident by:
            1. Logging the start of the case processing.
            2. Initializing the CreateIncident object with required parameters.
            3. Running the incident processing logic and capturing the result.
            4. Updating the request_progress_log and request_log tables based on success or failure.
            5. Logging the outcome of the update operation.
            6. Handling any exceptions that occur during the process.
        """
        logger.info(f"Processing case for account: {account_number}, incident: {incident_id}, request: {request_id}")
        
        try:
            # Create a CreateIncident instance
            process = CreateIncident(
                account_num=account_number,
                incident_id=incident_id
            )
            
            # Execute the incident processing logic
            success = process.process_incident()
            
            # Determine the request status based on the processing outcome
            status = "Completed" if success else "Error"
            
            # Update the request_progress_log and request_log tables
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for updating request_progress_log.")
                
                cursor = connection.cursor(dictionary=True)
                try:
                    # Update request_progress_log
                    update_progress_query = """
                        UPDATE request_progress_log
                        SET Request_Status = %s, Request_Status_Dtm = NOW()
                        WHERE Request_Id = %s AND account_num = %s AND Request_Status = 'Open'
                    """
                    cursor.execute(update_progress_query, (status, request_id, account_number))
                    connection.commit()
                    
                    # Check if the update was successful
                    if cursor.rowcount == 1:
                        logger.info(f"Marked request_progress_log as {status} for account {account_number}, request {request_id}")
                    else:
                        logger.warning(f"Failed to update request_progress_log for account {account_number}, request {request_id}")
                        return False

                    # Update request_log
                    update_request_log_query = """
                        UPDATE request_log
                        SET Request_Status = %s, Request_Status_Dtm = NOW()
                        WHERE Request_Id = %s AND account_num = %s AND Request_Status = 'Open'
                    """
                    cursor.execute(update_request_log_query, (status, request_id, account_number))
                    connection.commit()
                    
                    # Check if the update was successful
                    if cursor.rowcount == 1:
                        logger.info(f"Marked request_log as {status} for account {account_number}, request {request_id}")
                        return success
                    else:
                        logger.warning(f"Failed to update request_log for account {account_number}, request {request_id}")
                        return False
                    
                finally:
                    cursor.close()
            
        except Exception as e:
            logger.error(f"Error processing case for account {account_number}, request {request_id}: {str(e)}")
            return False

    def process_single_document(self, row):
        """
        Processes a single MySQL row representing an incident case.

        Inputs:
            row (dict): A dictionary representing a row fetched from the request_progress_log table.
                        Expected to contain at least Request_Id and account_num.

        Outputs:
            None

        Returns:
            bool:
                - True if the row was successfully processed.
                - False if required fields are missing or an error occurred.

        Description:
            This method extracts the Request_Id and account_num from the input row, then queries
            the request_log_details table to get the incident_id from para_1. If the required fields
            are present, it forwards the data to process_case() for further handling.
        """
        try:
            # Extract the request ID and account number
            request_id = row.get('Request_Id')
            account_number = row.get('account_num')
            
            # Skip processing if required fields are missing
            if not request_id or not account_number:
                logger.warning(f"Skipping row - missing required fields: Request_Id={request_id}, account_num={account_number}")
                return False
            
            # Validate request_id type
            try:
                request_id = int(request_id)
                logger.debug(f"Request_Id type: {type(request_id)}, value: {request_id}")
            except (TypeError, ValueError) as e:
                logger.error(f"Invalid Request_Id: {request_id}, error: {str(e)}")
                return False
            
            # Query request_log_details to get incident_id from para_1
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for fetching incident_id.")
                
                cursor = connection.cursor(dictionary=True)
                try:
                    query = """
                        SELECT para_1 AS incident_id 
                        FROM request_log_details 
                        WHERE Request_Id = %s
                    """
                    logger.debug(f"Executing query: {query % request_id}")
                    # Explicitly pass request_id as a single-element tuple
                    cursor.execute(query, (request_id,))
                    result = cursor.fetchone()
                    
                    if not result or not result.get('incident_id'):
                        logger.warning(f"No incident_id found in request_log_details for Request_Id={request_id}")
                        return False
                    
                    incident_id = result['incident_id']
                    logger.debug(f"Retrieved incident_id: {incident_id} for Request_Id={request_id}, account_num={account_number}")
                    
                    # Proceed with case processing
                    return self.process_case(account_number, incident_id, request_id)
                
                finally:
                    cursor.close()
        
        except Exception as e:
            logger.error(f"Error processing row for account {account_number}, request {request_id}: {str(e)}")
            return False

    def customer_details_for_case_registration(self, rows):
        """
        Processes rows with Order_Id equal to 1.

        Inputs:
            rows (list): A list of MySQL row dictionaries to be processed.

        Outputs:
            None

        Returns:
            tuple:
                - processed_count (int): Number of rows successfully processed.
                - error_count (int): Number of rows that failed to process.
        """
        processed_count = 0
        error_count = 0
        
        for row in rows:
            if row.get('Order_Id') == 1:
                if self.process_single_document(row):
                    processed_count += 1
                else:
                    error_count += 1
                time.sleep(1)  # Rate limiting
        
        logger.info(f"Successfully Processed {processed_count} document(s), {error_count} errors")
        return processed_count, error_count

    def monitor_payment(self, rows):
        """
        Process rows with Order_Id equal to 2 (Payment Monitoring).
        
        Args:
            rows (list): A list of MySQL row dictionaries to process.
            
        Returns:
            tuple: (processed_count, error_count)
        """
        logger.info("Processing option 2 - Monitor Payment")
        processed_count = 0
        error_count = 0
        
        # Initialize the MonitorPayment class
        monitor_payment = MonitorPayment()
        
        for row in rows:
            if row.get('Order_Id') == 2:
                if monitor_payment.process_monitoring_request(row):
                    processed_count += 1
                else:
                    error_count += 1
                time.sleep(1)  # Rate limiting
        
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def monitor_payment_cancel(self, rows):
        """
        Process rows with Order_Id equal to 3 (Monitor Payment Cancel).
        
        Args:
            rows (list): A list of MySQL row dictionaries to process.
            
        Returns:
            tuple: (processed_count, error_count)
        """
        logger.info("Processing option 3 - Monitor Payment Cancel")
        processed_count = 0
        error_count = 0
        
        monitor_payment = MonitorPayment()
        
        for row in rows:
            if row.get('Order_Id') == 3:
                if monitor_payment.cancel_monitoring_request(row):
                    processed_count += 1
                else:
                    error_count += 1
                time.sleep(1)  # Rate limiting
        
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def close_monitor_if_no_transaction(self, rows):
        """
        Placeholder for processing rows with Order_Id equal to 4.
        """
        logger.info("Processing option 4 - Close_Monitor_If_No_Transaction")
        print("Processing Order_Id 4: Close_Monitor_If_No_Transaction")
        processed_count = 0
        error_count = 0
        for row in rows:
            if row.get('Order_Id') == 4:
                logger.info(f"Closing monitor for account {row.get('account_num')}")
                processed_count += 1
                time.sleep(1)
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def get_open_orders(self):
        """
        Fetches all rows from the request_progress_log table where the Request_Status is 'Open'.

        Inputs:
            None

        Outputs:
            None

        Returns:
            list: A list of rows (dictionaries) representing open orders.
        """
        try:
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for fetching open orders.")
                
                cursor = connection.cursor(dictionary=True)
                try:
                    query = """
                        SELECT * FROM request_progress_log 
                        WHERE Request_Status = 'Open'
                    """
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    
                    logger.info(f"Fetched {len(rows)} open orders from request_progress_log")
                    return rows
                finally:
                    cursor.close()
        except Exception as e:
            logger.error(f"Error fetching open orders: {str(e)}")
            return []

    def process_selected_option(self, option, rows):
        """
        Routes processing to the appropriate method based on the selected option.

        Inputs:
            option (int): The selected processing mode (expected values: 1, 2, 3, 4).
            rows (list): A list of MySQL rows to process.

        Outputs:
            None

        Returns:
            None
        """
        match option: #TODO: See if can this switch case remove and just get the option directly.
            case 1:
                self.customer_details_for_case_registration(rows) #TODO: Change these methods into actual order process names.
            case 2:
                self.monitor_payment(rows)
            case 3:
                self.monitor_payment_cancel(rows)
            case 4:
                self.close_monitor_if_no_transaction(rows)
            case _:
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
        """
        logger.info("Starting Order Processor")
        
        while True:
            try:
                open_orders = self.get_open_orders()
                
                if not open_orders:
                    logger.info("No open orders found. Waiting...")
                    time.sleep(5)
                    continue
                
                logger.info(f"Found {len(open_orders)} open orders")
                
                optionCount = 5
                for option in range(1, optionCount):
                    relevant_rows = [row for row in open_orders if row.get('Order_Id') == option]
                    if relevant_rows:
                        logger.info(f"Processing {len(relevant_rows)} rows for Order_Id {option}")
                        self.process_selected_option(option, relevant_rows)
                
                time.sleep(1)
            
            except KeyboardInterrupt:
                logger.info("Program terminated by user")
                break
            
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                time.sleep(5)