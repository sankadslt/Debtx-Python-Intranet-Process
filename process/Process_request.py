'''
Process_Request.py file is as follows:
    Purpose: This script handles database connections and case creation, routing orders to specific processing classes based on Order_Id.
    Created Date: 
    Created By: Dulhan Perera
    Modified By: Dulhan Perera, Pasan Bathiya
    Version: Python 3.9
    Dependencies: json, datetime, decimal, requests, mysql.connector, utils.logger, utils.connectionSQL, utils.connectAPI, utils.custom_exceptions
    Notes: Added input validation for account_num and Request_Id to ensure data integrity before processing.
'''

# Import time module for delays and rate limiting
import time
# Import mysql.connector for MySQL database interactions
import mysql.connector
# Import processing classes for different Order_Id types
from process.MonitorPayment import MonitorPayment
from process.CancelMonitoring import CancelMonitoring
from process.CloseMonitor import CloseMonitor
from process.CreateIncident import CreateIncident
# Import Singleton utilities for logging and MySQL connection
from utils.logger import SingletonLogger
from utils.connectionSQL import DBConnectionSingleton
# Import custom exceptions for error handling
from utils.custom_exceptions.customize_exceptions import DatabaseConnectionError
from datetime import datetime, timedelta

# Initialize the logger using SingletonLogger
logger = SingletonLogger.get_logger("appLogger")

class Process_request:
    
    def __init__(self):
        """
        Constructor for initializing the class with a MySQL connection.
        """
        logger.info("MySQL connection initialization attempted")

    def _validate_inputs(self, account_num, request_id, context=""):
        """
        Validate account_num and request_id to ensure they are valid before processing.
        
        Args:
            account_num: The account number to validate (must be a non-empty string)
            request_id: The request ID to validate (must be a positive integer)
            context (str): Context for logging (e.g., method name)
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            # Check account_num: must be a non-empty string
            if account_num is None or not isinstance(account_num, str) or not account_num.strip():
                logger.error(f"Invalid account_num in {context}: {account_num}")
                return False
                
            # Check request_id: must be a positive integer
            if request_id is None:
                logger.error(f"Invalid request_id in {context}: {request_id}")
                return False
                
            try:
                request_id = int(request_id)
                if request_id <= 0:
                    logger.error(f"Request_id must be positive in {context}: {request_id}")
                    return False
            except (TypeError, ValueError) as e:
                logger.error(f"Invalid request_id in {context}: {request_id}, error: {str(e)}")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Unexpected validation error in {context}: {str(e)}")
            return False

    def process_case(self, account_number, incident_id, request_id):
        """
        Processes an incident case for a given account by invoking CreateIncident.
        
        Args:
            account_number (str): The account number
            incident_id: The incident ID
            request_id: The request ID
            
        Returns:
            bool: True if processing is successful, False otherwise
        """
        context = f"process_case for account: {account_number}, request: {request_id}"
        # Validate inputs before processing
        if not self._validate_inputs(account_number, request_id, context):
            return False
            
        logger.info(f"Processing case for account: {account_number}, incident: {incident_id}, request: {request_id}")
        
        try:
            # Create and process incident
            process = CreateIncident(account_num=account_number, incident_id=incident_id)
            success = process.process_incident()
            status = "Completed" if success else "Error"
            
            # Update request_progress_log and request_log statuses
            with DBConnectionSingleton() as db_connection:
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
        Processes a single MySQL row representing an incident case for Order_Id = 1.
        
        Args:
            row (dict): Row data from request_progress_log
            
        Returns:
            bool: True if processing is successful, False otherwise
        """
        request_id = row.get('Request_Id')
        account_number = row.get('account_num')
        context = f"process_single_document for Request_Id={request_id}"
        
        # Validate inputs before processing
        if not self._validate_inputs(account_number, request_id, context):
            return False
        
        try:
            # Fetch incident_id from request_log_details
            with DBConnectionSingleton() as db_connection:
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
                    cursor.execute(query, (request_id,))
                    result = cursor.fetchone()
                    
                    if not result or not result.get('incident_id'):
                        logger.warning(f"No incident_id found in request_log_details for Request_Id={request_id}")
                        return False
                    
                    incident_id = result['incident_id']
                    return self.process_case(account_number, incident_id, request_id)
                
                finally:
                    cursor.close()
        
        except Exception as e:
            logger.error(f"Error processing row for account {account_number}, request {request_id}: {str(e)}")
            return False

    def customer_details_for_case_registration(self, row):
        """
        Processes a single row with Order_Id equal to 1 (case registration).
        
        Args:
            row (dict): Row data from request_progress_log
            
        Returns:
            tuple: (processed_count, error_count)
        """
        request_id = row.get('Request_Id')
        logger.info(f"Processing case registration for Request_Id={request_id}")
        processed_count = 0
        error_count = 0
        
        # Process the document and update counts
        if self.process_single_document(row):
            processed_count += 1
        else:
            error_count += 1
        
        logger.info(f"Processed {processed_count} document(s), {error_count} errors")
        return processed_count, error_count

    def monitor_payment(self, row):
        """
        Process a single row with Order_Id equal to 2 (Payment Monitoring).
        
        Args:
            row (dict): Row data from request_progress_log
            
        Returns:
            tuple: (processed_count, error_count)
        """
        request_id = row.get('Request_Id')
        account_num = row.get('account_num')
        context = f"monitor_payment for Request_Id={request_id}"
        
        # Validate inputs before processing
        if not self._validate_inputs(account_num, request_id, context):
            return 0, 1
        
        logger.info(f"Processing payment monitoring for Request_Id={request_id}")
        processed_count = 0
        error_count = 0
        
        # Process monitoring request
        monitor_payment = MonitorPayment()
        
        if monitor_payment.process_monitoring_request(row):
            processed_count += 1
        else:
            error_count += 1
        
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def monitor_payment_cancel(self, row):
        """
        Process a single row with Order_Id equal to 3 (Monitor Payment Cancel).
        
        Args:
            row (dict): Row data from request_progress_log
            
        Returns:
            tuple: (processed_count, error_count)
        """
        request_id = row.get('Request_Id')
        account_num = row.get('account_num')
        context = f"monitor_payment_cancel for Request_Id={request_id}"
        
        # Validate inputs before processing
        if not self._validate_inputs(account_num, request_id, context):
            return 0, 1
        
        logger.info(f"Processing payment cancellation for Request_Id={request_id}")
        processed_count = 0
        error_count = 0
        
        # Process cancellation request
        cancel_monitoring = CancelMonitoring()
        
        if cancel_monitoring.cancel_monitoring_request(row):
            processed_count += 1
        else:
            error_count += 1
        
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def close_monitor_if_no_transaction(self, row):
        """
        Process a single row with Order_Id equal to 4 (Close Monitor If No Transaction).
        
        Args:
            row (dict): Row data from request_progress_log
            
        Returns:
            tuple: (processed_count, error_count)
        """
        request_id = row.get('Request_Id')
        account_num = row.get('account_num')
        context = f"close_monitor_if_no_transaction for Request_Id={request_id}"
        
        # Validate inputs before processing
        if not self._validate_inputs(account_num, request_id, context):
            return 0, 1
        
        logger.info(f"Processing close monitor for Request_Id={request_id}")
        processed_count = 0
        error_count = 0
        
        # Process close monitor request
        close_monitor = CloseMonitor()
        
        if close_monitor.close_monitor_if_no_transaction(row):
            processed_count += 1
        else:
            error_count += 1
        
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def get_open_orders(self):
        """
        Fetches all rows from the request_progress_log table where the Request_Status is 'Open'.
        
        Returns:
            list: List of dictionaries containing open order data
        """
        try:
            with DBConnectionSingleton() as db_connection:
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

    def run_process(self):
        """
        Main method to continuously process open orders in a loop.
        """
        logger.info("Starting Order Processor")
        
        while True:
            try:
                # Fetch all open orders
                open_orders = self.get_open_orders()
                
                if not open_orders:
                    logger.info("No open orders found. Waiting...")
                    time.sleep(5)
                    continue
                
                logger.info(f"Fetched {len(open_orders)} open orders")
                
                # Process each order based on Order_Id
                for order in open_orders:
                    order_id = order.get('Order_Id')
                    logger.info(f"Processing order with Request_Id={order.get('Request_Id')} and Order_Id={order_id}")
                    
                    # Route to appropriate processing method
                    match order_id:
                        case 1:
                            self.customer_details_for_case_registration(order)
                        case 2:
                            self.monitor_payment(order)
                        case 3:
                            self.monitor_payment_cancel(order)
                        case 4:
                            self.close_monitor_if_no_transaction(order)
                        case _:
                            logger.warning(f"Invalid Order_Id {order_id} for Request_Id={order.get('Request_Id')}")
                    
                    time.sleep(1)  # Rate limiting after processing each order
                
                time.sleep(5)  # Wait before fetching the next batch
            
            except KeyboardInterrupt:
                logger.info("Program terminated by user")
                break
            
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                time.sleep(5)