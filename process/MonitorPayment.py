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

# Import Singleton utilities for logging and MySQL connection
import mysql
from utils.logger import SingletonLogger
from utils.connectionSQL import MySQLConnectionSingleton
from datetime import datetime, timedelta, time
# Import custom exceptions for error handling
from utils.custom_exceptions.customize_exceptions import DatabaseConnectionError

# Initialize the logger using SingletonLogger
SingletonLogger.configure()
logger = SingletonLogger.get_logger("appLogger")

class MonitorPayment:
    
    def __init__(self):
        """Initialize the MonitorPayment class."""
        logger.info("MonitorPayment class initialized")
        # Set default monitoring interval (e.g., monitor every 24 hours)
        self.monitoring_interval_hours = 24

    def get_request_progress_data(self, order_id=2):
        """
        Retrieve all rows from request_progress_log where Order_Id = 2.
        
        Args:
            order_id (int): The order ID to filter by (default: 2)
            
        Returns:
            list: List of dictionaries containing the request progress data
        """
        try:
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for fetching request progress data.")
                
                cursor = connection.cursor(dictionary=True)
                try:
                    query = """
                        SELECT * FROM request_progress_log 
                        WHERE Order_Id = %s AND Request_Status = 'Open'
                    """
                    cursor.execute(query, (order_id,))
                    results = cursor.fetchall()
                    
                    logger.info(f"Fetched {len(results)} open orders with Order_Id={order_id}")
                    return results
                
                finally:
                    cursor.close()
        
        except Exception as e:
            logger.error(f"Error fetching request progress data: {str(e)}")
            return []

    def get_request_details(self, request_id):
        """
        Retrieve all parameters (para_1 to para_10) from request_log_details.
        
        Args:
            request_id (int): The request ID to fetch details for
            
        Returns:
            dict: Dictionary containing all parameters if found, None otherwise
        """
        try:
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for fetching request details.")
                
                cursor = connection.cursor(dictionary=True)
                try:
                    query = """
                        SELECT 
                            para_1, para_2, para_3, para_4, para_5,
                            para_6, para_7, para_8, para_9, para_10
                        FROM request_log_details 
                        WHERE Request_Id = %s
                    """
                    cursor.execute(query, (request_id,))
                    result = cursor.fetchone()
                    
                    if not result:
                        logger.warning(f"No details found in request_log_details for Request_Id={request_id}")
                        return None
                    
                    logger.debug(f"Retrieved request details for Request_Id={request_id}")
                    return result
                
                finally:
                    cursor.close()
        
        except Exception as e:
            logger.error(f"Error fetching request details for Request_Id={request_id}: {str(e)}")
            return None

    def create_process_monitor_log(self, request_data):
        """
        Create entry in process_monitor_log table.
        
        Args:
            request_data (dict): Data from request_progress_log
            
        Returns:
            int: The generated Monitor_Id if successful, None otherwise
        """
        try:
            # Calculate monitoring dates
            now = datetime.now()
            next_monitor_dtm = now + timedelta(hours=self.monitoring_interval_hours)
            
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for creating monitor log.")
                
                cursor = connection.cursor(dictionary=True)
                try:
                    query = """
                        INSERT INTO process_monitor_log (
                            case_id, Request_Id, last_monitored_dtm, next_monitor_dtm,
                            Order_Id, account_num, Expire_Dtm, monitor_status,
                            monitor_status_dtm, monitor_status_description
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(query, (
                        request_data.get('case_id'),
                        request_data.get('Request_Id'),
                        now,  # last_monitored_dtm
                        next_monitor_dtm,
                        request_data.get('Order_Id'),
                        request_data.get('account_num'),
                        None,  # Expire_Dtm (can be set later)
                        'Open',  # monitor_status
                        now,  # monitor_status_dtm
                        'Initial monitoring setup'  # monitor_status_description
                    ))
                    connection.commit()
                    
                    if cursor.rowcount == 1:
                        monitor_id = cursor.lastrowid
                        logger.info(f"Created process_monitor_log record with Monitor_Id={monitor_id}")
                        return monitor_id
                    else:
                        logger.warning("Failed to create process_monitor_log record")
                        return None
                
                finally:
                    cursor.close()
        
        except Exception as e:
            logger.error(f"Error creating process_monitor_log record: {str(e)}")
            return None

    def create_process_monitor_progress_log(self, monitor_id, request_data):
        """
        Create entry in process_monitor_progress_log table.
        
        Args:
            monitor_id (int): The Monitor_Id from process_monitor_log
            request_data (dict): Data from request_progress_log
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            now = datetime.now()
            
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for creating monitor progress log.")
                
                cursor = connection.cursor(dictionary=True)
                try:
                    # First get the record we just created in process_monitor_log
                    get_query = """
                        SELECT * FROM process_monitor_log 
                        WHERE Monitor_Id = %s
                    """
                    cursor.execute(get_query, (monitor_id,))
                    monitor_log_data = cursor.fetchone()
                    
                    if not monitor_log_data:
                        logger.warning(f"No record found in process_monitor_log with Monitor_Id={monitor_id}")
                        return False
                    
                    # Insert into progress log
                    insert_query = """
                        INSERT INTO process_monitor_progress_log (
                            Monitor_Id, created_dtm, case_id, Request_Id,
                            last_monitored_dtm, next_monitor_dtm, Order_Id,
                            account_num, Expire_Dtm, monitor_status,
                            monitor_status_dtm, monitor_status_description
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        monitor_id,
                        now,
                        monitor_log_data['case_id'],
                        monitor_log_data['Request_Id'],
                        monitor_log_data['last_monitored_dtm'],
                        monitor_log_data['next_monitor_dtm'],
                        monitor_log_data['Order_Id'],
                        monitor_log_data['account_num'],
                        monitor_log_data['Expire_Dtm'],
                        monitor_log_data['monitor_status'],
                        monitor_log_data['monitor_status_dtm'],
                        monitor_log_data['monitor_status_description']
                    ))
                    connection.commit()
                    
                    if cursor.rowcount == 1:
                        logger.info(f"Created process_monitor_progress_log record for Monitor_Id={monitor_id}")
                        return True
                    else:
                        logger.warning("Failed to create process_monitor_progress_log record")
                        return False
                
                finally:
                    cursor.close()
        
        except Exception as e:
            logger.error(f"Error creating process_monitor_progress_log record: {str(e)}")
            return False

    def create_process_monitor_details(self, monitor_id, request_id):
        """
        Create entry in process_monitor_details table.
        
        Args:
            monitor_id (int): The Monitor_Id from process_monitor_log
            request_id (int): The original Request_Id to get details for
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            details = self.get_request_details(request_id)
            if not details:
                return False
            
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for creating monitor details.")
                
                cursor = connection.cursor(dictionary=True)
                try:
                    query = """
                        INSERT INTO process_monitor_details (
                            Monitor_Id, para_1, para_2, para_3, para_4, para_5,
                            para_6, para_7, para_8, para_9, para_10
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(query, (
                        monitor_id,
                        details.get('para_1'),
                        details.get('para_2'),
                        details.get('para_3'),
                        details.get('para_4'),
                        details.get('para_5'),
                        details.get('para_6'),
                        details.get('para_7'),
                        details.get('para_8'),
                        details.get('para_9'),
                        details.get('para_10')
                    ))
                    connection.commit()
                    
                    if cursor.rowcount == 1:
                        logger.info(f"Created process_monitor_details record for Monitor_Id={monitor_id}")
                        return True
                    else:
                        logger.warning("Failed to create process_monitor_details record")
                        return False
                
                finally:
                    cursor.close()
        
        except Exception as e:
            logger.error(f"Error creating process_monitor_details record: {str(e)}")
            return False

    def update_request_progress_status(self, request_id, account_num, status="Completed"):
        """
        Update the request_progress_log and request_log status.
        
        Args:
            request_id (int): The request ID to update
            account_num (str): The associated account number
            status (str): The status to set (default: "Completed")
            
        Returns:
            bool: True if both updates are successful, False otherwise
        """
        try:
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    error_msg = "Failed to connect to MySQL for updating request status"
                    logger.error(f"Database connection error: {error_msg}")
                    raise DatabaseConnectionError(error_msg)
                
                cursor = connection.cursor(dictionary=True)
                try:
                    # Update request_progress_log
                    progress_query = """
                        UPDATE request_progress_log
                        SET Request_Status = %s, 
                            Request_Status_Dtm = NOW(),
                            Request_Status_Description = 'Monitoring setup completed'
                        WHERE Request_Id = %s AND account_num = %s AND Request_Status = 'Open'
                    """
                    cursor.execute(progress_query, (status, request_id, account_num))
                    
                    if cursor.rowcount != 1:
                        error_msg = (
                            f"No rows updated in request_progress_log for Request_Id={request_id}, "
                            f"account_num={account_num}. Ensure row exists with Request_Status='Open'."
                        )
                        logger.warning(error_msg)
                        connection.rollback()
                        return False
                    
                    # Update request_log
                    request_log_query = """
                        UPDATE request_log
                        SET Request_Status = %s, 
                            Request_Status_Dtm = NOW(),
                            Request_Status_Description = 'Monitoring setup completed'
                        WHERE Request_Id = %s AND account_num = %s AND Request_Status = 'Open'
                    """
                    cursor.execute(request_log_query, (status, request_id, account_num))
                    
                    if cursor.rowcount != 1:
                        error_msg = (
                            f"No rows updated in request_log for Request_Id={request_id}, "
                            f"account_num={account_num}. Ensure row exists with Request_Status='Open'."
                        )
                        logger.warning(error_msg)
                        connection.rollback()
                        return False
                    
                    connection.commit()
                    logger.info(
                        f"Successfully updated request_progress_log and request_log to {status} "
                        f"for Request_Id={request_id}, account_num={account_num}"
                    )
                    return True
                
                except mysql.connector.Error as db_err:
                    error_msg = (
                        f"Database error during update for Request_Id={request_id}, "
                        f"account_num={account_num}: {str(db_err)} (errno: {db_err.errno})"
                    )
                    logger.error(error_msg)
                    connection.rollback()
                    return False
                
                finally:
                    cursor.close()
        
        except DatabaseConnectionError as dce:
            logger.error(f"Connection error: {str(dce)}")
            return False
        
        except Exception as e:
            error_msg = (
                f"Unexpected error updating request_progress_log or request_log for "
                f"Request_Id={request_id}, account_num={account_num}: {str(e)}"
            )
            logger.error(error_msg)
            return False

    def process_monitoring_request(self, request_data):
        """
        Process a single monitoring request through all tables.
        
        Args:
            request_data (dict): Data from request_progress_log
            
        Returns:
            bool: True if all steps completed successfully, False otherwise
        """
        request_id = request_data.get('Request_Id')
        account_num = request_data.get('account_num')
        
        if not request_id or not account_num:
            logger.warning("Skipping request - missing Request_Id or account_num")
            return False
        
        logger.info(f"Processing monitoring request {request_id} for account {account_num}")
        
        # Step 1: Create process_monitor_log entry
        monitor_id = self.create_process_monitor_log(request_data)
        if not monitor_id:
            return False
        
        # Step 2: Create process_monitor_progress_log entry
        progress_success = self.create_process_monitor_progress_log(monitor_id, request_data)
        if not progress_success:
            return False
        
        # Step 3: Create process_monitor_details entry
        details_success = self.create_process_monitor_details(monitor_id, request_id)
        if not details_success:
            return False
        
        # Step 4: Update request_progress_log status
        status_success = self.update_request_progress_status(request_id, account_num)
        
        return status_success

    def process_all_monitoring_requests(self):
        """
        Main method to process all open monitoring requests (Order_Id = 2).
        
        Returns:
            tuple: (success_count, error_count)
        """
        success_count = 0
        error_count = 0
        
        # Get all open monitoring requests
        requests = self.get_request_progress_data(order_id=2)
        
        if not requests:
            logger.info("No open monitoring requests found")
            return (0, 0)
        
        logger.info(f"Found {len(requests)} monitoring requests to process")
        
        # Process each request
        for request in requests:
            if self.process_monitoring_request(request):
                success_count += 1
            else:
                error_count += 1
            time.sleep(0.1)  # Small delay to avoid database contention
        
        logger.info(f"Completed processing: {success_count} successful, {error_count} failed")
        return (success_count, error_count)

    def cancel_monitoring_request(self, request_data):
        """
        Process a single cancellation request for Order_Id = 3 through all tables.
        
        Args:
            request_data (dict): Data from request_progress_log
            
        Returns:
            bool: True if all steps completed successfully, False otherwise
        """
        request_id = request_data.get('Request_Id')
        account_num = request_data.get('account_num')
        
        if not request_id or not account_num:
            logger.warning("Skipping cancellation request - missing Request_Id or account_num")
            return False
        
        logger.info(f"Processing cancellation request {request_id} for account {account_num}")
        
        # Step 1: Create process_monitor_log entry
        monitor_id = self.create_process_monitor_log(request_data)
        if not monitor_id:
            return False
        
        # Step 2: Create process_monitor_progress_log entry
        progress_success = self.create_process_monitor_progress_log(monitor_id, request_data)
        if not progress_success:
            return False
        
        # Step 3: Create process_monitor_details entry
        details_success = self.create_process_monitor_details(monitor_id, request_id)
        if not details_success:
            return False
        
        # Step 4: Update request_progress_log and request_log status
        status_success = self.update_request_progress_status(request_id, account_num, status="Cancelled")
        
        return status_success