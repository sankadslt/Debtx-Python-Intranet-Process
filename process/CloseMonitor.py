'''
CloseMonitor.py file is as follows:
    Purpose: This script handles database connections and closing of monitors with no transactions for Order_Id = 4.
    Created Date: 
    Created By: Dulhan Perera
    Modified By: Dulhan Perera, Pasan Bathiya
    Version: Python 3.9
    Dependencies: json, datetime, decimal, requests, mysql.connector, utils.logger, utils.connectionSQL, utils.connectAPI, utils.custom_exceptions
    Notes: Added input validation for account_num, Request_Id, and case_id to ensure data integrity before processing.
'''

# Import required modules
import mysql
from utils.logger import SingletonLogger
from utils.connectionSQL import DBConnectionSingleton
from datetime import datetime, timedelta
# Import custom exceptions for error handling
from utils.custom_exceptions.customize_exceptions import DatabaseConnectionError
# Import shared methods from MonitorPayment
from process.MonitorPayment import MonitorPayment

# Initialize the logger using SingletonLogger
SingletonLogger.configure()
logger = SingletonLogger.get_logger("appLogger")

class CloseMonitor:
    
    def __init__(self):
        """
        Initialize the CloseMonitor class with a reference to MonitorPayment for shared methods.
        """
        logger.info("CloseMonitor class initialized")
        self.monitoring_interval_hours = 24
        self.monitor_payment = MonitorPayment()  # Instance to access shared methods

    def _validate_inputs(self, account_num, request_id, case_id, context=""):
        """
        Validate account_num, request_id, and case_id to ensure they are valid before processing.
        
        Args:
            account_num: The account number to validate (must be a non-empty string)
            request_id: The request ID to validate (must be a positive integer)
            case_id: The case ID to validate (must be a non-empty string)
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
                
            # Check case_id: must be a non-empty string
            if case_id is None or not isinstance(case_id, str) or not case_id.strip():
                logger.error(f"Invalid case_id in {context}: {case_id}")
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

    def close_monitor_if_no_transaction(self, request_data):
        """
        Process a single row with Order_Id equal to 4 (Close Monitor If No Transaction).
        
        Args:
            request_data (dict): Data from request_progress_log
            
        Returns:
            bool: True if all steps completed successfully, False otherwise
        """
        request_id = request_data.get('Request_Id')
        account_num = request_data.get('account_num')
        case_id = request_data.get('case_id')
        context = f"close_monitor_if_no_transaction for Request_Id={request_id}"
        
        # Validate inputs before processing
        if not self._validate_inputs(account_num, request_id, case_id, context):
            return False
            
        logger.info(f"Processing close monitor for Request_Id={request_id}, account_num={account_num}, case_id={case_id}")
        
        try:
            with DBConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for close monitor processing.")
                
                # Ensure connection is alive
                connection.ping(reconnect=True)
                
                try:
                    # Step 1: Create process_monitor_log entry
                    now = datetime.now()
                    next_monitor_dtm = now + timedelta(hours=self.monitoring_interval_hours)
                    
                    cursor = connection.cursor(dictionary=True)
                    monitor_query = """
                        INSERT INTO process_monitor_log (
                            case_id, Request_Id, last_monitored_dtm, next_monitor_dtm,
                            Order_Id, account_num, Expire_Dtm, monitor_status,
                            monitor_status_dtm, monitor_status_description
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(monitor_query, (
                        case_id,
                        request_id,
                        now,
                        next_monitor_dtm,
                        request_data.get('Order_Id'),
                        account_num,
                        None,
                        'Closed',
                        now,
                        'Monitor closed due to no transaction'
                    ))
                    connection.commit()
                    
                    if cursor.rowcount != 1:
                        logger.warning(f"Failed to create process_monitor_log for Request_Id={request_id}")
                        connection.rollback()
                        return False
                        
                    monitor_id = cursor.lastrowid
                    cursor.close()
                    
                    # Step 2: Create process_monitor_progress_log entry
                    cursor = connection.cursor(dictionary=True)
                    progress_query = """
                        INSERT INTO process_monitor_progress_log (
                            Monitor_Id, created_dtm, case_id, Request_Id,
                            last_monitored_dtm, next_monitor_dtm, Order_Id,
                            account_num, Expire_Dtm, monitor_status,
                            monitor_status_dtm, monitor_status_description
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(progress_query, (
                        monitor_id,
                        now,
                        case_id,
                        request_id,
                        now,
                        next_monitor_dtm,
                        request_data.get('Order_Id'),
                        account_num,
                        None,
                        'Closed',
                        now,
                        'Monitor closed due to no transaction'
                    ))
                    connection.commit()
                    
                    if cursor.rowcount != 1:
                        logger.warning(f"Failed to create process_monitor_progress_log for Monitor_Id={monitor_id}")
                        connection.rollback()
                        return False
                    cursor.close()
                    
                    # Step 3: Copy request_log_details to process_monitor_details
                    details_success = self.monitor_payment.create_process_monitor_details(monitor_id, request_id, connection)
                    if not details_success:
                        logger.warning(f"Failed to create process_monitor_details for Monitor_Id={monitor_id}")
                        connection.rollback()
                        return False
                    
                    # Step 4: Update request_progress_log and request_log
                    status_success = self.monitor_payment.update_request_progress_status(
                        request_id,
                        account_num,
                        status="Completed",
                        connection=connection
                    )
                    
                    if not status_success:
                        logger.warning(f"Failed to update request status for Request_Id={request_id}")
                        connection.rollback()
                        return False
                        
                    connection.commit()
                    logger.info(f"Successfully processed close monitor for Request_Id={request_id}")
                    return True
                    
                except mysql.connector.Error as db_err:
                    logger.error(f"Database error during close monitor processing: {str(db_err)}")
                    connection.rollback()
                    return False
                    
                except Exception as e:
                    logger.error(f"Unexpected error in close monitor processing for Request_Id={request_id}: {str(e)}")
                    connection.rollback()
                    return False
                    
        except DatabaseConnectionError as dce:
            logger.error(f"Connection error: {str(dce)}")
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error in close monitor processing for Request_Id={request_id}: {str(e)}")
            return False