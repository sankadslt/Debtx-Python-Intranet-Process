'''
CancelMonitoring.py file is as follows:
    Purpose: This script handles database connections and cancellation of payment monitoring for Order_Id = 3.
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

class CancelMonitoring:
    
    def __init__(self):
        """
        Initialize the CancelMonitoring class with a reference to MonitorPayment for shared methods.
        """
        logger.info("CancelMonitoring class initialized")
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
        case_id = request_data.get('case_id')
        context = f"cancel_monitoring_request for Request_Id={request_id}"
        
        # Validate inputs before processing
        if not self._validate_inputs(account_num, request_id, case_id, context):
            return False
        
        logger.info(f"Processing cancellation request {request_id} for account {account_num}, case {case_id}")
        
        try:
            with DBConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for cancellation process.")
                
                # Ensure connection is alive
                connection.ping(reconnect=True)
                
                try:
                    # Step 1: Get request_log_details
                    request_details = self.monitor_payment.get_request_details(request_id, connection)
                    if not request_details:
                        logger.warning(f"No details found in request_log_details for Request_Id={request_id}")
                        return False
                    
                    # Step 2: Check if record exists in process_monitor_progress_log
                    cursor = connection.cursor(dictionary=True)
                    check_query = """
                        SELECT Monitor_Id FROM process_monitor_progress_log 
                        WHERE case_id = %s AND account_num = %s
                    """
                    cursor.execute(check_query, (case_id, account_num))
                    existing_record = cursor.fetchone()
                    cursor.close()
                    
                    if not existing_record:
                        logger.warning(f"No record found in process_monitor_progress_log for case_id={case_id}, account_num={account_num}")
                        return False
                    
                    monitor_id = existing_record['Monitor_Id']
                    
                    # Step 3: Update process_monitor_progress_log
                    now = datetime.now()
                    next_monitor_dtm = now + timedelta(hours=self.monitoring_interval_hours)
                    cursor = connection.cursor(dictionary=True)
                    update_progress_query = """
                        UPDATE process_monitor_progress_log
                        SET Request_Id = %s,
                            last_monitored_dtm = %s,
                            next_monitor_dtm = %s,
                            Order_Id = %s,
                            monitor_status = 'Cancelled',
                            monitor_status_dtm = %s,
                            monitor_status_description = 'Cancellation processed'
                        WHERE Monitor_Id = %s AND case_id = %s AND account_num = %s
                    """
                    cursor.execute(update_progress_query, (
                        request_id,
                        now,
                        next_monitor_dtm,
                        request_data.get('Order_Id'),
                        now,
                        monitor_id,
                        case_id,
                        account_num
                    ))
                    
                    if cursor.rowcount != 1:
                        logger.warning(f"Failed to update process_monitor_progress_log for Monitor_Id={monitor_id}")
                        connection.rollback()
                        cursor.close()
                        return False
                    cursor.close()
                    
                    # Step 4: Update process_monitor_log
                    cursor = connection.cursor(dictionary=True)
                    update_monitor_query = """
                        UPDATE process_monitor_log
                        SET Request_Id = %s,
                            last_monitored_dtm = %s,
                            next_monitor_dtm = %s,
                            Order_Id = %s,
                            monitor_status = 'Cancelled',
                            monitor_status_dtm = %s,
                            monitor_status_description = 'Cancellation processed'
                        WHERE Monitor_Id = %s AND case_id = %s AND account_num = %s
                    """
                    cursor.execute(update_monitor_query, (
                        request_id,
                        now,
                        next_monitor_dtm,
                        request_data.get('Order_Id'),
                        now,
                        monitor_id,
                        case_id,
                        account_num
                    ))
                    
                    if cursor.rowcount != 1:
                        logger.warning(f"Failed to update process_monitor_log for Monitor_Id={monitor_id}")
                        connection.rollback()
                        cursor.close()
                        return False
                    cursor.close()
                    
                    # Step 5: Update process_monitor_details
                    cursor = connection.cursor(dictionary=True)
                    update_details_query = """
                        UPDATE process_monitor_details
                        SET para_1 = %s,
                            para_2 = %s,
                            para_3 = NULL,
                            para_4 = NULL,
                            para_5 = NULL,
                            para_6 = NULL,
                            para_7 = NULL,
                            para_8 = NULL,
                            para_9 = NULL,
                            para_10 = NULL
                        WHERE Monitor_Id = %s
                    """
                    cursor.execute(update_details_query, (
                        request_details.get('para_1'),
                        request_details.get('para_2'),
                        monitor_id
                    ))
                    
                    if cursor.rowcount != 1:
                        logger.warning(f"Failed to update process_monitor_details for Monitor_Id={monitor_id}")
                        connection.rollback()
                        cursor.close()
                        return False
                    cursor.close()
                    
                    # Step 6: Update request_progress_log and request_log
                    status_success = self.monitor_payment.update_request_progress_status(request_id, account_num, status="Completed", connection=connection)
                    if not status_success:
                        connection.rollback()
                        return False
                    
                    connection.commit()
                    logger.info(f"Successfully processed cancellation for Request_Id={request_id}, account_num={account_num}")
                    return True
                
                except mysql.connector.Error as db_err:
                    logger.error(f"Database error during cancellation process: {str(db_err)}")
                    connection.rollback()
                    return False
                
                except Exception as e:
                    logger.error(f"Unexpected error in cancellation process for Request_Id={request_id}: {str(e)}")
                    connection.rollback()
                    return False
        
        except DatabaseConnectionError as dce:
            logger.error(f"Connection error: {str(dce)}")
            return False
        
        except Exception as e:
            logger.error(f"Unexpected error in cancellation process for Request_Id={request_id}: {str(e)}")
            return False