'''
 jsonMapping.py file is as follows:
    Purpose: This script handles database connections and case creation.
    Created Date: 
    Created By: Dulhan Perera
    Modified By: Dulhan Perera, Pasan Bathiya
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
from process.CreateIncident import CreateIncident
# Import Singleton utilities for logging and MySQL connection
from utils.logger import SingletonLogger
from utils.connectionSQL import MySQLConnectionSingleton
# Import custom exceptions for error handling
from utils.custom_exceptions.customize_exceptions import DatabaseConnectionError
from datetime import datetime, timedelta

# Initialize the logger using SingletonLogger
SingletonLogger.configure()
logger = SingletonLogger.get_logger("appLogger")

class Process_request:
    
    def __init__(self):
        """
        Constructor for initializing the class with a MySQL connection.
        """
        logger.info("MySQL connection initialization attempted")

    def process_case(self, account_number, incident_id, request_id):
        """
        Processes an incident case for a given account.
        """
        logger.info(f"Processing case for account: {account_number}, incident: {incident_id}, request: {request_id}")
        
        try:
            process = CreateIncident(account_num=account_number, incident_id=incident_id)
            success = process.process_incident()
            status = "Completed" if success else "Error"
            
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for updating request_progress_log.")
                
                cursor = connection.cursor(dictionary=True)
                try:
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
        Processes a single MySQL row representing an incident case.
        """
        try:
            request_id = row.get('Request_Id')
            account_number = row.get('account_num')
            
            if not request_id or not account_number:
                logger.warning(f"Skipping row - missing required fields: Request_Id={request_id}, account_num={account_number}")
                return False
            
            try:
                request_id = int(request_id)
            except (TypeError, ValueError) as e:
                logger.error(f"Invalid Request_Id: {request_id}, error: {str(e)}")
                return False
            
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

    def customer_details_for_case_registration(self, rows):
        """
        Processes rows with Order_Id equal to 1.
        """
        processed_count = 0
        error_count = 0
        
        for row in rows:
            if row.get('Order_Id') == 1:
                if self.process_single_document(row):
                    processed_count += 1
                else:
                    error_count += 1
                time.sleep(1)
        
        logger.info(f"Successfully Processed {processed_count} document(s), {error_count} errors")
        return processed_count, error_count

    def monitor_payment(self, rows):
        """
        Process rows with Order_Id equal to 2 (Payment Monitoring).
        """
        logger.info("Processing option 2 - Monitor Payment")
        processed_count = 0
        error_count = 0
        
        monitor_payment = MonitorPayment()
        
        for row in rows:
            if row.get('Order_Id') == 2:
                if monitor_payment.process_monitoring_request(row):
                    processed_count += 1
                else:
                    error_count += 1
                time.sleep(1)
        
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def monitor_payment_cancel(self, rows):
        """
        Process rows with Order_Id equal to 3 (Monitor Payment Cancel).
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
                time.sleep(1)
        
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def close_monitor_if_no_transaction(self, rows):
        """
        Process rows with Order_Id equal to 4 (Close Monitor If No Transaction).
        
        Args:
            rows (list): A list of MySQL row dictionaries to process.
            
        Returns:
            tuple: (processed_count, error_count)
        """
        logger.info("Processing option 4 - Close Monitor If No Transaction")
        processed_count = 0
        error_count = 0
        monitoring_interval_hours = 24
        
        monitor_payment = MonitorPayment()
        
        for row in rows:
            if row.get('Order_Id') != 4:
                continue
                
            request_id = row.get('Request_Id')
            account_num = row.get('account_num')
            case_id = row.get('case_id')
            
            if not request_id or not account_num or not case_id:
                logger.warning(f"Skipping row - missing required fields: Request_Id={request_id}, account_num={account_num}, case_id={case_id}")
                error_count += 1
                continue
                
            logger.info(f"Processing close monitor for Request_Id={request_id}, account_num={account_num}, case_id={case_id}")
            
            try:
                with MySQLConnectionSingleton() as db_connection:
                    connection = db_connection.get_connection()
                    if not connection:
                        raise DatabaseConnectionError("Failed to connect to MySQL for close monitor processing.")
                    
                    connection.ping(reconnect=True)
                    
                    try:
                        # Step 1: Create process_monitor_log entry
                        now = datetime.now()
                        next_monitor_dtm = now + timedelta(hours=monitoring_interval_hours)
                        
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
                            row.get('Order_Id'),
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
                            error_count += 1
                            continue
                            
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
                            row.get('Order_Id'),
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
                            error_count += 1
                            continue
                        cursor.close()
                        
                        # Step 3: Copy request_log_details to process_monitor_details
                        details_success = monitor_payment.create_process_monitor_details(monitor_id, request_id, connection)
                        if not details_success:
                            logger.warning(f"Failed to create process_monitor_details for Monitor_Id={monitor_id}")
                            connection.rollback()
                            error_count += 1
                            continue
                        
                        # Step 4: Update request_progress_log and request_log
                        status_success = monitor_payment.update_request_progress_status(
                            request_id,
                            account_num,
                            status="Completed",
                            connection=connection
                        )
                        
                        if not status_success:
                            logger.warning(f"Failed to update request status for Request_Id={request_id}")
                            connection.rollback()
                            error_count += 1
                            continue
                            
                        connection.commit()
                        logger.info(f"Successfully processed close monitor for Request_Id={request_id}")
                        processed_count += 1
                        
                    except mysql.connector.Error as db_err:
                        logger.error(f"Database error during close monitor processing: {str(db_err)}")
                        connection.rollback()
                        error_count += 1
                        continue
                        
                    except Exception as e:
                        logger.error(f"Unexpected error in close monitor processing for Request_Id={request_id}: {str(e)}")
                        connection.rollback()
                        error_count += 1
                        continue
                        
            except DatabaseConnectionError as dce:
                logger.error(f"Connection error: {str(dce)}")
                error_count += 1
                continue
                
            time.sleep(1)  # Rate limiting
        
        logger.info(f"Processed {processed_count} documents, {error_count} errors")
        return processed_count, error_count

    def get_open_orders(self):
        """
        Fetches all rows from the request_progress_log table where the Request_Status is 'Open'.
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
        """
        match option:
            case 1:
                self.customer_details_for_case_registration(rows)
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