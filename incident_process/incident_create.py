#region -- Incident Creation Module
'''

 incident_create.py file is as follows:

    Purpose: This script handles database connections and collection initialization.
    Created Date: 2025-01-08
    Created By:  T.S.Balasooriya (tharindutsb@gmail.com) , Pasan(pasanbathiya246@gmail.com),Amupama(anupamamaheepala999@gmail.com)
    Last Modified Date: 2024-01-19
    Modified By: T.S.Balasooriya (tharindutsb@gmail.com), Pasan(pasanbathiya246@gmail.com),Amupama(anupamamaheepala999@gmail.com)     
    Version: Python 3.9
    Dependencies: configparser, pymongo, os, utils.loggers, utils.Custom_Exceptions
    Notes:
'''
#endregion



import pymysql  # Library for interacting with MySQL databases
from datetime import datetime, date, timedelta,timezone,time # Modules for working with date and time
from decimal import Decimal  # Module for handling precise decimal arithmetic
import json  # Module for parsing and handling JSON data
import requests  # Library for making HTTP requests


# Import a function to establish a MySQL database connection
from utils.database.connectSQL import get_mysql_connection

# Import a function to configure and retrieve a logger for logging messages
from utils.logger.logger import get_logger

# Import a function to read API config
from utils.api.connectAPI import read_api_config
# Import custom exceptions for error handling
from utils.custom_exceptions.customize_exceptions import APIConfigError, IncidentCreationError, DatabaseConnectionError, DataProcessingError




logger = get_logger("incident_logger")

class create_incident:

    def __init__(self, account_num, incident_id):
        """
        Constructor for the create_incident class.
        
        Args:
            account_num (str): The account number associated with the incident.
            incident_id (int): The unique identifier for the incident.

        Raises:
            ValueError: If either account_num or incident_id fails conversion.
        """
        try: # either one
            if account_num is None:
                raise ValueError("account_num cannot be None")
            self.account_num = str(account_num)

            if incident_id is None:
                raise ValueError("incident_id cannot be None")
            self.incident_id = int(incident_id)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid input: {e}")
        
        self.mongo_data = self.initialize_mongo_doc()
        self.last_execution_time = None
        self.current_sequence = 0

    def initialize_mongo_doc(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return {
            "Doc_Version": 1.0,
            "Incident_Id": self.incident_id,
            "Account_Num": self.account_num,
            "Customer_Ref": "",
            "Arrears": 0,
            "arrears_band": "",
            "Bss_Arrears_Amount": 0,
            "Last_Bss_Reading_Date": "1900-01-01T00:00:00.000Z",
            "Created_By": "drs_admin",
            "Created_Dtm": now,
            "Incident_Status": "",
            "Incident_Status_Dtm": now,
            "Status_Description": "",
            "File_Name_Dump": "",
            "Batch_Id": "",
            "Batch_Id_Tag_Dtm": now,
            "External_Data_Update_On": now,
            "Filtered_Reason": "",
            "Export_On": now,
            "File_Name_Rejected": "",
            "Rejected_Reason": "",
            "Incident_Forwarded_By": "",
            "Incident_Forwarded_On": now,
            "Accounts_Details":[
                {
                "Id":0,
                "DRCMood":"",
                "Account_Num":self.account_num,
                "Account_Status":"",
                "OutstandingBalance":0,
                }
            ]
            ,
            "Contact_Details": [],
            "Product_Details": [],
            "Customer_Details": {},
            "Account_Details": {},
            "Last_Actions": [
                {
                    "Billed_Seq": "",
                    "Billed_Created": "",
                    "Payment_Seq": 0,
                    "Payment_Created": "",
                    "Payment_Money": 0,
                    "Billed_Amount": 0
                }
            ],
            "Marketing_Details": [
                {
                    "ACCOUNT_MANAGER": "",
                    "CONSUMER_MARKET": "",
                    "Informed_To": "",
                    "Informed_On": "1900-01-01T00:00:00.100Z"
                }
            ],
            "Action": "",
            "Validity_period": 0,
            "Remark": "",
            "updatedAt": now,
            "Rejected_By": "",
            "Rejected_Dtm": now,
            "Arrears_Band": "",
            "Source_Type": ""
        }

    def read_customer_details(self,time_period_start, time_period_end):
        """
        Retrieves and processes customer account data from MySQL, transforming it into MongoDB document structure.

        This method performs three key operations:
        1. Fetches all customer records for the specified account number
        2. Transforms relational MySQL data into document-oriented MongoDB structure
        3. Handles data quality issues and type conversions

        Data Flow:
        MySQL (debt_cust_detail table) → Python Dict → mongo_data structure

        Returns:
            str: 
                - "success" if:
                    * Data was successfully retrieved AND
                    * Customer_Details was populated AND
                    * No unhandled exceptions occurred
                - "error" if:
                    * MySQL connection failed OR
                    * SQL execution failed OR
                    * Any exception occurred during processing

        Database Requirements:
            Requires MySQL table 'debt_cust_detail' with columns:
            - ACCOUNT_NUM (account identifier)
            - ASSET_ID (product identifier)
            - CONTACT_PERSON, NIC, ASSET_ADDRESS (customer info)
            - ACCOUNT_STATUS_BSS, EMAIL (account info)
            - LAST_PAYMENT_DAT, LAST_PAYMENT_MNY (payment info)
            - PROMOTION_INTEG_ID, PRODUCT_NAME (product info)
        """
        def format_datetime_z(dt):
            if not dt:
                return "1900-01-01T00:00:00.000Z"
            if isinstance(dt, datetime):
                return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            if isinstance(dt, date):
                return datetime.combine(dt, time.min).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            try:
                # Try parsing string date
                parsed = datetime.strptime(str(dt), "%Y-%m-%d")
                return parsed.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            except Exception:
                return "1900-01-01T00:00:00.000Z"

        mysql_conn = None
        cursor = None
        try:
            logger.info(f"Reading customer details for account number: {self.account_num} within {time_period_start} to {time_period_end}")
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                raise DatabaseConnectionError("Failed to connect to MySQL for reading customer details.")
            
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(
                f"""
                SELECT * FROM debt_cust_detail 
                WHERE ACCOUNT_NUM = '{self.account_num}' 
                AND LOAD_DATE BETWEEN '{time_period_start}' AND '{time_period_end}'
                """
            )
            rows = cursor.fetchall()

            # Track seen product IDs to avoid duplicates
            seen_products = set()

            for row in rows:
                # Only process customer/account details once
                if not self.mongo_data["Customer_Details"]:
                    # Handle contact details - maintaining your exact structure
                    if row.get("TECNICAL_CONTACT_EMAIL"):
                        contact_details_element = {
                            "Contact_Type": "email",
                            "Contact": row["TECNICAL_CONTACT_EMAIL"] if "@" in row["TECNICAL_CONTACT_EMAIL"] else "",
                            "Create_Dtm": format_datetime_z(row.get("LOAD_DATE")),
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    if row.get("MOBILE_CONTACT"):
                        contact_details_element = {
                            "Contact_Type": "mobile",
                            "Contact": row["MOBILE_CONTACT"],
                            "Create_Dtm": format_datetime_z(row.get("LOAD_DATE")),
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    if row.get("WORK_CONTACT"):
                        contact_details_element = {
                            "Contact_Type": "fix",
                            "Contact": row["WORK_CONTACT"],
                            "Create_Dtm": format_datetime_z(row.get("LOAD_DATE")),
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    # Customer details
                    self.mongo_data["Customer_Details"] = {
                        "Customer_Name": row.get("CONTACT_PERSON", ""),
                        "Company_Name": "",
                        "Company_Registry_Number": "",
                        "Full_Address": row.get("ASSET_ADDRESS", ""),
                        "Zip_Code": row.get("ZIP_CODE", ""),
                        "Customer_Type_Name": "",
                        "Nic": str(row.get("NIC", "")), 
                        "Customer_Type_Id": int(row.get("CUSTOMER_TYPE_ID", "")),
                        "Customer_Type": row.get("CUSTOMER_TYPE", ""),
                        "customer_ref": row.get("CUSTOMER_REF", ""),
                        
                    }

                    # Account details
                    self.mongo_data["Account_Details"] = {
                        "Account_Status": row.get("ACCOUNT_STATUS_BSS", ""),
                        "Acc_Effective_Dtm": format_datetime_z(row.get("ACCOUNT_EFFECTIVE_DTM_BSS")),
                        "Acc_Activate_Date": "1900-01-01T00:00:00.000Z",
                        "Credit_Class_Id": int(row.get("CREDIT_CLASS_ID", "")),
                        "Credit_Class_Name": row.get("CREDIT_CLASS_NAME", ""),
                        "Billing_Centre": row.get("BILLING_CENTER_NAME", ""),
                        "Customer_Segment": row.get("CUSTOMER_SEGMENT_ID", ""),
                        "Mobile_Contact_Tel": "",
                        "Daytime_Contact_Tel": "",
                        "Email_Address": str(row.get("EMAIL", "")),  
                        "Last_Rated_Dtm": "1900-01-01T00:00:00.000Z"
                    }

                    # Last actions from customer table
                    if row.get("LAST_PAYMENT_DAT"):
                        self.mongo_data["Last_Actions"] = [
                            {
                                "Billed_Seq": int(row.get("LAST_BILL_SEQ", "")),
                                "Billed_Created": format_datetime_z(row.get("LAST_BILL_DTM")),
                                "Payment_Seq": 0,
                                "Payment_Created": format_datetime_z(row.get("LAST_PAYMENT_DAT")),
                                "Payment_Money": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else "0",
                                "Billed_Amount": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else "0"
                            }
                        ]

                # Process product details (avoid duplicates)
                product_id = row.get("ASSET_ID")
                if product_id and product_id not in seen_products:
                    seen_products.add(product_id)
                    self.mongo_data["Product_Details"].append({
                        "Product_Label": row.get("PROMOTION_INTEG_ID", ""),
                        "Customer_Ref": row.get("CUSTOMER_REF", ""),
                        "Product_Seq": int(row.get("BSS_PRODUCT_SEQ", 0)),
                        "Equipment_Ownership": "",
                        "Product_Id": product_id,
                        "Product_Name": row.get("PRODUCT_NAME", ""),
                        "Product_Status": row.get("ASSET_STATUS", ""),
                        "Effective_Dtm": format_datetime_z(row.get("ACCOUNT_EFFECTIVE_DTM_BSS")),
                        "Service_Address": row.get("ASSET_ADDRESS", ""),
                        "Cat": row.get("CUSTOMER_TYPE_CAT", ""),
                        "Db_Cpe_Status": "",
                        "Received_List_Cpe_Status": "",
                        "Service_Type": row.get("OSS_SERVICE_ABBREVIATION", ""),
                        "Region": row.get("CITY", ""),
                        "Province": row.get("PROVINCE", "")
                    })

            logger.info("Successfully read customer details.")
            return "success"

        except Exception as e:
            logger.error(f"Error reading customer details: {e}")
            raise DataProcessingError(f"Error reading customer details: {e}")
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

    def get_payment_data(self, time_period_start, time_period_end):
        """
        Retrieves and processes payment data within a specific time period.
        """
        mysql_conn = None
        cursor = None
        try:
            logger.info(f"Getting payment data for account: {self.account_num} within {time_period_start} to {time_period_end}")
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                raise DatabaseConnectionError("MySQL connection failed")
            
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(
                f"""
                SELECT * FROM debt_payment 
                WHERE AP_ACCOUNT_NUMBER = '{self.account_num}' 
                AND LOAD_DATE BETWEEN '{time_period_start}' AND '{time_period_end}'
                ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1
                """
            )
            payment_rows = cursor.fetchall()

            if payment_rows:
                payment = payment_rows[0]
                
                # Get corresponding bill data for the required Billed_Created field
                cursor.execute(
                    f"""
                    SELECT LAST_BILL_DTM FROM debt_cust_detail 
                    WHERE ACCOUNT_NUM = '{self.account_num}' 
                    AND LOAD_DATE BETWEEN '{time_period_start}' AND '{time_period_end}'
                    ORDER BY LOAD_DATE DESC LIMIT 1
                    """
                )
                bill_data = cursor.fetchone()
                
                # Format dates properly
                payment_date = payment["ACCOUNT_PAYMENT_DAT"].isoformat(timespec='milliseconds') + "Z"
                billed_date = (
                    bill_data["LAST_BILL_DTM"].strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    if bill_data and bill_data.get("LAST_BILL_DTM")
                    else "1900-01-01T00:00:00.000Z"
                )
                
                self.mongo_data["Last_Actions"] = [
                    {
                        "Billed_Seq": int(payment.get("ACCOUNT_PAYMENT_SEQ", "")),
                        "Billed_Created": billed_date,
                        "Payment_Seq": int(payment.get("ACCOUNT_PAYMENT_SEQ", "")),
                        "Payment_Created": payment_date,
                        "Payment_Money": float(payment["AP_ACCOUNT_PAYMENT_MNY"]) if payment.get("AP_ACCOUNT_PAYMENT_MNY") else 0,
                        "Billed_Amount": float(payment["AP_ACCOUNT_PAYMENT_MNY"]) if payment.get("AP_ACCOUNT_PAYMENT_MNY") else 0
                    }
                ]
                logger.info("Payment data processed with all required fields")
                return "success"
            return "failure"

        except Exception as e:
            logger.error(f"Payment processing error: {e}")
            raise DataProcessingError(f"Payment data error: {e}")
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

    def format_json_object(self):
        """
        Ensures all required fields are present in the JSON output
        """
        # Create a deep copy to avoid modifying original data
        json_data = json.loads(json.dumps(self.mongo_data, default=self.json_serializer))
        
        # Ensure Last_Actions has all required fields
        if "Last_Actions" in json_data and len(json_data["Last_Actions"]) > 0:
            for action in json_data["Last_Actions"]:
                action.setdefault("Billed_Seq", "")
                action.setdefault("Billed_Created", "1900-01-01T00:00:00.000Z")
                action.setdefault("Payment_Seq", "")
                action.setdefault("Payment_Created", "1900-01-01T00:00:00.000Z")
                action.setdefault("Payment_Money", 0)
                action.setdefault("Billed_Amount", 0)
        
        # Ensure other required structures exist
        json_data.setdefault("Customer_Details", {})
        json_data["Customer_Details"].setdefault("Nic", "")
        json_data["Customer_Details"].setdefault("Email_Address", "")
        
        return json.dumps(json_data, indent=4)

    def json_serializer(self, obj):
        """
        Custom JSON serializer that handles non-native JSON types in Python objects.

        Converts specific Python types to JSON-compatible formats:
        - datetime/date → ISO8601 strings
        - Decimal → float
        - None → empty string

        Parameters:
            obj (any): 
                The Python object to be serialized. Expected types:
                - datetime.datetime or datetime.date objects
                - decimal.Decimal objects
                - None/null values
                - NOTE: Other types will raise TypeError

        Returns:
            str or float:
                - For datetime/date: ISO8601 formatted string (e.g., "2023-01-01T00:00:00+00:00")
                - For Decimal: Converted to float (e.g., Decimal("10.5") → 10.5)
                - For None: Returns empty string ("")
        """
        if isinstance(obj, datetime):
            return obj.replace(microsecond=0).isoformat()  # Remove microseconds and ensure no extra 'Z'
        if isinstance(obj, date):
            return obj.isoformat()  # For date objects, no timezone is included
        if isinstance(obj, Decimal):
            return float(obj)
        if obj is None:
            return ""  # Convert None to empty string
        raise TypeError(f"Type {type(obj)} not serializable")

    def send_to_api(self, json_payload, api_url):
        """
        Enhanced API sending with better error handling
        """
        logger.info(f"Sending to API: {api_url}")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            # Validate payload first
            try:
                payload = json.loads(json_payload)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON payload: {e}")
                return False

            # Log the complete payload for debugging
            logger.debug(f"Full payload being sent: {json.dumps(payload, indent=2)}")

            response = requests.post(api_url, data=json_payload, headers=headers, timeout=30)
            response.raise_for_status()
            
            logger.info("API request successful")
            return True
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"API HTTP Error: {e}")
            if e.response is not None:
                try:
                    error_details = e.response.json()
                    logger.error(f"API Error Details: {json.dumps(error_details, indent=2)}")
                except ValueError:
                    logger.error(f"API Response: {e.response.text}")
            return False
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API Request Failed: {e}")
            return False
    
    def process_incident(self,time_period_start, time_period_end):
                """
                Processes a debt collection incident by gathering customer/payment data and sending to API.
                
                This method:
                1. Retrieves customer details from MySQL (already done in initialization)
                2. Retrieves optional payment data if customer exists
                3. Formats the data as JSON
                4. Sends to the configured API endpoint
                
                Returns:
                    bool: 
                        - True if incident was successfully created and sent to API
                        - False if:
                            * No customer details found
                            * Database query failed
                            * API communication failed
                """
                try:
                    logger.info(f"Processing incident for account: {self.account_num}, ID: {self.incident_id}")
                    
                    # 1. Retrieve Customer Details (critical failure if missing)
                    customer_status = self.read_customer_details(time_period_start, time_period_end)
                    if customer_status != "success" or not self.mongo_data["Customer_Details"]:
                        logger.error(f"No customer details found for account {self.account_num}")
                        return False
                    
                    # 2. Optional: Retrieve Payment Data
                    payment_status = self.get_payment_data(time_period_start, time_period_end)
                    if payment_status != "success":
                        logger.warning(f"Failed to retrieve payment data for account {self.account_num}")
                        return False
                        
                    # 3. Format and Send to API
                    json_output = self.format_json_object()
                    print(json_output)  # For debugging purposes
                    
                    api_url = read_api_config()
                    if not api_url:
                        raise APIConfigError("Empty API URL in config")
                            
                    api_response = self.send_to_api(json_output, api_url)
                    if not api_response:
                        raise IncidentCreationError("Empty API response")
                            
                    logger.info(f"API Success: {api_response}")
                    return True
                    
                except IncidentCreationError as e:
                    logger.error(f"Incident processing failed: {e}")
                    return False
                except APIConfigError as e:
                    logger.error(f"Configuration error: {e}")
                    return False
                except Exception as e:
                    logger.error(f"Unexpected error: {e}", exc_info=True)
                    return False
               

