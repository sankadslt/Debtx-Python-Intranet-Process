
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

# --- Standard library imports ---
import json  # For encoding and decoding JSON data
from datetime import datetime, date, timezone  # For handling date and time operations
from decimal import Decimal  # For precise decimal arithmetic (e.g., financial calculations)

# --- Third-party library imports ---
import requests  # For making HTTP requests to external APIs
import mysql.connector  # For connecting and interacting with a MySQL database

# --- Internal project imports ---
from utils.logger import SingletonLogger  # Singleton logger for logging messages
from utils.connectionSQL import MySQLConnectionSingleton  # Singleton MySQL connection
from utils.connectAPI import Get_API_URL_Singleton  # Singleton API URL configuration
from utils.custom_exceptions.customize_exceptions import APIConfigError, IncidentCreationError, DatabaseConnectionError, DataProcessingError  # Custom exceptions

# Initialize the logger using SingletonLogger
SingletonLogger.configure()
logger = SingletonLogger.get_logger("appLogger")

class CreateIncident:
    def __init__(self, account_num, incident_id):
        """
        Constructor for the CreateIncident class.

        Args:
            account_num (str): The account number associated with the incident.
            incident_id (int): The unique identifier for the incident.

        Raises:
            ValueError: If either account_num or incident_id fails conversion.
        """
        try:
            if account_num is None:
                raise ValueError("account_num cannot be None")
            self.account_num = str(account_num)

            if incident_id is None:
                raise ValueError("incident_id cannot be None")
            self.incident_id = int(incident_id)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid input: {e}")
        
        self.mongo_data = self.initialize_mongo_doc()

    def initialize_mongo_doc(self):
        """
        Initialize a default document structure for an incident.

        Returns:
            dict: A fully prepared document structure with default values and timestamps.
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return {
            "Doc_Version": 1.0,
            "Incident_Id": self.incident_id,
            "Account_Num": self.account_num,
            "Customer_Ref": f"CR{self.account_num}",  # Generate Customer_Ref
            "Arrears": 1000,  # Hard-coded for now
            "Bss_Arrears_Amount": 1000,  # Required by API
            "Last_Bss_Reading_Date": now,  # Required by API
            "arrears_band": "",
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
            "Accounts_Details": [{
                "Id": 0,
                "DRCMood": "",
                "Account_Num": self.account_num,
                "Account_Status": "",
                "OutstandingBalance": 0,
            }],
            "Contact_Details": [],
            "Product_Details": [],
            "Customer_Details": {},
            "Account_Details": {},
            "Last_Actions": [{
                "Billed_Seq": "",
                "Billed_Created": "",
                "Payment_Seq": 0,
                "Payment_Created": "",
                "Payment_Money": 0,
                "Billed_Amount": 0
            }],
            "Marketing_Details": [{
                "ACCOUNT_MANAGER": "",
                "CONSUMER_MARKET": "",
                "Informed_To": "",
                "Informed_On": "1900-01-01T00:00:00.100Z"
            }],
            "Action": "",
            "Validity_period": 0,
            "Remark": "",
            "updatedAt": now,
            "Rejected_By": "",
            "Rejected_Dtm": now,
            "Arrears_Band": "",
            "Source_Type": "",
            "Ref_Data_Temp_Permanent": [],
            "Case_Status": [],
            "Approvals": [],
            "DRC": [],
            "RO": [],
            "RO_Requests": [],
            "RO_Negotiation": [],
            "RO_Customer_Details_Edit": [],
            "RO_CPE_Collect": [],
            "Mediation_Board": [],
            "Settlement": [],
            "Money_Transactions": [],
            "Commission_Bill_Payment": [],
            "Bonus": [],
            "FTL_LOD": [],
            "Litigation": [],
            "LOD_Final_Reminder": [],
            "Dispute": [],
            "Abnormal_Abs": [""]
        }

    def format_datetime_z(self, date_value):
        """
        Formats a given datetime value into ISO 8601 format with a `.000Z` suffix.

        Args:
            date_value (str, datetime, or date): The input value to format.

        Returns:
            str: A string representing the formatted date-time in ISO 8601 format with `.000Z` suffix.
        """
        if not date_value:
            return "1900-01-01T00:00:00.000Z"
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        if isinstance(date_value, date):
            return datetime.combine(date_value, datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        try:
            parsed = datetime.strptime(str(date_value), "%Y-%m-%d %H:%M:%S")
            return parsed.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except Exception:
            return "1900-01-01T00:00:00.000Z"

    def read_customer_details(self):
        """
        Retrieves and processes customer account data from MySQL, transforming it into MongoDB document structure.

        Returns:
            str: "success" if data is retrieved and processed, "error" or "no_data" otherwise.
        """
        try:
            logger.info(f"Reading customer details for account number: {self.account_num}")
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("Failed to connect to MySQL for reading customer details.")
                
                # Log connection type for debugging
                logger.debug(f"Connection type: {type(connection)}")

                # Use dictionary cursor for mysql-connector-python
                cursor = connection.cursor(dictionary=True)

                try:
                    query = """
                        SELECT * FROM debt_cust_detail 
                        WHERE ACCOUNT_NUM = %s 
                        ORDER BY LOAD_DATE DESC
                    """
                    logger.debug(f"Executing query: {query % self.account_num}")
                    cursor.execute(query, (self.account_num,))
                    rows = cursor.fetchall()

                    # Debug: Log the type of rows and first row (if available)
                    logger.debug(f"Type of rows: {type(rows)}")
                    if rows:
                        logger.debug(f"Type of first row: {type(rows[0])}")

                    if rows is None:
                        logger.error(f"Query returned None for account {self.account_num}")
                        return "error"

                    if not rows:
                        logger.warning(f"No customer data found for account {self.account_num}")
                        return "no_data"

                    seen_products = set()
                    for row in rows:
                        if not self.mongo_data["Customer_Details"]:
                            customer_ref_value = row.get("CUSTOMER_REF", "") or f"CR{self.account_num}"
                            self.mongo_data["Customer_Ref"] = customer_ref_value

                            if row.get("TECNICAL_CONTACT_EMAIL"):
                                contact_details_element = {
                                    "Contact_Type": "email",
                                    "Contact": row["TECNICAL_CONTACT_EMAIL"] if "@" in row["TECNICAL_CONTACT_EMAIL"] else "",
                                    "Create_Dtm": self.format_datetime_z(row.get("LOAD_DATE")),
                                    "Create_By": "drs_admin"
                                }
                                self.mongo_data["Contact_Details"].append(contact_details_element)

                            if row.get("MOBILE_CONTACT"):
                                contact_details_element = {
                                    "Contact_Type": "mobile",
                                    "Contact": row["MOBILE_CONTACT"],
                                    "Create_Dtm": self.format_datetime_z(row.get("LOAD_DATE")),
                                    "Create_By": "drs_admin"
                                }
                                self.mongo_data["Contact_Details"].append(contact_details_element)

                            if row.get("WORK_CONTACT"):
                                contact_details_element = {
                                    "Contact_Type": "fix",
                                    "Contact": row["WORK_CONTACT"],
                                    "Create_Dtm": self.format_datetime_z(row.get("LOAD_DATE")),
                                    "Create_By": "drs_admin"
                                }
                                self.mongo_data["Contact_Details"].append(contact_details_element)

                            self.mongo_data["Customer_Details"] = {
                                "Customer_Name": row.get("CONTACT_PERSON", ""),
                                "Company_Name": "",
                                "Company_Registry_Number": "",
                                "Full_Address": row.get("ASSET_ADDRESS", ""),
                                "Zip_Code": row.get("ZIP_CODE", ""),
                                "Customer_Type_Name": "",
                                "Nic": str(row.get("NIC", "")),
                                "Customer_Type_Id": str(row.get("CUSTOMER_TYPE_ID", 0)),  # Convert to string
                                "Customer_Type": row.get("CUSTOMER_TYPE", "")
                            }

                            self.mongo_data["Account_Details"] = {
                                "Account_Status": row.get("ACCOUNT_STATUS_BSS", ""),
                                "Acc_Effective_Dtm": self.format_datetime_z(row.get("ACCOUNT_EFFECTIVE_DTM_BSS")),
                                "Acc_Activate_Date": "1900-01-01T00:00:00.000Z",
                                "Credit_Class_Id": str(row.get("CREDIT_CLASS_ID", 0)),  # Convert to string
                                "Credit_Class_Name": row.get("CREDIT_CLASS_NAME", ""),
                                "Billing_Centre": row.get("BILLING_CENTER_NAME", ""),
                                "Customer_Segment": row.get("CUSTOMER_SEGMENT_ID", ""),
                                "Mobile_Contact_Tel": "",
                                "Daytime_Contact_Tel": "",
                                "Email_Address": str(row.get("EMAIL", "")),
                                "Last_Rated_Dtm": "1900-01-01T00:00:00.000Z"
                            }

                            if row.get("LAST_PAYMENT_DAT"):
                                self.mongo_data["Last_Actions"] = [{
                                    "Billed_Seq": str(row.get("LAST_BILL_SEQ", 0)),  # Convert to string
                                    "Billed_Created": self.format_datetime_z(row.get("LAST_BILL_DTM")),
                                    "Payment_Seq": str(row.get("LAST_PAYMENT_SEQ", 0)),  # Convert to string
                                    "Payment_Created": self.format_datetime_z(row.get("LAST_PAYMENT_DAT")),
                                    "Payment_Money": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else 0.0,
                                    "Billed_Amount": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else 0.0
                                }]

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
                                "Effective_Dtm": self.format_datetime_z(row.get("ACCOUNT_EFFECTIVE_DTM_BSS")),
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

                finally:
                    cursor.close()
        except Exception as e:
            logger.error(f"Error reading customer details: {e}", exc_info=True)
            raise DataProcessingError(f"Error reading customer details: {e}")

    def get_payment_data(self):
        """
        Retrieves and processes payment data for the account.

        Returns:
            str: "success" if data is retrieved and processed, "failure" otherwise.
        """
        try:
            logger.info(f"Getting payment data for account: {self.account_num}")
            with MySQLConnectionSingleton() as db_connection:
                connection = db_connection.get_connection()
                if not connection:
                    raise DatabaseConnectionError("MySQL connection failed.")
                
                # Log connection type for debugging
                logger.debug(f"Connection type: {type(connection)}")

                # Use dictionary cursor for mysql-connector-python
                cursor = connection.cursor(dictionary=True)

                try:
                    query = """
                        SELECT * FROM debt_payment 
                        WHERE AP_ACCOUNT_NUMBER = %s 
                        ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1
                    """
                    logger.debug(f"Executing query: {query % self.account_num}")
                    cursor.execute(query, (self.account_num,))
                    payment_rows = cursor.fetchall()

                    if payment_rows:
                        payment = payment_rows[0]
                        cursor.execute("""
                            SELECT LAST_BILL_DTM FROM debt_cust_detail 
                            WHERE ACCOUNT_NUM = %s 
                            ORDER BY LOAD_DATE DESC LIMIT 1
                        """, (self.account_num,))
                        bill_data = cursor.fetchone()
                        
                        payment_date = self.format_datetime_z(payment.get("ACCOUNT_PAYMENT_DAT"))
                        billed_date = (
                            self.format_datetime_z(bill_data["LAST_BILL_DTM"])
                            if bill_data and bill_data.get("LAST_BILL_DTM")
                            else "1900-01-01T00:00:00.000Z"
                        )

                        self.mongo_data["Last_Actions"] = [{
                            "Billed_Seq": str(payment.get("ACCOUNT_PAYMENT_SEQ", 0)),  # Convert to string
                            "Billed_Created": billed_date,
                            "Payment_Seq": str(payment.get("ACCOUNT_PAYMENT_SEQ", 0)),  # Convert to string
                            "Payment_Created": payment_date,
                            "Payment_Money": float(payment.get("AP_ACCOUNT_PAYMENT_MNY", 0)),
                            "Billed_Amount": float(payment.get("AP_ACCOUNT_PAYMENT_MNY", 0))
                        }]
                        logger.info("Payment data processed with all required fields")
                        return "success"
                    return "failure"
                finally:
                    cursor.close()
        except Exception as e:
            logger.error(f"Payment processing error: {e}")
            raise DataProcessingError(f"Payment data error: {e}")

    def format_json_object(self):
        """
        Formats the document data into a JSON string, ensuring compatibility with API model.

        Returns:
            str: A formatted JSON string with proper indentation and data conversions.
        """
        def to_camel_case(snake_str):
            """Convert snake_case or uppercase to camelCase."""
            components = snake_str.lower().split('_')
            return components[0] + ''.join(x.capitalize() for x in components[1:])

        json_data = json.loads(json.dumps(self.mongo_data, default=self.json_serializer()))

        # Convert specific fields to strings
        if "Customer_Details" in json_data:
            json_data["Customer_Details"]["Nic"] = str(json_data["Customer_Details"].get("Nic", ""))
            json_data["Customer_Details"]["Customer_Type_Id"] = str(json_data["Customer_Details"].get("Customer_Type_Id", ""))
        if "Account_Details" in json_data:
            json_data["Account_Details"]["Email_Address"] = str(json_data["Account_Details"].get("Email_Address", ""))
            json_data["Account_Details"]["Credit_Class_Id"] = str(json_data["Account_Details"].get("Credit_Class_Id", ""))

        # Transform Last_Actions to match API model
        if "Last_Actions" in json_data:
            transformed_actions = []
            for action in json_data["Last_Actions"]:
                transformed_action = {
                    to_camel_case(key): value for key, value in action.items()
                }
                transformed_actions.append(transformed_action)
            json_data["Last_Actions"] = transformed_actions

        # Transform Marketing_Details to match API model
        if "Marketing_Details" in json_data:
            transformed_marketing = []
            for marketing in json_data["Marketing_Details"]:
                transformed_marketing_item = {
                    to_camel_case(key): value for key, value in marketing.items()
                }
                transformed_marketing.append(transformed_marketing_item)
            json_data["Marketing_Details"] = transformed_marketing

        # Remove Accounts_Details (not in API model) and add Account_Cross_Details
        if "Accounts_Details" in json_data:
            account_cross_details = [
                {
                    "Incident_Id": self.incident_id,
                    "Case_Id": None,
                    "Account_Num": detail["Account_Num"],
                    "Account_Status": detail["Account_Status"],
                    "Outstanding_Balance": float(detail["OutstandingBalance"])
                }
                for detail in json_data["Accounts_Details"]
            ]
            json_data["Account_Cross_Details"] = account_cross_details
            del json_data["Accounts_Details"]

        return json.dumps(json_data, indent=4)

    def json_serializer(self):
        """
        Returns a custom serializer function for JSON-compatible data.

        Returns:
            function: A function that serializes objects into JSON-compatible formats.
        """
        def _serialize(obj):
            if isinstance(obj, (datetime, date)):
                if isinstance(obj, date) and not isinstance(obj, datetime):
                    obj = datetime.combine(obj, datetime.min.time())
                return obj.replace(microsecond=0).isoformat() + ".000Z"
            if isinstance(obj, Decimal):
                return float(obj) if obj % 1 else int(obj)
            if obj is None or str(obj).lower() == "none":
                return None  # Changed to None to match API model
            if isinstance(obj, (int, float)):
                return obj
            return str(obj)
        return _serialize

    def send_to_api(self, json_output, api_url):
        """
        Sends JSON data to the specified API endpoint.

        Args:
            json_output (str): The JSON data to send.
            api_url (str): The API URL.

        Returns:
            dict: The JSON response from the API, or None if an error occurs.
        """
        try:
            logger.info(f"Sending data to API: {api_url}")
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            response = requests.post(api_url, data=json_output, headers=headers)
            logger.info(f"API Response: {response.status_code} - {response.text}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error: {e.response.status_code} - {e.response.text}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Error: {e}")
            return None

    def process_incident(self):
        """
        Processes a debt collection incident by gathering data and sending to API.

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info(f"Processing incident {self.incident_id} for account {self.account_num}")
            customer_status = self.read_customer_details()
            if customer_status != "success" or not self.mongo_data["Customer_Details"]:
                logger.error(f"No customer details found for account {self.account_num}")
                return False

            payment_status = self.get_payment_data()
            if payment_status != "success":
                logger.warning(f"Failed to retrieve payment data for account {self.account_num}")

            json_output = self.format_json_object()
            print(json_output)

            api_config = Get_API_URL_Singleton()
            api_url = api_config.get_api_url()
            if not api_url:
                raise APIConfigError("Empty API URL in config")

            api_response = self.send_to_api(json_output, api_url)
            if not api_response:
                raise IncidentCreationError("Empty API response")

            logger.info(f"API Success: {api_response}")
            return True

        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return False