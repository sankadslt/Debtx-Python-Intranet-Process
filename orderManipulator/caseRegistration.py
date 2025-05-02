# --- Standard library imports ---
import json  # For encoding and decoding JSON data
from datetime import datetime, date  # For handling date and time operations
from decimal import Decimal  # For precise decimal arithmetic (e.g., financial calculations)

# --- Third-party library imports ---
import requests  # For making HTTP requests to external APIs
import pymysql  # For connecting and interacting with a MySQL database
from pymongo import MongoClient  # For connecting and interacting with a MongoDB database

# --- Internal project imports ---
from utils.database.connectSQL import get_mysql_connection  # Function to establish MySQL database connection
from utils.logger.logger import get_logger  # Function to initialize and retrieve the project’s logger
from utils.api.connectAPI import read_api_config  # Function to read API configuration settings
from utils.custom_exceptions.customize_exceptions import APIConfigError, IncidentCreationError  # Custom exceptions for specific error handling


# Initialize the logger with the name "task_status_logger"
logger = get_logger("task_status_logger")

# Prevent duplicate handlers if the logger is initialized multiple times
if len(logger.handlers) > 1:
    logger.handlers = [logger.handlers[0]]  # Keep only the first handler

# Disable logger message propagation to parent loggers
logger.propagate = False


class Process_Incident:
    
    def __init__(self, account_num, incident_id, mongo_collection):
        """
        Initialize the class instance with account number, incident ID, and MongoDB collection.

        Args:
            account_num (str): The account number associated with the incident. 
                                    It will be stored as a string internally.
            incident_id (int): The unique incident ID. It will be converted and stored as an integer.
            mongo_collection (pymongo.collection.Collection): The MongoDB collection to interact with.

        Attributes:
            account_num (str): The account number as a string.
            incident_id (int): The incident ID as an integer.
            collection (Collection): MongoDB collection object for document operations.
            mongo_data (dict): The initialized MongoDB document related to the given account and incident.

        Returns:
            None
        """

        # Store the account number as a string
        self.account_num = str(account_num)
        
        # Store the incident ID as an integer
        self.incident_id = int(incident_id)
        
        # Save the MongoDB collection reference
        self.collection = mongo_collection
        
        # Initialize and store the MongoDB document associated with this account and incident
        self.mongo_data = self.initialize_mongo_doc()


    def get_config_from_mongo(self):
        """
        Fetch the configuration document from MongoDB for the given account number.

        Args:
            None (uses instance attributes: self.account_num and self.collection)

        Outputs:
            - Logs a success message if the document is found.
            - Logs a warning if no document is found.
            - Logs an error if an exception occurs during the query.

        Returns:
            dict or None:
                - Returns the configuration document (as a dictionary) if found.
                - Returns None if the document is not found or if an error occurs.
        """

        try:
            # Attempt to find a document in MongoDB with the matching account number
            config_doc = self.collection.find_one({"account_num": self.account_num})

            if config_doc:
                # Log successful retrieval
                logger.info(f"Fetched config document from MongoDB for account {self.account_num}")
                return config_doc

            # Log a warning if no document is found
            logger.warning(f"No config document found in MongoDB for account {self.account_num}")
            return None

        except Exception as e:
            # Log any exceptions that occur during the database query
            logger.error(f"Error fetching config document from MongoDB for account {self.account_num}: {e}")
            return None


    def get_doc_version_from_mongo(self, config_doc):
        """
        Retrieve the document version from the MongoDB configuration document.

        Args:
            config_doc (dict): The configuration document fetched from MongoDB.

        Outputs:
            - Logs a warning if the "doc_version" field is missing, invalid, or cannot be converted to a float.

        Returns:
            float:
                - Returns the document version as a float if valid.
                - Returns the default value 1.0 if the field is missing or invalid.
        """

        # Check if the config document exists and contains the "doc_version" field
        if config_doc and "doc_version" in config_doc:
            try:
                # Try to convert the doc_version to a float
                return float(config_doc["doc_version"])
            except (ValueError, TypeError) as e:
                # Log a warning and fall back to the default version if conversion fails
                logger.warning(f"Invalid doc_version value. Using default 1.0. Error: {e}")

        # Return default version if "doc_version" is missing or invalid
        return 1.0


    def initialize_mongo_doc(self):
        """
        Initialize a default MongoDB document structure for an incident.

        Description:
            - Prepares a structured document containing default values and timestamps.
            - Incorporates the latest config document and document version from MongoDB.
            - Ensures all necessary fields are populated even before any updates happen.

        Args:
            None (uses instance attributes: self.account_num, self.incident_id)

        Outputs:
            - Creates a dictionary representing a full MongoDB document ready for insertion or update.

        Returns:
            dict:
                - A fully prepared document structure with default values and timestamps.

        """
        
        # Get the current timestamp (ISO format) with milliseconds precision
        now = datetime.now().replace(microsecond=0).isoformat() + ".000Z"
        # Fetch the configuration document for the account (if available)
        config_doc = self.get_config_from_mongo()
        # Extract the document version from the config document or default to 1.0
        doc_version = self.get_doc_version_from_mongo(config_doc)
        
        return {
            "Doc_Version": doc_version,
            "Incident_Id": self.incident_id,
            "Account_Num": self.account_num,
            "Customer_Ref": "",
            "Arrears": 1000,
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
            "Contact_Details": [],
            "Product_Details": [],
            "Customer_Details": {
                "Customer_Name": "",
                "Company_Name": "",
                "Company_Registry_Number": "",
                "Full_Address": "",
                "Zip_Code": "",
                "Customer_Type_Name": "",
                "Nic": "",
                "Customer_Type_Id": 0,
                "Customer_Type": "",
                "Customer_Ref": "",
                "Email_Address": ""
            },
            "Account_Details": {
                "Id": 0,
                "DRCMood": "",
                "Account_Num": self.account_num,
                "Account_Status": "",
                "OutstandingBalance": 0,
                "Acc_Effective_Dtm": "1900-01-01T00:00:00.000Z",
                "Acc_Activate_Date": "1900-01-01T00:00:00.000Z",
                "Credit_Class_Id": 0,
                "Credit_Class_Name": "",
                "Billing_Centre": "",
                "Customer_Segment": "",
                "Mobile_Contact_Tel": "",
                "Daytime_Contact_Tel": "",
                "Email_Address": "",
                "Last_Rated_Dtm": "1900-01-01T00:00:00.000Z"
            },
            "Last_Actions": [],
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
            "Ref Data - temp,permanent": [],
            "Case Status": [],
            "Remark": "",
            "Approvals": [],
            "DRC": [],
            "RO": [],
            "RO Requests": [],
            "RO- Negotiation": [],
            "RO - Customer details Edit": [],
            "RO - CPE Collect": [],
            "Mediation Board": [],
            "Settlement": [],
            "Money Transactions": [],
            "Commission - Bill Payment": [],
            "Bonus": [],
            "FTL LOD": [],
            "Litigation": [],
            "LOD / Final Reminder": [],
            "Dispute": [],
            "Abnormal Stop": []
        }

    def format_datetime_z(self, date_value):
        """
        Formats a given datetime value into ISO 8601 format with a `.000Z` suffix. 
        If the provided value is invalid or empty, returns a default "1900-01-01T00:00:00.000Z".

        Args:
            date_value (str, datetime, or date): The input value to format. It can be:
                - A string in the format "%Y-%m-%d %H:%M:%S"
                - A datetime object
                - A date object

        Outputs:
            - If the input is valid, returns the formatted datetime string.
            - If the input is invalid or empty, returns the default "1900-01-01T00:00:00.000Z".

        Returns:
            str: A string representing the formatted date-time in ISO 8601 format with `.000Z` suffix.
        """
        
        # If no value is provided (empty or None), return the default date
        if not date_value:
            return "1900-01-01T00:00:00.000Z"
        
        # If the date_value is a string, attempt to parse it into a datetime object
        if isinstance(date_value, str):
            try:
                # Convert string to datetime object based on the expected format
                date_value = datetime.strptime(date_value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                # If the string is not in the correct format, return the default date
                return "1900-01-01T00:00:00.000Z"
        
        # If the date_value is a date object (not datetime), convert it to datetime
        elif isinstance(date_value, date) and not isinstance(date_value, datetime):
            # Combine date with the minimum time value to create a datetime object
            date_value = datetime.combine(date_value, datetime.min.time())
        
        # Format the datetime object into ISO 8601 format and add the ".000Z" suffix
        return date_value.replace(microsecond=0).isoformat() + ".000Z"


    def read_customer_details(self):
        """
        Reads customer details from a MySQL database and processes the information into a MongoDB document.

        This function fetches customer data from the MySQL database, processes the contact, account, 
        product details, and other related information, and stores them in the MongoDB document.

        Outputs:
            - "success" if customer details were successfully read and processed.
            - "error" if an error occurs during the database operation or data processing.
            - "no_data" if no customer data is found in the database for the given account.
            - Logs information at various levels to track the progress and any issues.

        Returns:
            str: One of "success", "error", or "no_data" based on the outcome of the operation.
        """
        
        mysql_conn = None
        cursor = None
        try:
            # Log the start of the process
            logger.info(f"Reading customer details for account: {self.account_num}")

            # Establish MySQL connection
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                # Log error and return if connection fails
                logger.error("MySQL connection failed")
                return "error"
            
            # Create a cursor object for database interaction
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            
            # Define the SQL query to fetch customer details based on the account number
            query = """
                SELECT * FROM debt_cust_detail 
                WHERE ACCOUNT_NUM = %s 
                ORDER BY LOAD_DATE DESC
            """
            # Log the query for debugging purposes
            logger.debug(f"Executing query: {query % self.account_num}")
            
            # Execute the query with the provided account number
            cursor.execute(query, (self.account_num,))
            
            # Fetch all rows returned by the query
            rows = cursor.fetchall()

            # If the query returned None, log the issue and return an error
            if rows is None:
                logger.error(f"Query returned None for account {self.account_num}. Possible query execution failure.")
                return "error"

            # If no data was found for the account, log the warning and return 'no_data'
            if not rows:
                logger.warning(f"No customer data found for account {self.account_num}")
                return "no_data"

            # Track products to avoid duplication in MongoDB
            seen_products = set()

            for row in rows:
                # Process customer/account details only once
                if not self.mongo_data["Customer_Details"].get("Customer_Name"):
                    
                    customer_ref_value = row.get("CUSTOMER_REF", "") or "UNKNOWN"  # Fallback to "UNKNOWN" if empty
                    self.mongo_data["Customer_Ref"] = customer_ref_value
                    
                    # Handle contact details (email, mobile, work contact)
                    if row.get("TECNICAL_CONTACT_EMAIL"):
                        # Validate email format and add to contact details
                        logger.debug(f"Processing email: {row.get('TECNICAL_CONTACT_EMAIL')}")
                        contact_details_element = {
                            "Contact_Type": "email",
                            "Contact": row["TECNICAL_CONTACT_EMAIL"] if "@" in row["TECNICAL_CONTACT_EMAIL"] else "",
                            "Create_Dtm": self.format_datetime_z(row.get("LOAD_DATE")),
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    if row.get("MOBILE_CONTACT"):
                        # Process mobile contact
                        logger.debug(f"Processing mobile: {row.get('MOBILE_CONTACT')}")
                        contact_details_element = {
                            "Contact_Type": "mobile",
                            "Contact": row["MOBILE_CONTACT"],
                            "Create_Dtm": self.format_datetime_z(row.get("LOAD_DATE")),
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    if row.get("WORK_CONTACT"):
                        # Process work contact
                        logger.debug(f"Processing work: {row.get('WORK_CONTACT')}")
                        contact_details_element = {
                            "Contact_Type": "fix",
                            "Contact": row["WORK_CONTACT"],
                            "Create_Dtm": self.format_datetime_z(row.get("LOAD_DATE")),
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    # Update customer details in MongoDB document
                    self.mongo_data["Customer_Details"] = {
                        "Customer_Name": row.get("CONTACT_PERSON", ""),
                        "Company_Name": "",
                        "Company_Registry_Number": "",
                        "Full_Address": row.get("ASSET_ADDRESS", ""),
                        "Zip_Code": row.get("ZIP_CODE", ""),
                        "Customer_Type_Name": "",
                        "Nic": str(row.get("NIC", "")),
                        "Customer_Type_Id": int(row.get("CUSTOMER_TYPE_ID", 0)),
                        "Customer_Type": row.get("CUSTOMER_TYPE", ""),
                        "Customer_Ref": row.get("CUSTOMER_REF", ""),
                        "Email_Address": str(row.get("EMAIL", ""))
                    }

                    # Set account details in MongoDB document
                    acc_eff_dtm = self.format_datetime_z(row.get("ACCOUNT_EFFECTIVE_DTM_BSS"))
                    self.mongo_data["Account_Details"] = {
                        "Id": 0,
                        "DRCMood": "",
                        "Account_Num": self.account_num,
                        "Account_Status": "",
                        "OutstandingBalance": 0,
                        "Account_Status": row.get("ACCOUNT_STATUS_BSS", ""),
                        "Acc_Effective_Dtm": acc_eff_dtm,
                        "Acc_Activate_Date": "1900-01-01T00:00:00.000Z",
                        "Credit_Class_Id": int(row.get("CREDIT_CLASS_ID", 0)),
                        "Credit_Class_Name": row.get("CREDIT_CLASS_NAME", ""),
                        "Billing_Centre": row.get("BILLING_CENTER_NAME", ""),
                        "Customer_Segment": row.get("CUSTOMER_SEGMENT_ID", ""),
                        "Mobile_Contact_Tel": "",
                        "Daytime_Contact_Tel": "",
                        "Email_Address": str(row.get("EMAIL", "")),
                        "Last_Rated_Dtm": "1900-01-01T00:00:00.000Z"
                    }

                    # Handle last payment details if available
                    if row.get("LAST_PAYMENT_DAT"):
                        self.mongo_data["Last_Actions"] = [{
                            "Billed_Seq": int(row.get("LAST_BILL_SEQ", 0)),
                            "Billed_Created": row.get("LAST_BILL_DTM", "1900-01-01T00:00:00.000Z"),
                            "Payment_Seq": 0,
                            "Payment_Created": row.get("LAST_PAYMENT_DAT", "1900-01-01T00:00:00.000Z"),
                            "Payment_Money": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else 0.0,
                            "Billed_Amount": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else 0.0
                        }]
                
                # Process product details and avoid duplication
                product_id = row.get("ASSET_ID")
                if product_id and product_id not in seen_products:
                    seen_products.add(product_id)
                    eff_dtm = self.format_datetime_z(row.get("ACCOUNT_EFFECTIVE_DTM_BSS"))
                    self.mongo_data["Product_Details"].append({
                        "Product_Label": row.get("PROMOTION_INTEG_ID", ""),
                        "Customer_Ref": row.get("CUSTOMER_REF", ""),
                        "Product_Seq": int(row.get("BSS_PRODUCT_SEQ", 0)),
                        "Equipment_Ownership": "",
                        "Product_Id": product_id,
                        "Product_Name": row.get("PRODUCT_NAME", ""),
                        "Product_Status": row.get("ASSET_STATUS", ""),
                        "Effective_Dtm": eff_dtm,
                        "Service_Address": row.get("ASSET_ADDRESS", ""),
                        "Cat": row.get("CUSTOMER_TYPE_CAT", ""),
                        "Db_Cpe_Status": "",
                        "Received_List_Cpe_Status": "",
                        "Service_Type": row.get("OSS_SERVICE_ABBREVIATION", ""),
                        "Region": row.get("CITY", ""),
                        "Province": row.get("PROVINCE", "")
                    })

            # Return success if everything completed without errors
            return "success"

        except Exception as e:
            # Log error and stack trace if an exception occurs
            logger.error(f"Error reading customer details: {e}", exc_info=True)
            return "error"

        finally:
            # Ensure cursor and MySQL connection are closed after operation
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()


    def get_payment_data(self):
        """
        Retrieves the most recent payment data for a given account from the MySQL database.
        
        The function executes a query to fetch the latest payment information for the account number,
        processes the data, and stores it in the MongoDB document under 'Last_Actions'.
        
        Outputs:
            - "success" if the payment data was retrieved and stored successfully.
            - "failure" if there was an error or if no payment data is found for the account.
        
        Returns:
            str: One of "success" or "failure" based on the outcome of the operation.
        """
        
        mysql_conn = None
        cursor = None
        try:
            # Log the start of the process
            logger.info(f"Getting payment data for account: {self.account_num}")

            # Establish MySQL connection
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                # Log error and return if connection fails
                logger.error("MySQL connection failed")
                return "failure"
            
            # Create a cursor object for database interaction
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            
            # Define the SQL query to fetch the most recent payment data based on account number
            query = "SELECT * FROM debt_payment WHERE AP_ACCOUNT_NUMBER = %s ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1"
            
            # Log the query for debugging purposes
            logger.debug(f"Executing query: {query % self.account_num}")
            
            # Execute the query with the provided account number
            cursor.execute(query, (self.account_num,))
            
            # Fetch all rows returned by the query (it will return only one row due to the LIMIT 1)
            payment_rows = cursor.fetchall()

            # If payment data is found, process the first row
            if payment_rows:
                payment = payment_rows[0]
                # Format the payment date using the provided method
                payment_date = self.format_datetime_z(payment.get("ACCOUNT_PAYMENT_DAT"))
                
                # Store the payment data in the MongoDB document
                self.mongo_data["Last_Actions"] = [{
                    "Billed_Seq": int(payment.get("ACCOUNT_PAYMENT_SEQ", 0)),
                    "Billed_Created": payment_date,
                    "Payment_Seq": int(payment.get("ACCOUNT_PAYMENT_SEQ", 0)),
                    "Payment_Created": payment_date,
                    "Payment_Money": float(payment.get("AP_ACCOUNT_PAYMENT_MNY", 0)),
                    "Billed_Amount": float(payment.get("AP_ACCOUNT_PAYMENT_MNY", 0))
                }]
                
                # Return success if data is processed successfully
                return "success"
            
            # Return failure if no payment data is found
            return "failure"

        except Exception as e:
            # Log error and stack trace if an exception occurs
            logger.error(f"Error retrieving payment data: {e}")
            return "failure"

        finally:
            # Ensure cursor and MySQL connection are closed after operation
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()


    def format_json_object(self):
        """
        Formats the MongoDB data object (`mongo_data`) into a well-structured JSON string.

        The method serializes the `mongo_data` dictionary, ensuring that specific fields such as
        "Nic" (in "Customer_Details") and "Email_Address" (in "Account_Details") are converted to strings.
        It also applies indentation for better readability.

        Outputs:
            A JSON string representation of the `mongo_data` object, with specific fields formatted.

        Returns:
            str: A formatted JSON string with proper indentation and data conversions.
        """
        
        # Convert the mongo_data dictionary to a JSON string, using a custom serializer if necessary
        json_data = json.loads(json.dumps(self.mongo_data, default=self.json_serializer()))
        
        # If the "Customer_Details" key exists in the JSON data, ensure "Nic" is a string
        if "Customer_Details" in json_data:
            json_data["Customer_Details"]["Nic"] = str(json_data["Customer_Details"].get("Nic", ""))
        
        # If the "Account_Details" key exists in the JSON data, ensure "Email_Address" is a string
        if "Account_Details" in json_data:
            json_data["Account_Details"]["Email_Address"] = str(json_data["Account_Details"].get("Email_Address", ""))
        
        # Convert the JSON data back to a string with proper indentation for readability
        return json.dumps(json_data, indent=4)


    def json_serializer(self):
        """
        Returns a custom serializer function that converts non-serializable objects into JSON-compatible data.

        The serializer handles the following types of objects:
        - datetime and date objects are converted to ISO format strings.
        - Decimal objects are converted to float or int depending on their value.
        - None or "None" values are converted to an empty string.
        - Other types (int, float) are returned as-is.
        - All other objects are converted to their string representation.

        Returns:
            function: A function that serializes various objects into JSON-compatible data.
        """
        def _serialize(obj):
            """
            Serializes a given object into a JSON-compatible format.

            Args:
                obj: The object to serialize.

            Returns:
                The serialized object in a JSON-compatible format.
            """
            
            # Handle datetime and date objects
            if isinstance(obj, (datetime, date)):
                # If the object is a date (not a datetime), convert it to datetime at the minimum time
                if isinstance(obj, date) and not isinstance(obj, datetime):
                    obj = datetime.combine(obj, datetime.min.time())
                # Return the ISO format of the datetime, with microseconds removed, followed by ".000Z"
                return obj.replace(microsecond=0).isoformat() + ".000Z"
            
            # Handle Decimal objects
            if isinstance(obj, Decimal):
                # Convert Decimal to either float or int depending on whether it has a fractional part
                return float(obj) if obj % 1 else int(obj)
            
            # Handle None or "None" values by returning an empty string
            if obj is None or str(obj).lower() == "none":
                return ""
            
            # Handle numeric types (int, float) by returning them as they are
            if isinstance(obj, (int, float)):
                return obj
            
            # For all other objects, convert them to a string
            return str(obj)

        return _serialize


    def send_to_api(self, json_output, api_url):
        """
        Sends the provided JSON data to the specified API endpoint and processes the response.

        Args:
            json_output (str): The JSON data to be sent in the POST request.
            api_url (str): The API URL to which the data will be sent.

        Returns:
            dict: The JSON response from the API if the request is successful.
            None: If there is an error (either HTTP error or general request error).
        
        Logs:
            Logs the status of the API request and the response (status code and text).
        """
        try:
            # Log the initiation of the API call
            logger.info(f"Sending data to API: {api_url}")
            
            # Set headers for the POST request to inform the API of the content type and response format
            headers = {
                "Content-Type": "application/json",  # The data sent is in JSON format
                "Accept": "application/json"         # Expecting JSON data in the response
            }
            
            # Send a POST request with the provided JSON data and headers
            response = requests.post(api_url, data=json_output, headers=headers)
            
            # Log the response details (status code and content) for debugging purposes
            logger.info(f"API Response: {response.status_code} - {response.text}")
            
            # If the response status code indicates an error (non-2xx status), raise an HTTPError
            response.raise_for_status()
            
            # If the response is successful, return the parsed JSON response
            return response.json()

        except requests.exceptions.HTTPError as e:
            # Handle HTTP errors (e.g., 404, 500 status codes)
            logger.error(f"HTTP Error: {e.response.status_code} - {e.response.text}")
            return None  # Return None if there's an HTTP error

        except requests.exceptions.RequestException as e:
            # Handle any other request-related issues (e.g., network errors, timeouts)
            logger.error(f"Request Error: {e}")
            return None  # Return None if there's a general request error


    def process_incident(self):
        """
        Processes an incident for a given account and incident ID by:
        - Retrieving customer and payment data.
        - Formatting the data into JSON.
        - Sending the data to an external API.
        
        This function involves multiple steps: reading customer details, retrieving payment data, 
        formatting data into JSON, and sending it to an API. It handles various errors related to 
        API configuration and incident creation, logging them as needed.

        Input:
        - self.account_num (str): The account number associated with the incident.
        - self.incident_id (int): The unique incident identifier.
        
        Output:
        - Returns a tuple (success_flag, message_or_response).
        - success_flag (bool): Indicates whether the incident was processed successfully.
        - message_or_response (str or dict): Error message in case of failure or API response on success.

        Returns:
        - Tuple of the form (bool, str/dict):
        - True, dict: If the incident is successfully processed and API responds with data.
        - False, str: If an error occurs, with the error message.
        """
        
        try:
            # Log the start of the incident processing with account and incident information
            logger.info(f"Processing incident for account: {self.account_num}, ID: {self.incident_id}")
            
            # Step 1: Retrieve customer details
            # customer_status can be "error", "no_data", or "success"
            customer_status = self.read_customer_details()

            # If there is an error reading customer details, log it and return False with the error message.
            if customer_status == "error":
                error_msg = f"Error reading customer details for account {self.account_num}"
                logger.error(error_msg)
                return False, error_msg
            
            # If no customer data is found, log a warning and return False with the message.
            elif customer_status == "no_data":
                error_msg = f"No customer data found for account {self.account_num}"
                logger.warning(error_msg)
                return False, error_msg
            
            # Step 2: Retrieve payment data for the account
            # payment_status indicates success or failure of retrieving payment data.
            payment_status = self.get_payment_data()

            # If payment data retrieval failed, log a warning but continue with the process.
            if payment_status != "success":
                logger.warning(f"Failed to retrieve payment data for account {self.account_num}")

            # Step 3: Format customer and payment data into a JSON object
            json_output = self.format_json_object()
            
            # Log the final JSON payload that will be sent to the API for debugging purposes.
            logger.debug(f"Final JSON payload: {json_output}")
            
            # Print the final JSON payload to the console (optional for debugging).
            print("\nJSON Payload to be sent to API:")
            print(json_output)

            # Step 4: Read the API URL from configuration file
            api_url = read_api_config()
            
            # If the API URL is empty, raise an APIConfigError to stop the process.
            if not api_url:
                raise APIConfigError("Empty API URL in config")

            # Step 5: Send the JSON payload to the API via a POST request
            api_response = self.send_to_api(json_output, api_url)
            
            # If the API response is empty or not successful, raise an IncidentCreationError.
            if not api_response:
                raise IncidentCreationError("Empty API response")
            
            # Log the successful API response for confirmation.
            logger.info(f"API Success: {api_response}")
            
            # Return True and the API response if the incident is successfully processed.
            return True, api_response

        # Custom exception for failure in incident creation, log the error and return False with the error message.
        except IncidentCreationError as e:
            logger.error(f"Incident processing failed: {e}")
            return False, str(e)

        # Custom exception for failure in reading API config, log the error and return False with the error message.
        except APIConfigError as e:
            logger.error(f"Configuration error: {e}")
            return False, str(e)

        # General exception handling for any unforeseen errors during the process.
        # Logs the error with full traceback and returns False with the error message.
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return False, str(e)
