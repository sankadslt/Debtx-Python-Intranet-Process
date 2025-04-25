# Importing the `json` library to handle JSON encoding and decoding
import json
# Importing `datetime` and `date` to work with date and time objects
from datetime import datetime, date
# Importing `Decimal` from `decimal` to handle high-precision arithmetic, especially useful for financial data
from decimal import Decimal
# Importing `requests` for making HTTP requests to external APIs
import requests
# Importing `pymysql` for interacting with MySQL databases
import pymysql
# Importing `MongoClient` from `pymongo` to interact with MongoDB databases
from pymongo import MongoClient
# Importing a utility function for getting MySQL database connections
from utils.database.connectSQL import get_mysql_connection
# Importing a logger utility to handle logging throughout the application
from utils.logger.logger import get_logger
# Importing a function to read API configuration details from an external source
from utils.api.connectAPI import read_api_config
# Importing custom exception classes for error handling related to API configuration and incident creation
from utils.custom_exceptions.customize_exceptions import APIConfigError, IncidentCreationError


logger = get_logger("task_status_logger")
if len(logger.handlers) > 1:
    logger.handlers = [logger.handlers[0]]
logger.propagate = False

class Process_Incident:
    
    def __init__(self, account_num, incident_id, mongo_collection):
        """
        Initializes an instance of the class with account number, incident ID, 
        and MongoDB collection. It also initializes the MongoDB document associated 
        with the account and incident.

        Inputs:
            account_num (str): The account number associated with the incident. 
                                    It will be converted to a string for consistency.
            incident_id (int): The unique incident ID. This will be converted to an integer.
            mongo_collection (pymongo.collection.Collection): The MongoDB collection object 
                                                            used for querying or updating documents.

        Outputs:
            None

        Returns:
            None

        Description:
            The constructor initializes the following:
            1. The `account_num` is converted to a string.
            2. The `incident_id` is converted to an integer.
            3. The provided MongoDB collection is assigned to `self.collection`.
            4. The `initialize_mongo_doc()` method is called to initialize the associated document 
            from the MongoDB collection and store it in `self.mongo_data`.
        """
        # Convert account_num to string and store it
        self.account_num = str(account_num)

        # Convert incident_id to integer and store it
        self.incident_id = int(incident_id)

        # Store the provided MongoDB collection for future use
        self.collection = mongo_collection

        # Initialize the MongoDB document associated with the account and incident
        self.mongo_data = self.initialize_mongo_doc()


    def get_config_from_mongo(self):
        """
        Fetches the configuration document from the MongoDB collection for the specified account number.

        Inputs:
            None

        Outputs:
            None

        Returns:
            dict or None: 
                - The configuration document (a dictionary) fetched from MongoDB if found.
                - None if no document is found or an error occurs during the process.

        Description:
            This method queries the MongoDB collection to find the configuration document
            associated with the account number stored in the instance. If a document is found, 
            it is returned. If no document is found or an error occurs, a corresponding log 
            message is generated, and `None` is returned.
        """
        try:
            # Query the MongoDB collection for a document with the specific account number
            config_doc = self.collection.find_one({"account_num": self.account_num})
            
            # If a document is found, log the success and return the document
            if config_doc:
                logger.info(f"Fetched config document from MongoDB for account {self.account_num}")
                return config_doc
            
            # If no document is found, log a warning and return None
            logger.warning(f"No config document found in MongoDB for account {self.account_num}")
            return None
        
        except Exception as e:
            # Log any exceptions that occur during the query process
            logger.error(f"Error fetching config document from MongoDB for account {self.account_num}: {e}")
            return None


    def get_doc_version_from_mongo(self, config_doc):
        """
        Extracts the 'doc_version' from the provided MongoDB configuration document.

        Inputs:
            config_doc (dict): The configuration document fetched from MongoDB.

        Outputs:
            None

        Returns:
            float: The document version extracted from the config document.
                Returns 1.0 if no valid version is found or if there is an error.

        Description:
            This method attempts to retrieve the 'doc_version' field from the given configuration document.
            - If the field is present and its value can be converted to a float, it returns the version.
            - If the field is missing or the value cannot be converted to a float, it logs a warning and returns the default version of 1.0.
        """
        if config_doc and "doc_version" in config_doc:
            try:
                # Attempt to convert the 'doc_version' value to a float
                return float(config_doc["doc_version"])
            except (ValueError, TypeError) as e:
                # If an error occurs during conversion, log a warning and return default version 1.0
                logger.warning(f"Invalid doc_version value. Using default 1.0. Error: {e}")
        
        # Return the default version if 'doc_version' is not found or if there's an error
        return 1.0


    def initialize_mongo_doc(self):
        now = datetime.now().replace(microsecond=0).isoformat() + ".000Z"
        config_doc = self.get_config_from_mongo()
        doc_version = self.get_doc_version_from_mongo(config_doc)
        
        return {
            "Doc_Version": doc_version,
            "Incident_Id": self.incident_id,
            "Account_Num": self.account_num,
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
            "Accounts_Details": [{
                "Id": 0,
                "DRCMood": "",
                "Account_Num": self.account_num,
                "Account_Status": "",
                "OutstandingBalance": 0
            }],
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
                "customer_ref": "",
                "Email_Address": ""
            },
            "Account_Details": {
                "Account_Status": "",
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
            "Commission - Bill Payment":[],
            "Bonus": [],
            "FTL LOD": [],
            "Litigation": [],
            "LOD / Final Reminder": [],
            "Dispute" : [],
            "Abnormal Stop" : []   
        }

    def read_customer_details(self):
        mysql_conn = None
        cursor = None
        try:
            logger.info(f"Reading customer details for account: {self.account_num}")
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                logger.error("MySQL connection failed")
                return "error"
            
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            query = "SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = %s"
            logger.debug(f"Executing query: {query % self.account_num}")
            cursor.execute(query, (self.account_num,))
            rows = cursor.fetchall()

            if rows is None:
                logger.error(f"Query returned None for account {self.account_num}. Possible query execution failure.")
                return "error"

            if not rows:
                logger.warning(f"No customer data found for account {self.account_num}")
                return "no_data"

            seen_products = set()
            seen_contacts = set()

            for row in rows:
                load_date = row.get("LOAD_DATE")
                load_date_str = self.format_date(load_date)

                self.process_contacts(row, seen_contacts, load_date_str)

                if not self.mongo_data["Customer_Details"].get("Customer_Name"):
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
                        "customer_ref": row.get("CUSTOMER_REF", ""),
                        "Email_Address": str(row.get("EMAIL", ""))
                    }

                    acc_eff_dtm = self.format_date(row.get("ACCOUNT_EFFECTIVE_DTM_BSS"))
                    self.mongo_data["Account_Details"] = {
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
                    
                    if row.get("LAST_PAYMENT_DAT"):
                        self.mongo_data["Last_Actions"] = [{
                            "Billed_Seq": int(row.get("LAST_BILL_SEQ", 0)),
                            "Billed_Created": row.get("LAST_BILL_DTM", "1900-01-01T00:00:00.000Z"),
                            "Payment_Seq": 0,
                            "Payment_Created": row.get("LAST_PAYMENT_DAT", "1900-01-01T00:00:00.000Z"),
                            "Payment_Money": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else 0.0,
                            "Billed_Amount": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else 0.0
                        }]

                product_id = row.get("ASSET_ID")
                if product_id and product_id not in seen_products:
                    seen_products.add(product_id)
                    eff_dtm = self.format_date(row.get("ACCOUNT_EFFECTIVE_DTM_BSS"))
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

            return "success"

        except Exception as e:
            logger.error(f"Error reading customer details: {e}", exc_info=True)
            return "error"
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

    def format_date(self, date_value):
        if not date_value:
            return "1900-01-01T00:00:00.000Z"
        
        if isinstance(date_value, str):
            try:
                date_value = datetime.strptime(date_value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return "1900-01-01T00:00:00.000Z"
        elif isinstance(date_value, date) and not isinstance(date_value, datetime):
            date_value = datetime.combine(date_value, datetime.min.time())
        
        return date_value.replace(microsecond=0).isoformat() + ".000Z"

    def process_contacts(self, row, seen_contacts, load_date_str):
        logger.debug(f"Processing contacts for row: {row}")
        
        email = row.get("TECNICAL_CONTACT_EMAIL", "")
        if email is not None and "@" in email:
            contact_key = ("email", email)
            if contact_key not in seen_contacts:
                seen_contacts.add(contact_key)
                self.mongo_data["Contact_Details"].append({
                    "Contact_Type": "email",
                    "Contact": email,
                    "Create_Dtm": load_date_str,
                    "Create_By": "drs_admin"
                })

        mobile = row.get(" manganese", "")
        if mobile is not None and mobile:
            contact_key = ("mobile", mobile)
            if contact_key not in seen_contacts:
                seen_contacts.add(contact_key)
                self.mongo_data["Contact_Details"].append({
                    "Contact_Type": "mobile",
                    "Contact": mobile,
                    "Create_Dtm": load_date_str,
                    "Create_By": "drs_admin"
                })

        work = row.get("WORK_CONTACT", "")
        if work is not None and work:
            contact_key = ("fix", work)
            if contact_key not in seen_contacts:
                seen_contacts.add(contact_key)
                self.mongo_data["Contact_Details"].append({
                    "Contact_Type": "fix",
                    "Contact": work,
                    "Create_Dtm": load_date_str,
                    "Create_By": "drs_admin"
                })

    def get_payment_data(self):
        mysql_conn = None
        cursor = None
        try:
            logger.info(f"Getting payment data for account: {self.account_num}")
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                logger.error("MySQL connection failed")
                return "failure"
            
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            query = "SELECT * FROM debt_payment WHERE AP_ACCOUNT_NUMBER = %s ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1"
            logger.debug(f"Executing query: {query % self.account_num}")
            cursor.execute(query, (self.account_num,))
            payment_rows = cursor.fetchall()

            if payment_rows:
                payment = payment_rows[0]
                payment_date = self.format_date(payment.get("ACCOUNT_PAYMENT_DAT"))
                
                self.mongo_data["Last_Actions"] = [{
                    "Billed_Seq": int(payment.get("ACCOUNT_PAYMENT_SEQ", 0)),
                    "Billed_Created": payment_date,
                    "Payment_Seq": int(payment.get("ACCOUNT_PAYMENT_SEQ", 0)),
                    "Payment_Created": payment_date,
                    "Payment_Money": float(payment.get("AP_ACCOUNT_PAYMENT_MNY", 0)),
                    "Billed_Amount": float(payment.get("AP_ACCOUNT_PAYMENT_MNY", 0))
                }]
                return "success"
            return "failure"

        except Exception as e:
            logger.error(f"Error retrieving payment data: {e}")
            return "failure"
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

    def format_json_object(self):
        """
        Formats the MongoDB document into a properly serialized JSON object.

        Inputs:
            None

        Outputs:
            str: A formatted JSON string with specific fields properly serialized.

        Returns:
            str: A formatted and indented JSON string.

        Description:
            This method performs the following tasks:
            1. Serializes the `mongo_data` object into JSON using a custom serializer.
            2. Ensures specific fields in the JSON, such as "Nic" and "Email_Address", are converted to strings if they exist.
            3. Returns the serialized JSON with an indentation of 4 spaces for better readability.
        """
        # Serialize the mongo_data object into a JSON object using custom serialization if needed
        json_data = json.loads(json.dumps(self.mongo_data, default=self.json_serializer()))
        
        # Ensure "Nic" is a string if "Customer_Details" is present in the JSON
        if "Customer_Details" in json_data:
            json_data["Customer_Details"]["Nic"] = str(json_data["Customer_Details"].get("Nic", ""))
        
        # Ensure "Email_Address" is a string if "Account_Details" is present in the JSON
        if "Account_Details" in json_data:
            json_data["Account_Details"]["Email_Address"] = str(json_data["Account_Details"].get("Email_Address", ""))
        
        # Return the formatted JSON string with indentation for readability
        return json.dumps(json_data, indent=4)


    def json_serializer(self):
        """
        Returns a custom JSON serializer function to handle special cases for specific object types.

        Inputs:
            None

        Outputs:
            function: A custom serializer function that can be passed to `json.dumps()` for serializing complex objects.

        Returns:
            function: A function that handles the serialization of different types of objects (e.g., datetime, Decimal, None).

        Description:
            This method defines a custom serializer for specific data types that are not natively serializable by Python's `json` module:
            - Dates and datetime objects are serialized into ISO 8601 format with a `.000Z` suffix.
            - Decimal objects are converted into either a float or an integer depending on their value.
            - `None` and the string `"none"` are serialized as empty strings.
            - Other objects are converted to their string representation.
        """
        def _serialize(obj):
            # Handle datetime and date objects
            if isinstance(obj, (datetime, date)):
                if isinstance(obj, date) and not isinstance(obj, datetime):
                    # Convert date objects to datetime objects at midnight
                    obj = datetime.combine(obj, datetime.min.time())
                # Return ISO format with a fixed ".000Z" for UTC time representation
                return obj.replace(microsecond=0).isoformat() + ".000Z"
            
            # Handle Decimal objects by converting them to float or int
            if isinstance(obj, Decimal):
                return float(obj) if obj % 1 else int(obj)
            
            # Handle None values or the string "none" by returning an empty string
            if obj is None or str(obj).lower() == "none":
                return ""
            
            # Return other objects (int, float, etc.) as they are
            if isinstance(obj, (int, float)):
                return obj
            
            # For all other object types, convert them to a string
            return str(obj)
        
        # Return the custom serializer function
        return _serialize


    def send_to_api(self, json_output, api_url):
        """
        Sends data to a specified API endpoint via a POST request.

        Inputs:
            json_output (str): The JSON-formatted data to be sent to the API.
            api_url (str): The URL of the API endpoint to which the data will be sent.

        Outputs:
            dict or None:
                - Returns the JSON response from the API if the request is successful.
                - Returns None if an error occurs during the request.

        Returns:
            dict or None: The parsed JSON response from the API, or None if the request fails.

        Description:
            This method performs the following actions:
            1. Logs the API URL and attempts to send the JSON data via a POST request.
            2. Sets the `Content-Type` and `Accept` headers to `"application/json"`.
            3. Logs the response status and body.
            4. Raises an error if the response status code indicates a failure (non-2xx).
            5. Handles HTTP and general request exceptions by logging the error and returning `None`.
        """
        try:
            # Log the API URL and attempt to send the data
            logger.info(f"Sending data to API: {api_url}")
            
            # Set up the headers for the request
            headers = {
                "Content-Type": "application/json",  # The body of the request is JSON
                "Accept": "application/json"         # Expect a JSON response
            }
            
            # Send the POST request with the JSON data and headers
            response = requests.post(api_url, data=json_output, headers=headers)
            
            # Log the API response status and body for debugging
            logger.info(f"API Response: {response.status_code} - {response.text}")
            
            # Raise an exception for HTTP errors (non-2xx status codes)
            response.raise_for_status()
            
            # Return the JSON response if the request was successful
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            # Handle HTTP errors and log the status code and error message
            logger.error(f"HTTP Error: {e.response.status_code} - {e.response.text}")
            return None
        
        except requests.exceptions.RequestException as e:
            # Handle general request errors (network issues, timeout, etc.)
            logger.error(f"Request Error: {e}")
            return None


    def process_incident(self):
        """
        Processes an incident for a given account by retrieving customer details, payment data, 
        formatting the JSON payload, sending the data to an API, and handling potential errors.

        Inputs:
            None

        Outputs:
            tuple:
                - bool: Indicates whether the incident was processed successfully (`True`) or not (`False`).
                - str: A message providing additional details on the result (e.g., error message, success details).

        Returns:
            tuple: 
                - A boolean value indicating success (True) or failure (False).
                - A string with either the error message or the successful API response.

        Description:
            This method performs the following tasks:
            1. Reads customer details for the account using the `read_customer_details()` method.
            2. If the customer details are successfully retrieved, it proceeds to fetch payment data.
            3. Formats the data into a JSON object using the `format_json_object()` method.
            4. Retrieves the API URL from configuration using the `read_api_config()` function.
            5. Sends the data to the API using the `send_to_api()` method.
            6. Logs appropriate success or error messages throughout the process.
            7. Handles errors with custom exceptions (`IncidentCreationError`, `APIConfigError`) and general exceptions.
        """
        try:
            # Log the start of the incident processing for the given account and incident
            logger.info(f"Processing incident for account: {self.account_num}, ID: {self.incident_id}")
            
            # Step 1: Read customer details
            customer_status = self.read_customer_details()
            if customer_status == "error":
                # Error while reading customer details
                error_msg = f"Error reading customer details for account {self.account_num}"
                logger.error(error_msg)
                return False, error_msg
            elif customer_status == "no_data":
                # No customer data found
                error_msg = f"No customer data found for account {self.account_num}"
                logger.warning(error_msg)
                return False, error_msg
            
            # Step 2: Get payment data
            payment_status = self.get_payment_data()
            if payment_status != "success":
                # Warning when payment data retrieval fails
                logger.warning(f"Failed to retrieve payment data for account {self.account_num}")
            
            # Step 3: Format the data into a JSON object
            json_output = self.format_json_object()
            logger.debug(f"Final JSON payload: {json_output}")
            
            # Step 4: Get API URL from the configuration
            api_url = read_api_config()
            if not api_url:
                raise APIConfigError("Empty API URL in config")  # Raise error if the API URL is missing
            
            # Step 5: Send the JSON data to the API
            api_response = self.send_to_api(json_output, api_url)
            if not api_response:
                raise IncidentCreationError("Empty API response")  # Raise error if API response is empty
                
            # Step 6: Log success and return success result
            logger.info(f"API Success: {api_response}")
            return True, api_response
                    
        except IncidentCreationError as e:
            # Handle errors specific to incident creation
            logger.error(f"Incident processing failed: {e}")
            return False, str(e)
        except APIConfigError as e:
            # Handle errors related to API configuration
            logger.error(f"Configuration error: {e}")
            return False, str(e)
        except Exception as e:
            # Catch any unexpected errors and log the stack trace
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return False, str(e)
