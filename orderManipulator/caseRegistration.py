import json
from datetime import datetime, date
from decimal import Decimal
import requests
import pymysql
from pymongo import MongoClient
from utils.database.connectSQL import get_mysql_connection
from utils.logger.logger import get_logger
from utils.api.connectAPI import read_api_config
from utils.custom_exceptions.customize_exceptions import APIConfigError, IncidentCreationError

logger = get_logger("task_status_logger")
if len(logger.handlers) > 1:
    logger.handlers = [logger.handlers[0]]
logger.propagate = False

class IncidentProcessor:
    
    def __init__(self, account_num, incident_id, mongo_collection):
        self.account_num = str(account_num)
        self.incident_id = int(incident_id)
        self.collection = mongo_collection
        self.mongo_data = self.initialize_mongo_doc()

    def get_config_from_mongo(self):
        try:
            config_doc = self.collection.find_one({"account_num": self.account_num})
            if config_doc:
                logger.info(f"Fetched config document from MongoDB for account {self.account_num}")
                return config_doc
            logger.warning(f"No config document found in MongoDB for account {self.account_num}")
            return None
        except Exception as e:
            logger.error(f"Error fetching config document from MongoDB for account {self.account_num}: {e}")
            return None

    def get_doc_version_from_mongo(self, config_doc):
        if config_doc and "doc_version" in config_doc:
            try:
                return float(config_doc["doc_version"])
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid doc_version value. Using default 1.0. Error: {e}")
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
                "customer_ref": "",  # Added new field
                "Email_Address": ""   # Added new field
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
            "Last_Actions": [],  # Changed to empty array
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
            "Source_Type": ""
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
            cursor.execute(f"SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = '{self.account_num}'")
            rows = cursor.fetchall()

            seen_products = set()
            seen_contacts = set()

            for row in rows:
                load_date = row.get("LOAD_DATE")
                load_date_str = self.format_date(load_date)

                # Process contacts
                self.process_contacts(row, seen_contacts, load_date_str)

                # Populate Customer_Details if empty
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
                        "customer_ref": row.get("CUSTOMER_REF", ""),  # Added
                        "Email_Address": str(row.get("EMAIL", ""))    # Added
                    }

                    # Process account effective date
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
                    
                    # Last actions from customer table
                    if row.get("LAST_PAYMENT_DAT"):
                        self.mongo_data["Last_Actions"] = [{
                            "Billed_Seq": int(row.get("LAST_BILL_SEQ", 0)),
                            "Billed_Created": row.get("LAST_BILL_DTM", "1900-01-01T00:00:00.000Z"),
                            "Payment_Seq": 0,
                            "Payment_Created": row.get("LAST_PAYMENT_DAT", "1900-01-01T00:00:00.000Z"),
                            "Payment_Money": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else 0.0,
                            "Billed_Amount": float(row["LAST_PAYMENT_MNY"]) if row.get("LAST_PAYMENT_MNY") else 0.0
                        }]

                # Process product details
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
            logger.error(f"Error reading customer details: {e}")
            return "error"
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

    def format_date(self, date_value):
        """Helper method to format dates consistently"""
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
        """Helper method to process contact details"""
        # Email contact
        email = row.get("TECNICAL_CONTACT_EMAIL", "")
        if "@" in email:
            contact_key = ("email", email)
            if contact_key not in seen_contacts:
                seen_contacts.add(contact_key)
                self.mongo_data["Contact_Details"].append({
                    "Contact_Type": "email",
                    "Contact": email,
                    "Create_Dtm": load_date_str,
                    "Create_By": "drs_admin"
                })

        # Mobile contact
        mobile = row.get("MOBILE_CONTACT", "")
        if mobile:
            contact_key = ("mobile", mobile)
            if contact_key not in seen_contacts:
                seen_contacts.add(contact_key)
                self.mongo_data["Contact_Details"].append({
                    "Contact_Type": "mobile",
                    "Contact": mobile,
                    "Create_Dtm": load_date_str,
                    "Create_By": "drs_admin"
                })

        # Work contact
        work = row.get("WORK_CONTACT", "")
        if work:
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
            cursor.execute(
                f"SELECT * FROM debt_payment WHERE AP_ACCOUNT_NUMBER = '{self.account_num}' "
                "ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1"
            )
            payment_rows = cursor.fetchall()

            if payment_rows:
                payment = payment_rows[0]
                payment_date = self.format_date(payment.get("ACCOUNT_PAYMENT_DAT"))
                
                # Replace existing Last_Actions with this single payment record
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
        """Convert the MongoDB document structure to properly formatted JSON"""
        json_data = json.loads(json.dumps(self.mongo_data, default=self.json_serializer()))
        # Ensure string types for specific fields
        if "Customer_Details" in json_data:
            json_data["Customer_Details"]["Nic"] = str(json_data["Customer_Details"].get("Nic", ""))
        if "Account_Details" in json_data:
            json_data["Account_Details"]["Email_Address"] = str(json_data["Account_Details"].get("Email_Address", ""))
        return json.dumps(json_data, indent=4)

    def json_serializer(self):
        """Custom JSON serializer for non-native types"""
        def _serialize(obj):
            if isinstance(obj, (datetime, date)):
                if isinstance(obj, date) and not isinstance(obj, datetime):
                    obj = datetime.combine(obj, datetime.min.time())
                return obj.replace(microsecond=0).isoformat() + ".000Z"
            if isinstance(obj, Decimal):
                return float(obj) if obj % 1 else int(obj)
            if obj is None or str(obj).lower() == "none":
                return ""  # Always return empty string for None/"None" values
            if isinstance(obj, (int, float)):
                return obj
            return str(obj)
        return _serialize

    def send_to_api(self, json_output, api_url):
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
        try:
            logger.info(f"Processing incident for account: {self.account_num}, ID: {self.incident_id}")
            
            # Step 1: Read customer details
            customer_status = self.read_customer_details()
            if customer_status != "success" or not self.mongo_data["Customer_Details"]:
                error_msg = f"No customer details found for account {self.account_num}"
                logger.error(error_msg)
                return False, error_msg
            
            # Step 2: Get payment data (optional)
            payment_status = self.get_payment_data()
            if payment_status != "success":
                logger.warning(f"Failed to retrieve payment data for account {self.account_num}")
                
            # Step 3: Format as JSON
            json_output = self.format_json_object()
            logger.debug(f"Final JSON payload: {json_output}")
            
            # Step 4: Get API URL and send data
            api_url = read_api_config()
            if not api_url:
                raise APIConfigError("Empty API URL in config")
                    
            api_response = self.send_to_api(json_output, api_url)
            if not api_response:
                raise IncidentCreationError("Empty API response")
                    
            logger.info(f"API Success: {api_response}")
            return True, api_response
                
        except IncidentCreationError as e:
            logger.error(f"Incident processing failed: {e}")
            return False, str(e)
        except APIConfigError as e:
            logger.error(f"Configuration error: {e}")
            return False, str(e)
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return False, str(e)