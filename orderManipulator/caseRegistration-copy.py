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

class IncidentProcessor:
    
    def __init__(self, account_num, incident_id, mongo_collection):
        """
        Initialize with account number, incident ID, and MongoDB collection
        """
        self.account_num = str(account_num)
        self.incident_id = int(incident_id)
        self.collection = mongo_collection
        self.mongo_data = self.initialize_mongo_doc()

    def initialize_mongo_doc(self):
        """
        Creates and initializes a standardized MongoDB document structure for incident.
        """
        now = datetime.now().isoformat()
        return {
            "Doc_Version": "1.0",
            "Incident_Id": self.incident_id,
            "Account_Num": self.account_num,
            "Case_ID" : "",
            "Customer_Name": "",
            "Customer_Ref": "",
            "Area": "",
            "BSS_Arrears_amount": 0,
            "Current_Arrears_Amount": 0,
            "Action type": "",
            "Filtered_Reason": "",
            "Last_Payment_Date": "",
            "Last_BSS_Reading_Date": "",
            "Case_Current_Status": "",
            "Current_Arrears_band": "",
            "DRC_Commission_Rule": "",
            "Created_Dtm": now,
            "Implemented_Dtm": now,
            "RTOM" : "",
            "Monitor_Months": "",
            "Contact_Details": [],
            "Product_Details": [],
            "Customer_Details": {},
            "Account_Details": {},
            "Ref_Data-temp,Permanent": [],
            "Case_Status": [],
            "Remark": [],
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

    def read_customer_details(self):
        """Retrieves and processes customer account data from MySQL"""
        mysql_conn = None
        cursor = None
        try:
            logger.info(f"Reading customer details for account number: {self.account_num}")
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                logger.error("MySQL connection failed. Skipping customer details retrieval.")
                return "error"
            
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = '{self.account_num}'")
            rows = cursor.fetchall()

            seen_products = set()

            for row in rows:
                if not self.mongo_data["Customer_Details"]:
                    if row.get("TECNICAL_CONTACT_EMAIL"):
                        contact_details_element = {
                            "Contact_Type": "email",
                            "Contact": row["TECNICAL_CONTACT_EMAIL"] if "@" in row["TECNICAL_CONTACT_EMAIL"] else "",
                            "Create_Dtm": row["LOAD_DATE"].isoformat() if row.get("LOAD_DATE") else "",
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    if row.get("MOBILE_CONTACT"):
                        contact_details_element = {
                            "Contact_Type": "mobile",
                            "Contact": row["MOBILE_CONTACT"],
                            "Create_Dtm": row["LOAD_DATE"].isoformat() if row.get("LOAD_DATE") else "",
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    if row.get("WORK_CONTACT"):
                        contact_details_element = {
                            "Contact_Type": "fix",
                            "Contact": row["WORK_CONTACT"],
                            "Create_Dtm": row["LOAD_DATE"].isoformat() if row.get("LOAD_DATE") else "",
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
                        "Customer_Type_Id": str(row.get("CUSTOMER_TYPE_ID", "")),
                        "Customer_Type": row.get("CUSTOMER_TYPE", "")
                    }

                    self.mongo_data["Account_Details"] = {
                        "Account_Status": row.get("ACCOUNT_STATUS_BSS", ""),
                        "Acc_Effective_Dtm": row["ACCOUNT_EFFECTIVE_DTM_BSS"].isoformat() if row.get("ACCOUNT_EFFECTIVE_DTM_BSS") else "",
                        "Acc_Activate_Date": "1900-01-01T00:00:00",
                        "Credit_Class_Id": str(row.get("CREDIT_CLASS_ID", "")),
                        "Credit_Class_Name": row.get("CREDIT_CLASS_NAME", ""),
                        "Billing_Centre": row.get("BILLING_CENTER_NAME", ""),
                        "Customer_Segment": row.get("CUSTOMER_SEGMENT_ID", ""),
                        "Mobile_Contact_Tel": "",
                        "Daytime_Contact_Tel": "",
                        "Email_Address": str(row.get("EMAIL", "")),
                        "Last_Rated_Dtm": "1900-01-01T00:00:00"
                    }

                product_id = row.get("ASSET_ID")
                if product_id and product_id not in seen_products:
                    seen_products.add(product_id)
                    self.mongo_data["Product_Details"].append({
                        "Product_Label": row.get("PROMOTION_INTEG_ID", ""),
                        "Customer_Ref": row.get("CUSTOMER_REF", ""),
                        "Product_Seq": str(row.get("BSS_PRODUCT_SEQ", "")),
                        "Equipment_Ownership": "",
                        "Product_Id": product_id,
                        "Product_Name": row.get("PRODUCT_NAME", ""),
                        "Product_Status": row.get("ASSET_STATUS", ""),
                        "Effective_Dtm": row["ACCOUNT_EFFECTIVE_DTM_BSS"].isoformat() if row.get("ACCOUNT_EFFECTIVE_DTM_BSS") else "",
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
            logger.error(f"Error : {e}")
            return "error"
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

    def get_payment_data(self):
        """Retrieves and processes the most recent payment record for the account from MySQL."""
        mysql_conn = None
        cursor = None
        try:
            logger.info(f"Getting payment data for account number: {self.account_num}")
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                logger.error("MySQL connection failed. Skipping payment data retrieval.")
                return "failure"
            
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(
                f"SELECT * FROM debt_payment WHERE AP_ACCOUNT_NUMBER = '{self.account_num}' "
                "ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1"
            )
            payment_rows = cursor.fetchall()

            if payment_rows:
                payment = payment_rows[0]
                self.mongo_data["Last_Actions"].update({
                    "Payment_Seq": str(payment.get("ACCOUNT_PAYMENT_SEQ", "")),
                    "Payment_Created": payment["ACCOUNT_PAYMENT_DAT"].isoformat() if payment.get("ACCOUNT_PAYMENT_DAT") else "",
                    "Payment_Money": str(float(payment["AP_ACCOUNT_PAYMENT_MNY"])) if payment.get("AP_ACCOUNT_PAYMENT_MNY") else "0",
                    "Billed_Amount": str(float(payment["AP_ACCOUNT_PAYMENT_MNY"])) if payment.get("AP_ACCOUNT_PAYMENT_MNY") else "0"
                })
                logger.info("Successfully retrieved payment data.")
                return "success"
            return "failure"

        except Exception as e:
            logger.error(f"Error : {e}")
            return "failure"
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

    def format_json_object(self):
        """Transforms the incident data into a well-formatted JSON string with type consistency."""
        json_data = json.loads(json.dumps(self.mongo_data, default=self.json_serializer))
        
        json_data["Customer_Details"]["Nic"] = str(json_data["Customer_Details"].get("Nic", ""))
        json_data["Account_Details"]["Email_Address"] = str(json_data["Account_Details"].get("Email_Address", ""))
        
        return json.dumps(json_data, indent=4)

    def json_serializer(self, obj):
        """Custom JSON serializer that handles non-native JSON types in Python objects."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if obj is None:
            return ""
        raise TypeError(f"Type {type(obj)} not serializable")

    def send_to_api(self, json_output, api_url):
        """Sends JSON data to a specified API endpoint via HTTP POST request."""
        logger.info(f"Sending data to API: {api_url}")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            response = requests.post(api_url, data=json_output, headers=headers)
            response.raise_for_status()
            logger.info("Successfully sent data to API.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending data to API: {e}")
            return None

    def process_incident(self):
        """
        Processes a debt collection incident by gathering customer/payment data and sending to API.
        
        Returns:
            tuple: 
                - (True, api_response) if incident was successfully created and sent to API
                - (False, error_message) if failed
        """
        try:
            logger.info(f"Processing incident for account: {self.account_num}, ID: {self.incident_id}")
            
            customer_status = self.read_customer_details()
            if customer_status != "success" or not self.mongo_data["Customer_Details"]:
                error_msg = f"No customer details found for account {self.account_num}"
                logger.error(error_msg)
                return False, error_msg
            
            payment_status = self.get_payment_data()
            if payment_status != "success":
                logger.warning(f"Failed to retrieve payment data for account {self.account_num}")
                
            json_output = self.format_json_object()
            print(json_output)
            
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