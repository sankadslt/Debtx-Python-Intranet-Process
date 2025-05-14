from incident_process.incident_create import create_incident  # Note: Class name changed to PascalCase
from incident_process.process_operation import ProcessOperation  # Note: Class name changed to PascalCase

if __name__ == "__main__":
    # Define the account number and incident ID
    account_num = "0000003746"
    incident_id = 2265575134

    # Call ProcessOperation to handle the process with account_num and incident_id
    process_op = ProcessOperation(account_num, incident_id)
    process_op.run()
    process_op.close_connection()
