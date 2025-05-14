# Time-Gapped Incident Processing System

## Overview

The **Time-Gapped Incident Processing System** is designed to process incident data incrementally using a time-windowed approach. It ensures no duplicate or missed records by leveraging MongoDB for state tracking and MySQL for data storage. The system also integrates with an external API to send processed data.

### Key Features:
1. **Time-Gapped Processing**:
   - Processes data incrementally between `Last_execution_dtm` and `(Current Time - 1 minute)`.
   - Ensures no duplicates or missed records.

2. **MongoDB State Tracking**:
   - Tracks the last processed timestamp (`Last_execution_dtm`) and sequence (`Process_Operation_Sequence`).
   - Maintains an audit trail of processing runs.

3. **MySQL Integration**:
   - Fetches customer and payment data from MySQL tables (`debt_cust_detail` and `debt_payment`).

4. **API Integration**:
   - Sends processed data to an external API.
   - Handles empty data scenarios by sending an empty JSON payload.

5. **Custom Exception Handling**:
   - Handles database connection errors, API configuration issues, and data processing errors gracefully.

---

## Folder Structure

```
incident_process/
│
├── incident_create_copy.py          # Core processing logic
├── utils/
│   ├── database/
│   │   ├── connectMongo.py          # MongoDB connection logic
│   │   ├── connectSQL.py            # MySQL connection logic
│   ├── custom_exceptions/
│   │   ├── customize_exceptions.py  # Custom exception definitions
│   ├── logger/
│   │   ├── logger.py                # Logger configuration
│   ├── filePath/
│   │   ├── filePath.py              # Utility to fetch file paths
├── config/
│   ├── databaseConfig.ini           # Configuration file for database connections
└── README.md                        # Project documentation
```

---

## Setup Instructions

### Prerequisites:
1. **Python**: Ensure Python 3.6+ is installed.
2. **MongoDB**: Install and configure MongoDB.
3. **MySQL**: Install and configure MySQL.
4. **Dependencies**: Install required Python libraries:
   ```bash
   pip install pymysql pymongo requests
   ```

### Configuration:
1. **Database Configuration**:
   - Update `config/databaseConfig.ini` with your MongoDB and MySQL credentials.
   - Example:
     ```ini
     [MONGODB]
     MONGO_URL = mongodb://localhost:27017/
     DATABASE_NAME = drc_incident

     [DATABASE]
     MYSQL_HOST = localhost
     MYSQL_DATABASE = incident_db
     MYSQL_USER = root
     MYSQL_PASSWORD = password
     ```

2. **API Configuration**:
   - Ensure the API endpoint is configured in the application.

---

## Usage

### Running the System:
1. **Run the Main Script**:
   ```bash
   python incident_create_copy.py
   ```

2. **Logs**:
   - Logs are generated for each step of the process, including:
     - Time window processing.
     - Data fetching from MySQL.
     - JSON payload generation.
     - API calls.
     - MongoDB updates.

### Example Workflow:
#### Step 1: First Run
- **Time Window**: `1900-01-01T00:00:00Z` to `(Current Time - 1 minute)`.
- **Action**: Processes all historical data and updates MongoDB.

#### Step 2: Incremental Run
- **Time Window**: `Last_execution_dtm` to `(Current Time - 1 minute)`.
- **Action**: Processes only new data since the last run.

#### Step 3: No New Data
- **Time Window**: `Last_execution_dtm` to `(Current Time - 1 minute)`.
- **Action**: Logs "No new data in the time window" and skips processing.

---

## Error Handling

### Custom Exceptions:
1. **APIConfigError**:
   - Raised when there is an issue with the API configuration.

2. **IncidentCreationError**:
   - Raised when there is an issue during the incident creation process.

3. **DatabaseConnectionError**:
   - Raised when there is an issue connecting to MongoDB or MySQL.

4. **DataProcessingError**:
   - Raised when there is an issue processing data.

### Logs:
- All errors are logged with detailed messages for debugging.

---

## Example Logs

### Successful Run:
```
INFO:incident_logger:Processing time window: 2025-02-12 to 2025-04-04T02:15:44.313220
INFO:incident_logger:Fetched 3 records from debt_cust_detail.
INFO:incident_logger:Fetched 1 record from debt_payment.
INFO:incident_logger:Generated JSON payload: {...}
INFO:incident_logger:Sending data to API: http://example.com/api
INFO:incident_logger:Successfully sent data to API.
INFO:incident_logger:Updated MongoDB with newest timestamp: 2025-04-04T02:15:00Z
```

### No New Data:
```
INFO:incident_logger:Processing time window: 2025-04-04T02:15:00Z to 2025-04-04T02:30:00Z
INFO:incident_logger:No new data in the time window.
```

### Error Example:
```
ERROR:incident_logger:Database connection error during incident processing: Failed to connect to MySQL.
```

---

## Key Takeaways

1. **Time-Gapped Processing**:
   - Ensures accurate, incremental processing while avoiding duplicates.

2. **MongoDB State Tracking**:
   - Maintains a clear audit trail of processing runs.

3. **Error Handling**:
   - Gracefully handles errors and logs detailed messages for debugging.

4. **Scalability**:
   - Designed to handle large datasets and incremental processing efficiently.

---

## Contributing

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Submit a pull request with a detailed description of your changes.

---

## Developed By
[Tharindu Balasooriya] (tharindutsb@gmail.com) 

