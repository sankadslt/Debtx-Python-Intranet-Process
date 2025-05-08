-- Table: request_log
CREATE TABLE request_log (
    Request_Id INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
    created_dtm DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    Order_Id INT NOT NULL,
    account_num VARCHAR(10) NOT NULL,
    case_id VARCHAR(100),
    Request_Status VARCHAR(50) DEFAULT 'Open',
    Request_Status_Description VARCHAR(255),
    Request_Status_Dtm DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: request_progress_log
CREATE TABLE request_progress_log (
    Request_Id INT,
    created_dtm DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    Order_Id INT,
    account_num VARCHAR(10),
    case_id VARCHAR(100),
    Request_Status VARCHAR(50) DEFAULT 'Open',
    Request_Status_Description VARCHAR(255),
    Request_Status_Dtm DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table: request_log_details
CREATE TABLE request_log_details (
    Request_Id INT PRIMARY KEY NOT NULL,
    para_1 VARCHAR2(255),
    para_2 VARCHAR2(255),
    para_3 VARCHAR2(255),
    para_4 VARCHAR2(255),
    para_5 VARCHAR2(255),
    para_6 VARCHAR2(255),
    para_7 VARCHAR2(255),
    para_8 VARCHAR2(255),
    para_9 VARCHAR2(255),
    para_10 VARCHAR2(255),
    FOREIGN KEY (Request_Id) REFERENCES request_log(Request_Id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;