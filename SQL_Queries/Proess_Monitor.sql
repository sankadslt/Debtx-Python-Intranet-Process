-- Table 1: process_monitor_log
CREATE TABLE process_monitor_log (
    Monitor_Id               INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
    created_dtm              DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    case_id                  VARCHAR(100),
    Request_Id               VARCHAR(100),
    last_monitored_dtm       DATETIME,
    next_monitor_dtm         DATETIME,
    Order_Id                 VARCHAR (100) NOT NULL,
    account_num              VARCHAR (10) NOT NULL,
    Expire_Dtm               DATETIME,
    monitor_status           VARCHAR(50)  DEFAULT 'Open',
    monitor_status_dtm       DATETIME,
    monitor_status_description VARCHAR(255)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table 2: process_monitor_progress_log
CREATE TABLE process_monitor_progress_log (
    Monitor_Id               INT,
    created_dtm              DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    case_id                  VARCHAR(100),
    Request_Id               VARCHAR(100),
    last_monitored_dtm       DATETIME,
    next_monitor_dtm         DATETIME,
    Order_Id                 VARCHAR(100),
    account_num              VARCHAR(10),
    Expire_Dtm               DATETIME,
    monitor_status           VARCHAR(50),
    monitor_status_dtm       DATETIME,
    monitor_status_description VARCHAR(255)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table 3: process_monitor_details
CREATE TABLE process_monitor_details (
    Monitor_Id INT PRIMARY KEY NOT NULL,
    para_1 VARCHAR(255),
    para_2 VARCHAR(255),
    para_3 VARCHAR(255),
    para_4 VARCHAR(255),
    para_5 VARCHAR(255),
    para_6 VARCHAR(255),
    para_7 VARCHAR(255),
    para_8 VARCHAR(255),
    para_9 VARCHAR(255),
    para_10 VARCHAR(255),
    FOREIGN KEY (Monitor_Id) REFERENCES process_monitor_log(Monitor_Id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
