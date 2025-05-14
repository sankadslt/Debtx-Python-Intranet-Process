CREATE TABLE Process_Operation (
    Process_Operation_Sequence INT NOT NULL,
    created_dtm DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    Operation_name VARCHAR(255) NOT NULL,
    execution_duration INT NOT NULL,
    Next_execution_dtm DATETIME NOT NULL,
    Last_execution_dtm DATETIME NOT NULL,
    end_dtm DATETIME NULL
);

INSERT INTO Process_Operation (
    Process_Operation_Sequence,
    created_dtm,
    Operation_name,
    execution_duration,
    Next_execution_dtm,
    Last_execution_dtm,
    end_dtm
)
VALUES (
    1,
    '1900-01-01 00:00:00',
    'Incident extraction from data lake',
    60,
    '1900-01-01 01:00:00',
    '1900-01-01 00:00:00',
    NULL
);
