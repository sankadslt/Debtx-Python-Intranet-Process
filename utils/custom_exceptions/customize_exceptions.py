class APIConfigError(Exception):
    """
    Raised when there is an issue with the API configuration.
    """
    def __init__(self, message="API configuration error"):
        self.message = message
        super().__init__(self.message)


class IncidentCreationError(Exception):
    """
    Raised when there is an issue during the incident creation process.
    """
    def __init__(self, message="Incident creation error"):
        self.message = message
        super().__init__(self.message)


class DatabaseConnectionError(Exception):
    """
    Raised when there is an issue connecting to the database.
    """
    def __init__(self, message="Database connection error"):
        self.message = message
        super().__init__(self.message)


class DataProcessingError(Exception):
    """
    Raised when there is an issue processing data.
    """
    def __init__(self, message="Data processing error"):
        self.message = message
        super().__init__(self.message)

