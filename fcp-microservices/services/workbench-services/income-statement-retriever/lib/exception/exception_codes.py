from enum import Enum

status_prefix = "MSG_FINCO_IncomeStatement_"

class Status(Enum):
    '''
    The status of the exception. API level errors
    '''
    EXTERNAL_ERROR = {"status": status_prefix + "EXTERNAL_ERROR", "status_code": 500}        # Dependent API errors
    INPUT_ERROR = {"status": status_prefix + "INPUT_ERROR", "status_code": 400}              # Client API call error
    INTERNAL_ERROR = {"status": status_prefix + "INTERNAL_ERROR", "status_code": 500}        # Other internal errors
    UNKNOWN_EXCEPTION = {"status": status_prefix + "UNKNOWN_EXCEPTION", "status_code": 500}  # Unknown errors


class Reason(Enum):
    '''
    The reason for the exception. Specific to each exception
    '''

    # Input errors
    INVALID_PARAMETER = {
        "reason": "INVALID_PARAMETER",
        "status_code": 412,         # Specific status_code if needed. If left blank it will take the default from status
        "status": Status.INPUT_ERROR
    }
    MISSING_KEY = {
        "reason": "MISSING_KEY",
        "status_code": 412,         
        "status": Status.INPUT_ERROR
    }
    INVALID_INPUT = {
        "reason": "INVALID_INPUT",
        "status_code": 412,  
        "status": Status.INPUT_ERROR
    }

    # Internal errors
    CONFIG_ERROR = {
        "reason": "CONFIG_ERROR",
        "status": Status.INTERNAL_ERROR
    }
    CONFIG_SECTION_MISSING = {
        "reason": "CONFIG_SECTION_MISSING",
        "status": Status.INTERNAL_ERROR
    }
    CONFIG_KEY_MISSING = {
        "reason": "CONFIG_KEY_MISSING",
        "status": Status.INTERNAL_ERROR
    }
    SQL_FILE_NOT_FOUND = {
        "reason": "SQL_FILE_NOT_FOUND",
        "status": Status.INTERNAL_ERROR
    }
    FILE_READ_ERROR = {
        "reason": "FILE_READ_ERROR",
        "status": Status.INTERNAL_ERROR
    }
    INVALID_SQL_QUERY = {
        "reason": "INVALID_SQL_QUERY",
        "status": Status.INTERNAL_ERROR
    }
    KEY_DESERIALIZATION_ERROR = {
        "reason": "KEY_DESERIALIZATION_ERROR",
        "status": Status.INTERNAL_ERROR
    }

    # External errors (APIs)
    DATABASE_CONNECTION_ERROR = { 
        "reason": "DATABASE_CONNECTION_ERROR",
        "status_code": 504,
        "status": Status.EXTERNAL_ERROR
    }
    NO_DATA_FOUND = {
        "reason": "NO_DATA_FOUND",
        "status_code": 200,
        "status": Status.EXTERNAL_ERROR
    }
    DATABASE_EXECUTION_ERROR = {
        "reason": "DATABASE_EXECUTION_ERROR",
        "status_code": 503,
        "status": Status.EXTERNAL_ERROR
    }
    SECRETS_MANAGER_ERROR = {
        "reason": "SECRETS_MANAGER_ERROR",
        "status_code": 502,
        "status": Status.EXTERNAL_ERROR
    }

    # Unknown errors
    UNEXPECTED_ERROR = {
        "reason": "UNEXPECTED_ERROR",
        "status": Status.UNKNOWN_EXCEPTION
    }
