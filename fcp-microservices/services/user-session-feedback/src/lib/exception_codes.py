from enum import Enum

status_prefix = "MSG_FINCO_USER_FEEDBACK_"

class Status(Enum):
    EXTERNAL_ERROR = {"status": status_prefix + "EXTERNAL_ERROR", "status_code": 500}        # Dependent API errors
    INPUT_ERROR = {"status": status_prefix + "INPUT_ERROR", "status_code": 400}              # Client API call error
    INTERNAL_ERROR = {"status": status_prefix + "INTERNAL_ERROR", "status_code": 500}        # Other internal errors
    DATABASE_ERROR = {"status": status_prefix + "DATABASE_ERROR", "status_code": 500}        # Database-related errors

    UNKNOWN_EXCEPTION = {"status": status_prefix + "UNKNOWN_EXCEPTION", "status_code": 500}  # Unknown exceptions


class Reason(Enum):

    # Input errors
    MISSING_REQUIRED_FIELD = {
        "reason": "MISSING_REQUIRED_FIELD",
        "status_code": 400,
        "status": Status.INPUT_ERROR
    }
    INVALID_UUID_FORMAT = {
        "reason": "INVALID_UUID_FORMAT",
        "status_code": 400,         
        "status": Status.INPUT_ERROR
    }
    
    INVALID_SESSION_ID = {
        "reason": "INVALID_SESSION_ID",
        "status_code": 400,         
        "status": Status.INPUT_ERROR
    }
    
    INVALID_INPUT_TYPE = {
        "reason": "INVALID_INPUT_TYPE",
        "status_code": 400,  
        "status": Status.INPUT_ERROR
    }

    INVALID_FEEDBACK_TYPE = {
        "reason": "INVALID_FEEDBACK_TYPE",
        "status_code": 400,  
        "status": Status.INPUT_ERROR
    }
    
    INVALID_FEEDBACK_SOURCE = {
        "reason": "INVALID_FEEDBACK_SOURCE",
        "status_code": 400,  
        "status": Status.INPUT_ERROR
    }
    
    # Internal errors
    CONFIGURATION_ERROR = {
        "reason": "CONFIGURATION_ERROR",
        "status": Status.INTERNAL_ERROR
    }
    INVALID_HANDLER_CONFIG = {
        "reason": "INVALID_HANDLER_CONFIG",
        "status": Status.INTERNAL_ERROR
    }

    # Database errors
    MISSING_DB_PARAM = { 
    "reason": "MISSING_DB_PARAM",
    "status_code": 500,
    "status": Status.INTERNAL_ERROR,
    }
    
    DATABASE_CONNECTION_FAILURE = {
        "reason": "DATABASE_CONNECTION_FAILURE",
        "status_code": 500,
        "status": Status.DATABASE_ERROR
    }
    SQL_EXECUTION_FAILURE = {
        "reason": "SQL_EXECUTION_FAILURE",
        "status_code": 500,
        "status": Status.DATABASE_ERROR
    }
    TRANSACTION_ERROR = {
        "reason": "TRANSACTION_ERROR",
        "status_code": 500,
        "status": Status.DATABASE_ERROR
    }

    # External errors (APIs)
    API_REQUEST_FAILED = {
        "reason": "API_REQUEST_FAILED",
        "status_code": 503,
        "status": Status.EXTERNAL_ERROR
    }
    TIMEOUT_ERROR = {
        "reason": "TIMEOUT_ERROR",
        "status_code": 504,
        "status": Status.EXTERNAL_ERROR
    }

    LOG_SQS_FAILURE = { 
    "reason": "LOG_SQS_FAILURE",
    "status_code": 500,
    "status": Status.EXTERNAL_ERROR,
    }
    # Unknown errors
    UNKNOWN_ERROR = {
        "reason": "UNKNOWN_ERROR",
        "status_code": 500,
        "status": Status.UNKNOWN_EXCEPTION
    }
