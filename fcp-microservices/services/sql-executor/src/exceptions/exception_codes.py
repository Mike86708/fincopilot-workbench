from enum import Enum

STATUS_PREFIX = "MSG_FINCO_SQLEXE_"

class Status(Enum):
    INPUT_ERROR = {"status": STATUS_PREFIX + "INPUT_ERROR", "status_code": 400}               # Client input errors
    DATABASE_ERROR = {"status": STATUS_PREFIX + "DATABASE_ERROR", "status_code": 500}         # Snowflake database errors
    AWS_SERVICE_ERROR = {"status": STATUS_PREFIX + "AWS_SERVICE_ERROR", "status_code": 503}   # AWS service related errors
    CONFIGURATION_ERROR = {"status": STATUS_PREFIX + "CONFIGURATION_ERROR", "status_code": 500} # Configuration errors
    PROCESSING_ERROR = {"status": STATUS_PREFIX + "PROCESSING_ERROR", "status_code": 500}     # Data processing errors
    UNKNOWN_ERROR = {"status": STATUS_PREFIX + "UNKNOWN_ERROR", "status_code": 500}           # Unknown/unhandled errors

class Reason(Enum):
    # Input Errors
    MISSING_REQUIRED_PARAMETER = {
        "reason": "MISSING_REQUIRED_PARAMETER",
        "status_code": 400,
        "status": Status.INPUT_ERROR
    }
    INVALID_SQL_QUERY = {
        "reason": "INVALID_SQL_QUERY",
        "status_code": 400,
        "status": Status.INPUT_ERROR
    }
    UNAUTHORIZED_ACCESS = {
        "reason": "UNAUTHORIZED_ACCESS",
        "status_code": 401,
        "status": Status.INPUT_ERROR
    }

    # Database Errors
    DATABASE_CONNECTION_FAILED = {
        "reason": "DATABASE_CONNECTION_FAILED",
        "status_code": 500,
        "status": Status.DATABASE_ERROR
    }
    SQL_EXECUTION_FAILED = {
        "reason": "SQL_EXECUTION_FAILED",
        "status_code": 500,
        "status": Status.DATABASE_ERROR
    }
    DATA_FETCH_ERROR = {
        "reason": "DATA_FETCH_ERROR",
        "status_code": 500,
        "status": Status.DATABASE_ERROR
    }

    # AWS Service Errors
    S3_UPLOAD_FAILED = {
        "reason": "S3_UPLOAD_FAILED",
        "status_code": 503,
        "status": Status.AWS_SERVICE_ERROR
    }
    SQS_SEND_MESSAGE_FAILED = {
        "reason": "SQS_SEND_MESSAGE_FAILED",
        "status_code": 503,
        "status": Status.AWS_SERVICE_ERROR
    }
    SECRETS_MANAGER_ERROR = {
        "reason": "SECRETS_MANAGER_ERROR",
        "status_code": 503,
        "status": Status.AWS_SERVICE_ERROR
    }

    # Configuration Errors
    MISSING_CONFIGURATION = {
        "reason": "MISSING_CONFIGURATION",
        "status_code": 500,
        "status": Status.CONFIGURATION_ERROR
    }
    INVALID_CONFIGURATION = {
        "reason": "INVALID_CONFIGURATION",
        "status_code": 500,
        "status": Status.CONFIGURATION_ERROR
    }

    # Processing Errors
    DATA_FORMATTING_ERROR = {
        "reason": "DATA_FORMATTING_ERROR",
        "status_code": 500,
        "status": Status.PROCESSING_ERROR
    }
    CSV_GENERATION_FAILED = {
        "reason": "CSV_GENERATION_FAILED",
        "status_code": 500,
        "status": Status.PROCESSING_ERROR
    }

    # Unknown Errors
    UNKNOWN_ERROR = {
        "reason": "UNKNOWN_ERROR",
        "status_code": 500,
        "status": Status.UNKNOWN_ERROR
    }
