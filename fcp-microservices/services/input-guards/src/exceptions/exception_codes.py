from enum import Enum

STATUS_PREFIX = "MSG_FINCO_INPUTGUARD_"

class Status(Enum):
    INPUT_ERROR = {"status": STATUS_PREFIX + "INPUT_ERROR", "status_code": 400}               # Client input errors
    CONFIGURATION_ERROR = {"status": STATUS_PREFIX + "CONFIGURATION_ERROR", "status_code": 500} # Configuration errors
    PROCESSING_ERROR = {"status": STATUS_PREFIX + "PROCESSING_ERROR", "status_code": 500}     # Data processing errors
    LLM_ERROR = {"status": STATUS_PREFIX + "LLM_ERROR", "status_code": 500}                  # LLM output errors
    UNKNOWN_ERROR = {"status": STATUS_PREFIX + "UNKNOWN_ERROR", "status_code": 500}           # Unknown/unhandled errors

class Reason(Enum):
    # Input Errors
    MISSING_REQUIRED_PARAMETER = {
        "reason": "MISSING_REQUIRED_PARAMETER",
        "status_code": 400,
        "status": Status.INPUT_ERROR
    }
    INVALID_INPUT = {
        "reason": "INVALID_INPUT",
        "status_code": 400,
        "status": Status.INPUT_ERROR
    }
    UNAUTHORIZED_ACCESS = {
        "reason": "UNAUTHORIZED_ACCESS",
        "status_code": 401,
        "status": Status.INPUT_ERROR
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
    PROCESSING_FAILED = {
        "reason": "PROCESSING_FAILED",
        "status_code": 500,
        "status": Status.PROCESSING_ERROR
    }

    # LLM Errors
    LLM_OUTPUT_ERROR = {
        "reason": "LLM_OUTPUT_ERROR",
        "status_code": 500,
        "status": Status.LLM_ERROR
    }

    # Unknown Errors
    UNKNOWN_ERROR = {
        "reason": "UNKNOWN_ERROR",
        "status_code": 500,
        "status": Status.UNKNOWN_ERROR
    }
