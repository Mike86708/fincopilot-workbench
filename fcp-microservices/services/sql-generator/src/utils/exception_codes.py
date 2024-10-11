from enum import Enum



status_prefix = "MSG_FINCO_SQLGEN_"

class Status(Enum):
    EXTERNAL_ERROR = {"status": status_prefix + "EXTERNAL_ERROR", "status_code": 500}        # Dependent API errors
    INPUT_ERROR = {"status": status_prefix + "INPUT_ERROR", "status_code": 400}              # client api call error
    INTERNAL_ERROR = {"status": status_prefix + "INTERNAL_ERROR", "status_code": 500}        # other internal errors
    LLM_ERROR = {"status": status_prefix + "LLM_ERROR", "status_code": 500}                  # llm outputs 

    UNKNOWN_EXCEPTION = {"status": status_prefix + "UNKNOWN_EXCEPTION", "status_code": 500}  # unknown



class Reason(Enum):


    # input errors
    INVALID_CONFIG = {
        "reason": "INVALID_CONFIG",
        "status_code": 412,         # Specific status_code if needed. If left blank it will take the default from status
        "status": Status.INPUT_ERROR
    }
    MISSING_KEY = {
        "reason": "INVALID_CONFIG",
        "status_code": 412,         
        "status": Status.INPUT_ERROR
    }
    INVALID_API_CALL = {
        "reason": "INVALID_API_CALL",
        "status_code": 412,  
        "status": Status.INPUT_ERROR
    }

    
    

    # internal errors
    MISSING_MODEL_CONFIG_FILE = {
        "reason": "MISSING_MODEL_CONFIG_FILE",
        "status": Status.INTERNAL_ERROR
    }
    MISSING_API_KEY = {
        "reason": "MISSING_API_KEY",
        "status": Status.INTERNAL_ERROR
    }
    MISSING_LLM_IN_CONFIG = {
        "reason": "MISSING_LLM_IN_CONFIG",
        "status": Status.INTERNAL_ERROR
    }

    INVALID_PROMPT_CHAIN = {
        "reason": "INVALID_PROMPT_CHAIN",
        "status": Status.INTERNAL_ERROR
    }


    # llm errors
    PARSING_ERROR =  {
                        "reason": "PARSING_ERROR", 
                        "status": Status.LLM_ERROR
                    }
    INVALID_LLM_CONFIG = {
                            "reason": "INVALID_LLM_CONFIG", 
                            "status_code": 502,
                            "status": Status.LLM_ERROR
                        }


    # external errors (apis)
    TIMEOUT_EXCEEDED = { 
        "reason": "TIMEOUT_EXCEEDED",
        "status_code": 504,
        "status": Status.EXTERNAL_ERROR
    }
    API_ERROR = {
        "reason": "API_ERROR",
        "status_code": 503,
        "status": Status.EXTERNAL_ERROR
    }
    RATE_LIMIT_EXCEEDED = {
        "reason": "RATE_LIMIT_EXCEEDED",
        "status_code": 503,
        "status": Status.EXTERNAL_ERROR
    }
    INVALID_API_OUTPUT = {
        "reason": "INVALID_API_OUTPUT",
        "status_code": 502,
        "status": Status.EXTERNAL_ERROR
    }


    # Unknown errors
    UNKNOWN = {
        "reason": "UNKNOWN",
        "status": Status.UNKNOWN_EXCEPTION
    }







