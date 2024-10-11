import time
import re

from util.utils import get_return_msg, get_filters, get_data
from guard.guard import Guard

from util.aws_logging_utils import set_api_level_logs, LogLevel, LogType, log_cloudwatch
from exceptions.exception import InputGuardException
from exceptions.exception_codes import Reason


class PIIGuard(Guard):
    def __init__(self):
        super().__init__()

    async def check(self, prompt, domain, subject_area, prompt_id, user_id, conversation_id,session_id, language="english", additional_pii_entities=None):        
        """
        Checks the prompt for any Personally Identifiable Information (PII).
        @param prompt: The input text to be validated.
        @param domain: The domain of the prompt.
        @param subject_area: The subject area of the prompt.
        @param language: The language of the prompt.
        @param additional_pii_entities: Additional PII entities to be checked.
        @return: A dictionary containing the result, justification, message, and any exception.
        """
        start_time = time.time()
        result = {
            "result": True,
            "justification": None,
            "message": None,
            "exception": None
        }

        # Logging data
        logging_data = {
            "prompt": prompt,
            "model_name": None,
            "model_version": None,
            "prompt_id": prompt_id,
            "user_id": user_id,
            "conversation_id": conversation_id,
            "session_id":session_id,
            "language": language,
            "start_time":time.time()
        }
        try:
            # log_data(logging_data, "null", "INFO", "BEGIN_PII_VALIDATION", "Checking for sensitive information...", None)
            log_cloudwatch(log_type=LogType.FUNCTION_INPUT, message="BEGIN_PII_VALIDATION",args={"logging_data":logging_data,},prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)

            # Extract PII configurations
            data = get_data()
            apply_pii_guard, _, _, _, language, pii_configs = get_filters()
            

            if apply_pii_guard:
                pii_result, entities = self.regex_matching(prompt, pii_configs, additional_pii_entities)
            
                if not pii_result:
                    justification = f"MSG: prompt contained the following entity: {entities}"
                    result["result"] = False
                    result["justification"] = justification
                    result["message"] = get_return_msg("VP_1000")

                    # log_data
                    log_cloudwatch(log_type=LogType.FUNCTION_OUTPUT, message="PII Detected", args={"logging_data":logging_data,"result":result},prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)
                else:
                    result["justification"] = "No sensitive information found"
                    # log_data
                    log_cloudwatch(log_type=LogType.FUNCTION_OUTPUT, message="No sensitive information found", args={"logging_data":logging_data,"result":result},prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)

        except Exception as e:
            result["result"] = False
            result["exception"] = f"PII exception caught: {e}"
            result["message"] = get_return_msg("APP-1002")
            # log_data(logging_data, "null", "DEBUG", "PII_GUARD_ERR_101001", f"PII exception caught: {e}", None)
            log_cloudwatch(log_level=LogLevel.ERROR,log_type=LogType.SERVICE_OUTPUT, message="PII_GUARD_ERR_101001", args={"payload": str(e)},prompt_id=prompt_id,conversation_id=conversation_id,session_id=session_id)
        # calculate execution time
        result["execution_time"] = time.time() - start_time
        
        return result["result"],result

    def regex_matching(self, prompt, configs, additional_pii_entities=None):
        """
        Performs regex matching on the prompt to identify PII.
        @param prompt: The input text.
        @param configs: The PII configurations.
        @param additional_pii_entities: Additional PII entities to consider.
        @return: Tuple containing the result (True/False) and a list of detected entities.
        """
        pii_patterns = {
       "PHONE_NUMBER": [
        #r"(?:\+(\d{1,3}))?[\s.-]?(\(?\d{1,4}\)?[\s.-]?)?(\d{1,4})[\s.-]?(\d{1,4})[\s.-]?(\d{1,4})[\s.-]?(\d{1,9})", 
        r"(?:\+1\s?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}"
        ],
        "EMAIL_ADDRESS": [
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        ],
        "IP_ADDRESS": [
            r"\b(?:\d{1,3}\.){3}\d{1,3}\b|(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}:(?:[0-9a-fA-F]{1,4}:){1,2}|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:[0-9a-fA-F]{1,4}:){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}:(?:[0-9a-fA-F]{1,4}:){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}:(?:[0-9a-fA-F]{1,4}:){1,5}|(?:[0-9a-fA-F]{1,4}:)?::(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){0,5}::(?:[0-9a-fA-F]{1,4}:){0,4}[0-9a-fA-F]{1,4}|::(?:[0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}"
        ],
        "MEDICAL_LICENSE": [
            r"\b[A-Z]{1,2}\d{5,7}\b"
        ],
        "EIN": [
            r"\b\d{2}-\d{7}\b"
        ],
        "US_SSN": [
            r"\b\d{3}-\d{2}-\d{4}\b"
        ],
        "CREDIT_CARD": [
            r"\b(?:\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{3,4}|\d{4}[- ]?\d{6}[- ]?\d{5})\b"
        ],
        "US_ITIN": [
            r"9\d{2}-\d{2}-\d{4}"
        ],
        "US_PASSPORT": [
            r"\b[A-Z]\d{6,8}\b"
        ],
        "CAN_SIN": [
            r"\d{3}[- ]\d{3}[- ]\d{3}"
        ], 
        "IP_ADDRESS": [
            r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)", # IPv4
            r"([0-9a-fA-F]{1,4}:){7}([0-9a-fA-F]{1,4}|:)|(([0-9a-fA-F]{1,4}:){1,7}|[0-9a-fA-F]{1,4}::)([0-9a-fA-F]{1,4}:){0,5}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,6}::([0-9a-fA-F]{1,4}:){0,4}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}::([0-9a-fA-F]{1,4}:){0,3}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,4}::([0-9a-fA-F]{1,4}:){0,2}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,3}::([0-9a-fA-F]{1,4}:){0,1}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,2}::([0-9a-fA-F]{1,4}:){0,0}[0-9a-fA-F]{1,4}|[0-9a-fA-F]{1,4}::([0-9a-fA-F]{1,4}:){0,0}[0-9a-fA-F]{1,4}|::([0-9a-fA-F]{1,4}:){0,0}[0-9a-fA-F]{1,4}" # IPv6
        ]
        }

        # Add additional PII entities if provided
        if additional_pii_entities:
            pii_patterns.update(additional_pii_entities)

        detected_entities = []
        for entity, patterns in pii_patterns.items():
            if configs and configs[entity] == "True":
                for pattern in patterns:
                    if re.search(pattern, prompt):
                        detected_entities.append(entity)

        return (False if detected_entities else True), detected_entities
