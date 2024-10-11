'''
message = {
    "prompt_id" :,
    "conversation_id":,
    "user_id": ,
    "log_category" : log_category, #metrics, status
    "execution_time_in_ms":get_exec_time(start_time),  
    "timestamp": time.time(),
    "user_input": prompt,
    "model_output": justification,
    "model_name": model_name,
    "model_version": model_version,
    "tokens_used" : "N/A",
    "log_type" : logging_type, # info err debug
    "log_message": msg,
    "module_name": "validateprompt"
}

"info" and "err" messages are used for in-between app information. 
Required for that: 
prompt_id, user_input, LogLevel, message, module_name
The rest are going to be null

"debug"
Only used at the end of the app execution
Required for that:
All fields in the message 

'''

import boto3
from enum import Enum 
from src.utils.main import logger, SETTINGS, json
from time import time

from langchain_core.language_models import BaseChatModel


AWS_LOGGING = SETTINGS['aws']['logging']




class LogLevel(Enum): # Maybe use logging module instead
    INFO = "INFO"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class LogType(Enum):
    SERVICE_INPUT = "SERVICE_INPUT",
    SERVICE_OUTPUT = "SERVICE_OUTPUT",
    LLM_DETAIL = "LLM_DETAIL",
    FUNCTION_INPUT = "FUNCTION_INPUT",
    FUNCTION_OUTPUT = "FUNCTION_OUTPUT",
    STATUS = "STATUS"



__session = boto3.Session(region_name=AWS_LOGGING['region']) 
__sqs = __session.client('sqs')



aws_log = {}
api_level_logs = {}



class ModelInformation:
    '''
    Data class for LLM detail schema
    '''
    settings: dict
    run_statistics: dict
    



def get_formatted_log_from_llm(llm: BaseChatModel):
    '''
    Get the formatted log for any langchain llm object ChatOpenAT, ChatBedrock, etc
     
    @param llm: Langchain llm object
    @return: The formatted log
    '''
    inputs = {}
    inputs['model_name'] = llm.model_name
    inputs['temperature'] = llm.temperature
    inputs['n'] = llm.n
    inputs['max_tokens'] = llm.max_tokens
    inputs['max_retries'] = llm.max_retries
    inputs['top_p'] = llm.model_kwargs['top_p']

    return inputs





def set_api_level_logs(args: dict):
    '''
    Sets the AWS log variables at the highest level
    '''
    global aws_log, api_level_logs
    aws_log = {}

    if not AWS_LOGGING['enabled']:
        return 

    aws_log['service_name'] = SETTINGS['app']['name']
    aws_log['source_name'] = 'query_controller'

    aws_log = aws_log | args

    api_level_logs = aws_log



def log_cloudwatch(log_type: LogType, message: str, args: dict, log_level: LogLevel = LogLevel.INFO):
    '''
    Logs message to AWS CloudWatch Logs

    @param log_level: Log level defaults to info
    @param message: Log message
    @param args: Log message arguments
    @return: None
    '''
    global aws_log

    if not AWS_LOGGING['enabled']:
        return 
    

    
    
    
    aws_log['timestamp'] = time()
    aws_log['log_level'] = log_level.value
    aws_log['log_type'] = log_type.value
    aws_log['message'] = message
    aws_log['log_info'] = args


    if AWS_LOGGING['enabled']:
        __sqs.send_message(
            QueueUrl=AWS_LOGGING['api_url'],
            MessageBody=json.dumps(aws_log)
        )
    
    logger.debug(f"aws_log: {aws_log}\n")
    # TODO: Change type of logging
    # if log_level == LogLevel.INFO:
    #     logger.info(aws_log)
    # elif log_level == LogLevel.ERROR:
    #     logger.error(aws_log)
    # elif log_level == LogLevel.DEBUG:
    #     logger.debug(aws_log)

    aws_log = api_level_logs