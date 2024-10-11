import boto3
from enum import Enum 
import json
from time import time
import os


region_name = os.environ.get('REGION_NAME')
sqs_logging_enabled = os.environ.get('SQS_ENABLED').lower() == "true"
sqs_url = os.environ.get('SQS_LOGGING_URL')


class LogLevel(Enum):
    '''
    Enum for log level
    '''
    INFO = "INFO"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class LogType(Enum):
    '''     
    Enum for log type
    '''
    SERVICE_INPUT = "SERVICE_INPUT",
    SERVICE_OUTPUT = "SERVICE_OUTPUT",
    LLM_DETAIL = "LLM_DETAIL",
    FUNCTION_INPUT = "FUNCTION_INPUT",
    FUNCTION_OUTPUT = "FUNCTION_OUTPUT",
    STATUS = "STATUS"



__session = boto3.Session(region_name=region_name) 
__sqs = __session.client('sqs')



aws_log = {}


class ModelInformation:
    '''
    Data class for LLM detail schema
    '''
    settings: dict
    run_statistics: dict
    


def set_api_level_logs(args: dict):
    '''
    Sets the AWS log variables at the highest level
    '''
    global aws_log

    if not sqs_logging_enabled:
        return 

    aws_log['service_name'] = os.environ.get('APP_NAME')
    aws_log['source_name'] = 'query_controller'

    aws_log = aws_log | args


def log_cloudwatch(log_type: LogType, message: str, args: dict, log_level: LogLevel = LogLevel.INFO):
    '''
    Conditionally logs message to AWS CloudWatch logs.
    Only logs if AWS_LOGGING['enabled'] is True
    
    @param log_level: Log level defaults to info
    @param message: Log message
    @param args: Log message arguments
    @return: None
    '''
    global aws_log

    if not sqs_logging_enabled:
        return 

    
    if args is not None:
        aws_log['timestamp'] = time()
        aws_log['log_level'] = log_level.value
        aws_log['log_type'] = log_type.value
        aws_log['message'] = message
        
    
    if log_type == LogType.SERVICE_INPUT or log_type == LogType.FUNCTION_INPUT:
        aws_log['log_info'] = {
            "payload": args
        }
    
    else:
        aws_log['log_info'] = args
        

    if sqs_logging_enabled:
        __sqs.send_message(
            QueueUrl=sqs_url,
            MessageBody=json.dumps(aws_log)
        )
    


    aws_log = {}
    
    
