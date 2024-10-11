import boto3
from enum import Enum 
import json
from time import time
import os

# # # Load environment variables from .env file
# from dotenv import load_dotenv
# load_dotenv()

# with open('config.json', 'r') as f:
#     config = json.load(f)

# Configuration dictionary loaded from environment variables
config={
    "sqs_logging": {
      "enabled": True,
      "region": os.getenv("REGION_NAME"),
      "api_url": os.getenv("LOG_SQS_URL")
    },
    "app": {
      "name": "Account Filter"
    }
  }

AWS_LOGGING = config['sqs_logging']


class LogLevel(Enum):
    '''
    Enum for log level.
    Defines different levels of logging such as INFO, ERROR, and DEBUG.
    '''
    INFO = "INFO"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class LogType(Enum):
    '''     
    Enum for log type.
    Defines various types of logs, such as SERVICE_INPUT, SERVICE_OUTPUT, FUNCTION_INPUT, FUNCTION_OUTPUT, and STATUS.
    '''
    SERVICE_INPUT = "SERVICE_INPUT",
    SERVICE_OUTPUT = "SERVICE_OUTPUT",
    FUNCTION_INPUT = "FUNCTION_INPUT",
    FUNCTION_OUTPUT = "FUNCTION_OUTPUT",
    STATUS = "STATUS"


# Initialize a boto3 session with the region specified in the config
__session = boto3.Session(region_name=AWS_LOGGING['region']) 
__sqs = __session.client('sqs')



aws_log = {}


class ModelInformation:
    '''
    Data class for LLM detail schema.
    This class holds information about model settings and run statistics.
    '''
    settings: dict
    run_statistics: dict
    


def set_api_level_logs(args: dict):
    '''
    Sets the AWS log variables at the highest level.
    Combines the given arguments with the global aws_log dictionary.
    
    @param args: Dictionary of log information to be added
    @return: None
    '''
    global aws_log

    if not AWS_LOGGING['enabled']:
        return 

    aws_log['service_name'] = config['app']['name']
    aws_log['source_name'] = 'account_controller'

    # Merge aws_log and args dictionaries
    # aws_log = aws_log | args # for python >3.8
    aws_log.update(args) # for python3.8
    


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

    if not AWS_LOGGING['enabled']:
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

    print(json.dumps(aws_log))
    if AWS_LOGGING['enabled']:
        __sqs.send_message(
            QueueUrl=AWS_LOGGING['api_url'],
            MessageBody=json.dumps(aws_log)
        )
    


    # Reset only the non-persistent fields, keep `service_name` and `source_name`
    for key in ['timestamp', 'log_level', 'log_type', 'message', 'log_info']:
        aws_log.pop(key, None)