import json
import logging

from lib.util.utils import Utils
from lib.exception.exceptions import JournalFilterException
from lib.util.aws_logging_utils import set_api_level_logs, log_cloudwatch, LogType, LogLevel

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    
    try:
        
        set_api_level_logs(
            {
                "prompt_id": "",
                "conversation_id": ""
            }
        )
        
        # Log the event received
        log_cloudwatch(log_type=LogType.SERVICE_INPUT, message="Lambda function invoked", args=event)
        
        filters = Utils.get_filters()
      
        # Return a successful response
        return {
            "statusCode": 200,
            "body": json.dumps(filters, indent=2)
        }
    except JournalFilterException as e:
        # Log the exception (SERVICE_OUTPUT)
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message=f"JournalFilterException occurred: {e}", args=e.get_response_data(), log_level=LogLevel.ERROR)

        response = {
            "statusCode": 500,
            "body": json.dumps(e.get_response_data(), indent=2),
        }
        return response


# Example of running the script
# if __name__ == "__main__":
#     try:
#         event = {}
#         context = {}
#         response = lambda_handler(event, context)
#         print("Lambda response:", response)
#     except Exception as e:
#         print(f"Error running the script: {e}")
