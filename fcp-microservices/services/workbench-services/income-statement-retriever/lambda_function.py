import json
import logging
from managers.income_statement_manager import IncomeStatementManager
from lib.util.utils import Utils
from lib.exception.exceptions import IncomeStatementException
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
        
        # Validate event parameters
        logger.info("Validating event parameters.")
        Utils.validate_event_params(event)

        # Extract parameters from the event
        subsidiary_id = event.get("subsidiary_id")
        from_period_id = event.get("from_period_id")
        to_period_id = event.get("to_period_id")

        logger.info(f"subsidiary_id: {subsidiary_id}, from_period_id: {from_period_id}, to_period_id: {to_period_id}")

        # Create an instance of the IncomeStatementManager
        manager = IncomeStatementManager(
            subsidiary_id=subsidiary_id,
            from_period_id=from_period_id,
            to_period_id=to_period_id
        )

        # Get the processed result
        income_statement = manager.get_result()

        # Log the successful response
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Successfully retrieved income statement", args=income_statement)
        
        # Return a successful response
        return {
            "statusCode": 200,
            "body": json.dumps(income_statement, indent=2)
        }

    except IncomeStatementException as e:
        logger.error(f"IncomeStatementException: {e}")
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Error during income statement execution", args=e.get_response_data(), log_level=LogLevel.ERROR)
        return {
            "statusCode": e.status_code,
            "body": json.dumps(e.get_response_data(), indent=2)
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Unexpected error", args=str(e), log_level=LogLevel.ERROR)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "An unexpected error occurred.",
                "details": str(e)
            }, indent=2)
        }

# Example of running the script
# if __name__ == "__main__":
#     try:
#         event = {
#                     "subsidiary_id": 5, 
#                     "from_period_id": 140,
#                     "to_period_id": 160
#                 }

#         context = {}
#         response = lambda_handler(event, context)
#         print("Lambda response:", response)
#     except Exception as e:
#         print(f"Error running the script: {e}")
