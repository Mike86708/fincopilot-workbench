import json
from managers.account_filter_manager import AccountFilterManager
from lib.util.utils import Utils
from lib.exception.exceptions import AccountFilterException
from lib.util.aws_logging_utils import log_cloudwatch, LogType, LogLevel, set_api_level_logs

def lambda_handler(event, context):
    # Set up initial logging
    set_api_level_logs(
        {
            "prompt_id": "",
            "conversation_id": ""
        }
    )
    
    try:
        # Validate event parameters
        log_cloudwatch(LogType.SERVICE_INPUT, "Validating event parameters.", event, LogLevel.INFO)
        Utils.validate_event_params(event)

        # Extract parameters from the event
        data_type = event.get("data_type")
        subsidiary_id = event.get("subsidiary_id")

        # Log extracted parameters
        log_cloudwatch(LogType.SERVICE_INPUT, "Extracted parameters from event.", {
            "data_type": data_type,
            "subsidiary_id": subsidiary_id
        }, LogLevel.DEBUG)

        # Create an instance of the AccountingActivityManager
        manager = AccountFilterManager(
            data_type=data_type,
            subsidiary_id=subsidiary_id,
        )

        # Get the processed result
        accounting_activity = manager.get_result()

        # Log the result
        log_cloudwatch(LogType.SERVICE_OUTPUT, "Processed accounting activity.", accounting_activity, LogLevel.INFO)

        # Return a successful response
        return {
            "statusCode": 200,
            "body": json.dumps(accounting_activity, indent=2)
        }

    except AccountFilterException as e:
        # Log the exception
        log_cloudwatch(LogType.STATUS, "AccountFilterException occurred.", {
            "message": str(e),
            "status_code": e.status_code
        }, LogLevel.ERROR)
        
        return {
            "statusCode": e.status_code,
            "body": json.dumps(e.get_response_data(), indent=2)
        }
    except Exception as e:
        # Log the unexpected exception
        log_cloudwatch(LogType.STATUS, "An unexpected error occurred.", {
            "message": str(e)
        }, LogLevel.ERROR)
        
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "An unexpected error occurred.",
                "details": str(e)
            }, indent=2)
        }


# if __name__ == "__main__":
#     try:
#         event = {
#             # "data_type": "INCOME_STATEMENT", #"BALANCE_SUMMARY", "TRIAL_BALANCE"
#             "data_type": "BALANCE_SUMMARY",
#             "subsidiary_id": 5
#         }
#         context = {}
#         response = lambda_handler(event, context)
#         print("Lambda response:", response)
#     except Exception as e:
#         print(f"Error running the script: {e}")
