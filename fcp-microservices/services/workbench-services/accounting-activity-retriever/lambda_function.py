import json
from managers.accounting_activity_manager import AccountingActivityManager
from lib.util.utils import Utils
from lib.exception.exceptions import AccountingActivityException
from lib.util.aws_logging_utils import set_api_level_logs, log_cloudwatch, LogType, LogLevel

def lambda_handler(event, context):
    try:
        set_api_level_logs(
            {
                "prompt_id": "",
                "conversation_id": ""
            }
        )
        
        # Log the incoming event data at INFO level
        log_cloudwatch(
            log_type=LogType.SERVICE_INPUT,
            message="Received event for lambda_handler",
            args=event,
            log_level=LogLevel.INFO
        )

        # Validate event parameters
        Utils.validate_event_params(event)
        log_cloudwatch(
            log_type=LogType.STATUS,
            message="Event parameters validation successful",
            args={}
        )

        # Extract parameters from the event
        request_type = event.get("type")
        from_period_id = event.get("from_period_id")
        to_period_id = event.get("to_period_id")
        subsidiary_id = event.get("subsidiary_id")
        account_id = event.get("account_id")

        # Log extracted parameters
        log_cloudwatch(
            log_type=LogType.SERVICE_INPUT,
            message="Extracted event parameters",
            args={
                "type": request_type,
                "from_period_id": from_period_id,
                "to_period_id": to_period_id,
                "subsidiary_id": subsidiary_id,
                "account_id": account_id
            }
        )

        # Create an instance of the AccountingActivityManager
        manager = AccountingActivityManager(
            type=request_type,
            from_period_id=from_period_id,
            to_period_id=to_period_id,
            subsidiary_id=subsidiary_id,
            account_id=account_id
        )
        
        # Log manager initialization
        log_cloudwatch(
            log_type=LogType.STATUS,
            message="AccountingActivityManager initialized",
            args={}
        )

        # Get the processed result
        accounting_activity = manager.get_result()
        
        # Log the result
        log_cloudwatch(
            log_type=LogType.SERVICE_OUTPUT,
            message="Accounting activity result retrieved",
            args=accounting_activity
        )

        # Return a successful response
        return {
            "statusCode": 200,
            "body": json.dumps(accounting_activity, indent=2)
        }

    except AccountingActivityException as e:
        # Log specific AccountingActivityException at ERROR level
        log_cloudwatch(
            log_type=LogType.STATUS,
            message="AccountingActivityException encountered",
            args=e.get_response_data(),
            log_level=LogLevel.ERROR
        )
        return {
            "statusCode": e.status_code,
            "body": json.dumps(e.get_response_data(), indent=2)
        }
    except Exception as e:
        # Log unexpected exceptions at ERROR level
        log_cloudwatch(
            log_type=LogType.STATUS,
            message="Unexpected error occurred",
            args={"error": str(e)},
            log_level=LogLevel.ERROR
        )
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
#             "type": "BALANCE_SUMMARY",
#             "from_period_id": 140,
#             "to_period_id": 160,
#             "subsidiary_id": 5,
#             "account_id": 2733
#         }
#         context = {}
#         response = lambda_handler(event, context)
#         print("Lambda response:", response)
#     except Exception as e:
#         print(f"Error running the script: {e}")
