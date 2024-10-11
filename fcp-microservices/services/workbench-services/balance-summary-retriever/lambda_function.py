import json
from managers.balance_summary_manager import BalanceSummaryManager
from lib.util.utils import Utils
from lib.exception.exceptions import BalanceSummaryException
from lib.exception.exception_codes import Reason
from lib.util.aws_logging_utils import set_api_level_logs, log_cloudwatch, LogType, LogLevel

# Lambda handler function
def lambda_handler(event, context):
    try:
        set_api_level_logs(
            {
                "prompt_id": "",
                "conversation_id": ""
            }
        )
        
        # Log the input received (SERVICE_INPUT)
        log_cloudwatch(log_type=LogType.SERVICE_INPUT, message="Lambda function invoked", args=event)

        subsidiary_id = event.get("subsidiary_id")
        period_id = event.get("period_id")

        # Validate that subsidiary_id is an integer
        if subsidiary_id is None or not isinstance(subsidiary_id, int):
            raise BalanceSummaryException(
                "Invalid or missing 'subsidiary_id'. It must be an integer.",
                reason=Reason.INVALID_INPUT,
                subcomponent="lambda_handler",
            )

        # Validate that period_id is a string and matches the format 'MMM YYYY'
        if not isinstance(period_id, str) or not Utils.validate_period_id(period_id):
            raise BalanceSummaryException(
                "Invalid or missing 'period_id'. It must be a string in the format 'MMM YYYY' (e.g., 'Jan 2023').",
                reason=Reason.INVALID_INPUT,
                subcomponent="lambda_handler",
            )

        # Log the validated input (FUNCTION_INPUT)
        log_cloudwatch(log_type=LogType.FUNCTION_INPUT, message="Validated input parameters", args={
            'subsidiary_id': subsidiary_id,
            'period_id': period_id
        })

        # Create an instance of the BalanceSummaryManager
        manager = BalanceSummaryManager(
            subsidiary_id=subsidiary_id,
            period_id=period_id
        )

        # Get the processed balance summary result
        balance_summary = manager.get_result()

        # Log the result output (FUNCTION_OUTPUT)
        log_cloudwatch(log_type=LogType.FUNCTION_OUTPUT, message="Successfully retrieved balance summary", args=balance_summary)

        response = {
            "statusCode": 200,
            "body": json.dumps(balance_summary),
        }

        # Log the successful response (SERVICE_OUTPUT)
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Lambda response successfully generated", args=response)

    except BalanceSummaryException as e:
        # Log the exception (SERVICE_OUTPUT)
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message=f"BalanceSummaryException occurred: {e}", args=e.get_response_data(), log_level=LogLevel.ERROR)

        response = {
            "statusCode": 500,
            "body": json.dumps(e.get_response_data(), indent=2),
        }
    except Exception as e:
        # Log any other unforeseen exceptions (SERVICE_OUTPUT)
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message=f"Unexpected error: {str(e)}", args=str(e), log_level=LogLevel.ERROR)

        response = {
            "statusCode": 500,
            "body": json.dumps({
                "message": "An unexpected error occurred.",
                "details": str(e),
            }, indent=2),
        }

    return response


# if __name__ == "__main__":
#     try:
#         event = {"subsidiary_id": 5, "period_id": "Jul 2022"}
#         context = {}
#         response = lambda_handler(event, context)
#         print("Lambda response:", response)
#     except Exception as e:
#         print(f"Error running the script: {e}")
