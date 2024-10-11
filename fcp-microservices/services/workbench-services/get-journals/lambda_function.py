import json

from lib.util.utils import Utils
from lib.exception.exceptions import GetJournalsException
from lib.exception.exception_codes import Reason
from lib.util.aws_logging_utils import set_api_level_logs, log_cloudwatch, LogType, LogLevel

# Lambda handler function
def lambda_handler(event, context):
    try:
        # Set up API level logs
        set_api_level_logs({
            "prompt_id": event.get("prompt_id", ""),
            "conversation_id": event.get("conversation_id", "")
        })
        
        # Log input received (SERVICE_INPUT)
        log_cloudwatch(log_type=LogType.SERVICE_INPUT, message="Lambda function invoked", args=event)
        
        # Try to establish a connection to Snowflake
        try:
            conn = Utils.connect_to_snowflake()
        except GetJournalsException as e:
            log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message=f"Failed to connect to Snowflake: {e}", log_level=LogLevel.ERROR)
            raise GetJournalsException(
                message="Failed to connect to Snowflake.",
                reason=Reason.DATABASE_CONNECTION_ERROR,
                subcomponent="lambda_handler"
            )
        
        cursor = conn.cursor()

        # Extract filters, ensuring that all necessary filters are correctly formatted
        try:
            filters = Utils.get_filters(event)
            search_string = event.get("search", None)
        except Exception as e:
            log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message=f"Error processing filters: {e}", log_level=LogLevel.ERROR)
            raise GetJournalsException(
                message="Error processing filters.",
                reason=Reason.INVALID_INPUT,
                subcomponent="lambda_handler",
                e=e
            )

        # Build the query and execute it
        try:
            query = Utils.build_query(filters, search_string)
            print(query)
            cursor.execute(query)
        except Exception as e:
            log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message=f"Query execution failed: {e}", log_level=LogLevel.ERROR)
            raise GetJournalsException(
                message="Query execution failed.",
                reason=Reason.QUERY_EXECUTION_ERROR,
                subcomponent="lambda_handler",
                e=e
            )
        
        # Fetch and process the results
        try:
            result = cursor.fetchall()
            final_result = Utils.process_result(result)
        except Exception as e:
            log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message=f"Error processing query results: {e}", log_level=LogLevel.ERROR)
            raise GetJournalsException(
                message="Error processing query results.",
                reason=Reason.RESULT_PROCESSING_ERROR,
                subcomponent="lambda_handler",
                e=e
            )

        # Log and return successful response
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Lambda response successfully generated")
        response = {
            "statusCode": 200,
            "body": json.dumps(final_result),
        }

    except GetJournalsException as e:
        # Handle known exceptions with detailed error logging
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message=f"Handled exception occurred: {e}", args=e.get_response_data(), log_level=LogLevel.ERROR)
        response = {
            "statusCode": 500,
            "body": json.dumps(e.get_response_data(), indent=2),
        }
    
    except Exception as e:
        # Log any other unforeseen exceptions
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
#         # Example event for local testing, including a search string
#         event = {
#             "Period": "Jan 2023",
#             "Creation Date": "Feb 13, 2023"
#         }
#         context = {}
#         response = lambda_handler(event, context)
#         print("Lambda response:", response)
#     except Exception as e:
#         print(f"Error running the script: {e}")
