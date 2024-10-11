import os
import json
import time
import asyncio
from manager.guard_manager import GuardManager
from exceptions.exception import InputGuardException
from util.utils import get_guard_messages, input_validator,get_prompt_guard_blocked_list

from util.aws_logging_utils import set_api_level_logs, LogLevel, LogType, log_cloudwatch
from exceptions.exception import InputGuardException
from exceptions.exception_codes import Reason


# Function to safely get the field or return None if the field is missing
def get_field(dictionary: dict, field: str):
    """
    Safely retrieves a field from a dictionary, returning None if the field does not exist.

    Args:
        dictionary (dict): The dictionary to search.
        field (str): The field key to retrieve.

    Returns:
        The value associated with the field, or None if the field is not found.
    """
    return dictionary.get(field, None)

def lambda_handler(event: dict, context: any) -> dict:
    """
    AWS Lambda handler function to run input guards on user-provided data.

    This function extracts input fields from the event payload, validates them, and
    applies various guards to the input using the GuardManager. The guards are
    executed based on the configuration provided via environment variables.

    Args:
        event (dict): The event data passed to the Lambda function. Expected keys include:
            - "prompt": The text prompt to be checked.
            - "domain": The domain associated with the prompt.
            - "subject_area": The subject area of the prompt.
            - "prompt_id": A unique identifier for the prompt.
            - "user_id": The ID of the user who provided the prompt.
            - "conversation_id": The ID of the conversation associated with the prompt.
            - "language": The language of the prompt.
        context (LambdaContext): The context object passed to the Lambda function.

    Returns:
        dict: The response to be sent back, containing the guard results and status code.
    """
    try:
        # Start measuring execution time for performance tracking
        start_time = time.time()

        # Extract and validate input fields from the event payload
        
        prompt = event.get('input', {}).get('user_prompt', '')
        domain = event.get('input', {}).get('domain', '')
        subject_area = event.get('input', {}).get('subject_area', '')
        language = event.get('input', {}).get('language', '')
        scope = event.get('input', {}).get('user_info', {}).get('scope', '')

        # Accessing session data correctly with default values
        session = event.get('session', {})
        prompt_id = session.get('prompt_id','')
        session_id = session.get('session_id') 
        conversation_id = session.get('conversation_id') 
        username=session.get('user_id') 
        
        
        # Prepare the input dictionary for validation
        user_input = {
            'user_prompt': prompt,
            'domain': domain,
            'subject_area': subject_area,
            'language': language,
            'user_id': username,
            'prompt_id': prompt_id,
            'conversation_id': conversation_id,
            'session_id': session_id
        }

        # Log service Input
        log_cloudwatch(log_type=LogType.SERVICE_INPUT, message="Input received", args=user_input,prompt_id=prompt_id)
        # Validate input to ensure required fields are present
        input_validator(user_input)
        

        # Initialize the GuardManager using mode and filters specified in environment variables
        guard_manager = GuardManager()
        
                # Run the guards asynchronously using the GuardManager
        guard_run_result = asyncio.run(
            guard_manager.run(prompt, domain, subject_area, prompt_id,  conversation_id,session_id,username, language)
        )

        # Retrieve any failure justification and message from the guard results
        justification, message = get_guard_messages(guard_run_result, guard_manager.filters)

        # Update the result with justification and message (if any)
        guard_run_result["justification"] = justification
        guard_run_result["message"] = message

       
        # get the list of blocked guards
        prompt_guard_blocked=get_prompt_guard_blocked_list(guard_run_result)
        
        # Calculate execution time for logging
        execution_time = time.time() - start_time
        

        # Add logs
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Output generated", args={
            "latency_ms": execution_time, # convert to ms
            "payload": {
             "statusCode": 200,
              "status":"SUCCESS",
            "body": guard_run_result,
            "prompt_guard_blocked":prompt_guard_blocked
            }
        },
            prompt_id=prompt_id)
        
        
        # Return the results in the expected format
        return {
            "statusCode": 200,
            "body": guard_run_result
        }

    except InputGuardException as e:
        
        log_cloudwatch(log_level=LogLevel.ERROR,log_type=LogType.SERVICE_OUTPUT, message="Lambda function error", args={"payload": e.get_response_data()},prompt_id=prompt_id) 
        
        # Handle known exceptions with a custom response
        # print(f"InputGuardException: {e}")
        return {
            "statusCode": 500,
            "body": {
                "code": e.get_response_data()['status'],
                "error_message": e.get_response_data()['message'],
                "message": "Apologies, we are unable to complete your request at this time. Please try again later. If the issue continues, please contact your System Administrator for assistance.",
                "metadata": e.get_response_data()['error_info']['metadata'],
            }
        }

    except Exception as e:
        log_cloudwatch(log_level=LogLevel.ERROR,log_type=LogType.SERVICE_OUTPUT, message="Lambda function error", args={"payload": str(e)},prompt_id=prompt_id) 
        
        # Catch any unexpected exceptions and return a 500 error
        # print(f"Unexpected error: {e}")
        return {
            "statusCode": 500,
            "body": {
                "error": "An unexpected error occurred.",
                "details": str(e),
                "message": "Apologies, we are unable to complete your request at this time. Please try again later. If the issue continues, please contact your System Administrator for assistance."
            }
        }
