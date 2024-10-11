import json
import decimal
from datetime import datetime, timezone
import logging
import boto3
import os
import botocore.exceptions

from .config import SETTINGS

logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
class CustomJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle Decimal and datetime objects.
    """
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        elif isinstance(o, datetime):
            return o.replace(tzinfo=timezone.utc).isoformat()
        return super().default(o)


# Function to send logs to SQS
def send_log_to_sqs(log_message):
    """
    Sends the log data to the SQS queue.

    Args:
    - log_message (dict): Log data to send.
    """

    sqs_client = boto3.client('sqs')
    queue_url = os.environ.get('SQS_QUEUE_URL')
    
    
    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(log_message, cls=CustomJSONEncoder)
        )
        return response
    except boto3.exceptions.Boto3Error as e:
        error_message = f"Failed to send log to SQS: {str(e)}"
        logging.error(error_message)
        raise RuntimeError(error_message)
    except Exception as e:
        error_message = f"Unexpected error occurred while sending log to SQS: {str(e)}"
        logging.error(error_message)
        raise RuntimeError(error_message)


def get_secret(secret_name):
    """
    Retrieve a secret from AWS Secrets Manager.

    Parameters:
        secret_name (str): The name of the secret to retrieve.

    Returns:
        dict: A dictionary containing the secret values.
    """
    # Create a Secrets Manager client
    client = boto3.client('secretsmanager')  # Replace with your region

    try:
        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)
        # Parse the secret value (it should be in JSON format)
        secret = response['SecretString']
        return json.loads(secret)
    except Exception as e:
        logger.error(f"Error retrieving secret: {e}")
        return None


def retrieve_secret(secret_name):
    """
    Retrieve a secret from AWS Secrets Manager and handle potential errors.

    Parameters:
        secret_name (str): The name of the secret to retrieve.

    Returns:
        tuple: A tuple containing the secret dictionary and a status code.
    """
    logger = logging.getLogger()
    
    try:
        # Attempt to retrieve the secret
        secret = get_secret(secret_name)
       
        if secret:
            logger.info("Successfully connected to secrets")
            return secret, 200
        else:
            logger.info("Failed to retrieve the secret.")
            return {
                "statusCode": 500,
                "body": {
                    "code": "secret_retrieval_error",
                    "message": "Failed to retrieve the secret from Secrets Manager.",
                    "field_errors": []
                }
            }, 500
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDeniedException':
            logger.error("Access denied to Secrets Manager: " + str(e))
            return {
                "statusCode": 403,
                "body": {
                    "code": "access_denied",
                    "message": "Access denied to retrieve the secret from Secrets Manager.",
                    "field_errors": []
                }
            }, 403
        else:
            logger.error("Error retrieving secret: " + str(e))
            return {
                "statusCode": 500,
                "body": {
                    "code": "secret_retrieval_error",
                    "message": "An error occurred while retrieving the secret.",
                    "field_errors": []
                }
            }, 500
    except Exception as e:
        logger.error("Unexpected error: " + str(e))
        return {
            "statusCode": 500,
            "body": {
                "code": "internal_error",
                "message": "An unexpected error occurred while retrieving the secret.",
                "field_errors": []
            }
        }, 500




import uuid

def is_valid_uuid(user_session_id):
    """
    Validate if the provided user_session_id is a valid UUID.

    Parameters:
        user_session_id (str): The user session ID to validate.

    Returns:
        bool: True if valid UUID, False otherwise.
    """
    try:
        # Attempt to create a UUID object from the provided string
        uuid_obj = uuid.UUID(user_session_id)
        # Check if the string representation of the UUID matches the original string
        return str(uuid_obj) == user_session_id
    except ValueError:
        # If an exception is raised, the string is not a valid UUID
        return False

def validate_request_data(cursor, user_session_id, prompt_id, conversion_id, feedback_source, feedback_type, feedback_text):
    """
    Validate the user_session_id, prompt_id, conversion_id, feedback_source, feedback_type, and feedback_text.

    Parameters:
        cursor (psycopg2.cursor): The database cursor.
        user_session_id (str): The user's session ID.
        prompt_id (str): The prompt ID.
        conversion_id (str): The conversion ID.
        feedback_source (str): The feedback source.
        feedback_type (str): The feedback type.
        feedback_text (str): The feedback text.

    Returns:
        dict: A response dict with validation errors if any, else None.
    """
    validation_errors = []

    # Validate if user_session_id is a valid UUID
    if  isinstance(user_session_id, str) and not is_valid_uuid(user_session_id):
        validation_errors.append({
            "field": "user_session_id_invalid_format",
            "error": "User session_id is not a valid UUID format"
        })
    elif isinstance(prompt_id, str) and not is_valid_uuid(prompt_id):
        validation_errors.append({
            "field": "prompt_id_invalid_format",
            "error": "Prompt_id is not a valid UUID format"
        })
    elif isinstance(conversion_id, str) and not is_valid_uuid(conversion_id):
        validation_errors.append({
            "field": "conversion_id_invalid_format",
            "error": "Conversion_id is not a valid UUID format"
        })
    
    # Validate that feedback_source, feedback_type, and feedback_text are strings
    if not isinstance(feedback_source, str):
        validation_errors.append({
            "field": "feedback_source_invalid_type",
            "error": "Feedback type must be a string"
        })
    if not isinstance(feedback_type, str):
        validation_errors.append({
            "field": "feedback_type_invalid_type",
            "error": "Feedback type must be a string"
        })
    if not isinstance(feedback_text, str):
        validation_errors.append({
            "field": "feedback_text_invalid_type",
            "error": "Feedback text must be a string"
        })

    if not isinstance(user_session_id, str):
        validation_errors.append({
            "field": "user_session_id_invalid_format",
            "error": "User session_id is not a valid UUID format"
        })
    if not isinstance(prompt_id, str):
        validation_errors.append({
            "field": "prompt_id_invalid_format",
            "error": "Prompt_id is not a valid UUID format"
        })
    if not isinstance(conversion_id, str):
        validation_errors.append({
           "field": "conversion_id_invalid_format",
            "error": "Conversion_id is not a valid UUID format"
        })
        
    # If there are validation errors, log and return them
    if validation_errors:
        log_message = {
            "level": "ERROR",
            "message": "Validation error",
            "field_errors": validation_errors
        }
        try:
            send_log_to_sqs(log_message)
        except Exception as e:
            logger.error(f"Failed to send log to SQS: {str(e)}")

        return validation_errors
      

    return None


        
def validate_feedback_type(feedback_type_list):
    """
    Validates the feedback types provided in the input list against a predefined list of valid feedback types.

    Args:
        feedback_type_list (list): A list of strings representing feedback types to be validated.

    Returns:
        tuple: 
            - bool: True if all feedback types in the input list are valid, False otherwise.
            - str: A comma-separated string of invalid feedback types. 
                   If all feedback types are valid, this string will be empty.
                   
    Example:
        feedback_type_list = ["Incorrect Output", "Unsupported Type", "Slow Response"]
        is_valid, invalid_feedback_types_str = validate_feedback_type(feedback_type_list)
        
        print(is_valid)  # Output: False
        print(invalid_feedback_types_str)  # Output: "Unsupported Type"
    """
    
    # Predefined valid feedback types (for example, this could come from SETTINGS)
    valid_feedback_types = SETTINGS['feedback_type_list']
    
    # Split input feedback_type_list into individual types
    input_feedback_types = [feedback_type.strip() for feedback_type in feedback_type_list if feedback_type]
    
    # Find invalid feedback types by checking against valid_feedback_types
    invalid_feedback_types = [ft for ft in input_feedback_types if ft not in valid_feedback_types]
    
    # Determine if the validation is successful (no invalid feedback types)
    is_valid = len(invalid_feedback_types) == 0
    
    # Join invalid feedback types into a single comma-separated string
    invalid_feedback_types_str = ', '.join(invalid_feedback_types)
    
    # Return the boolean result and the invalid feedback types string
    return is_valid, invalid_feedback_types_str


def validate_feedback_source(feedback_source):
    feedback_source_list=SETTINGS['feedback_source_list']
    
    if feedback_source in feedback_source_list:
        return True
    else:
        False