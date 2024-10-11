import json
import decimal
from datetime import datetime, timezone
import logging
import boto3
import os
import botocore.exceptions

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
    print(queue_url)
    
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
        # return secret
    except Exception as e:
        print(f"Error retrieving secret: {e}")
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

def validate_request_data(cursor, user_session_id):
    """
    Validate the user_session_id against the database.

    Parameters:
        cursor (psycopg2.cursor): The database cursor.
        user_session_id (str): The user'session id.

    Returns:
        dict: A response dict with validation errors if any, else None.
    """
    validation_errors = []

    # Validate if user_session_id is a valid UUID
    if not is_valid_uuid(user_session_id):
        validation_errors.append({
            "field": "user_session_id_invalid_format",
            "error": "User session_id is not a valid UUID format"
        })
    else:
        # Check if user_session_id exists in user_data.user_session
        cursor.execute("SELECT 1 FROM user_data.user_session WHERE user_session_id = %s", (str(user_session_id),))
        user_session_exists = cursor.fetchone()

        if not user_session_exists:
            validation_errors.append({
                "field": "user_session_id_not_exist",
                "error": "User session_id does not exist."
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
            print(f"Failed to send log to SQS: {str(e)}")

        return {
            "statusCode": 400,
            "body": json.dumps({
                "code": "validation_error",
                "message": "One or more request values couldn't be validated.",
                "field_errors": validation_errors
            })
        }

    return None
