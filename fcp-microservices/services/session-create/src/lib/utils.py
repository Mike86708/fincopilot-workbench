import json
import decimal
from datetime import datetime, timezone
import logging
import boto3
import os
import botocore.exceptions

logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set log level 

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
            logger.error("Failed to retrieve the secret.")
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


def validate_request_data(cursor, user_email, role, subject_area):
    """
    Validate the user_email, role, and subject_area against the database.

    Parameters:
        cursor (psycopg2.cursor): The database cursor.
        user_email (str): The user's email.
        role (str): The user's role.
        subject_area (str): The subject area code.

    Returns:
        dict: A response dict with validation errors if any, else None.
    """
    # Check if user_email exists in user_data.fincopilot_user
    cursor.execute("SELECT 1 FROM user_data.fincopilot_user WHERE email = %s", (user_email,))
    user_exists = cursor.fetchone()

    # # Check if role exists in user_data.app_role
    # cursor.execute("SELECT 1 FROM user_data.app_role WHERE role_name = %s", (role,))
    # role_exists = cursor.fetchone()
    if role is None or role=="":
        role_exists=True
    else:        
        # Check if role exists in user_data.app_role
        cursor.execute("SELECT 1 FROM user_data.app_role WHERE role_name = %s", (role,))
        role_exists = cursor.fetchone()

    # Check if subject_area exists in user_data.subject_area
    cursor.execute("SELECT 1 FROM user_data.subject_area WHERE code = %s", (subject_area,))
    subject_area_exists = cursor.fetchone()

    # Collect validation errors
    validation_errors = []
    if not user_exists:
        validation_errors.append({
            "field": "user_email",
            "error": "User email does not exist in user_data.fincopilot_user"
        })
    if not role_exists:
        validation_errors.append({
            "field": "role",
            "error": "Role does not exist in user_data.app_role"
        })
    if not subject_area_exists:
        validation_errors.append({
            "field": "subject_area",
            "error": "Subject area does not exist in user_data.subject_area"
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

        return {
            "statusCode": 400,
            "body": {
                "code": "validation_error",
                "message": "One or more request values couldn't be validated.",
                "field_errors": validation_errors
            }
        }

    return None
