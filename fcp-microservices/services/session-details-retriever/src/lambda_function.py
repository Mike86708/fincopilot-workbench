import psycopg2
import os
import json
# from dotenv import load_dotenv
from datetime import datetime, timezone
from lib.utils import send_log_to_sqs, retrieve_secret, validate_request_data
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set log level 
    
def validate_input(event):
    """
    Validate the input event.

    Parameters:
        event (dict): The input event containing user data.

    Returns:
        dict: A response dict with validation errors if any, else None.
    """
    required_fields = ['session_id']
    field_errors = []

    # Check for missing or empty required fields
    for field in required_fields:
        if field not in event or not event[field]:
            field_errors.append({
                "field": field,
                "error": f"{field} is required"
            })

    # If there are validation errors, log and return them
    if field_errors:
        log_message = {
            "level": "ERROR",
            "message": "Validation error",
            "field_errors": field_errors
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
                "field_errors": field_errors
            }
        }

    return None

def lambda_handler(event, context):
    """
    AWS Lambda handler to create a user session.

    Parameters:
        event (dict): The input event containing user data.
        context (object): The context in which the Lambda function is running.

    Returns:
        dict: A response dict with status code and body.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Set log level 
    
    start_time = datetime.utcnow()  # Record the start time

    # Validate input
    if event.get('queryStringParameters'):
        session_id = event.get('queryStringParameters', {}).get('session_id')
        # print(session_id)
    else:
        session_id=None
    
    if not session_id:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "code": "validation_error",
                "message": "session_id query parameter is required."
            })
        }
    
    # Validate input
    validation_error = validate_input({'session_id': session_id})
    if validation_error:
        logger.info("Validation error")
        return validation_error
    else:
        logger.info("Validation successful")
        
    
    # Load environment variables from .env file
    # load_dotenv()

    # Database connection parameters from environment variables
    db_host = os.environ.get('DB_HOST')
    db_name = os.environ.get('DB_NAME')
    db_schema = os.environ.get('DB_SCHEMA')
    db_port = os.environ.get('DB_PORT')
    
    # Retrieve secret
    secret_name = os.environ.get('secret_name')
    secret, status_code = retrieve_secret(secret_name)
    
    if status_code != 200:
        return {
            "statusCode": status_code,
            "body": json.dumps(secret)
        }
    
    if secret:
        # Access individual secret values
        db_user = secret.get('username')
        db_password = secret.get('password')
        logger.info("Successfully connected to secrets")
    else:
        logger.info("Failed to retrieve the secret.")
    
    # Check for required database env variables 
    required_vars = {
    'DB_HOST': db_host,
    'DB_NAME': db_name,
    'DB_USER': db_user,
    'DB_PASSWORD': db_password,
    'DB_PORT': db_port,
    'DB_SCHEMA':db_schema
    }

    missing_vars = [var for var, value in required_vars.items() if not value]

    if missing_vars:
        missing_vars_str = ', '.join(missing_vars)
        logger.info(f"Missing database connection environment variables: {missing_vars_str}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "code": "environment_variable_error",
                "message": f"One or more database connection environment variables are missing: {missing_vars_str}"
            })
        }
    
    # Initialize the conn and cursor objects
    conn = None
    cursor = None

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port,
            options="-c search_path=dbo,"+db_schema
        )
        cursor = conn.cursor()

        # Log successful connection
        log_message = {
            "session_id": session_id,
            "log_category": "status",
            "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
            "level": "INFO",
            "message": "Database connection successful",
            "module_name": "Session Details Retriever"
        }
        try:
            send_log_to_sqs(log_message)
            logger.info("Database connection successful")
        except Exception as e:
            logger.info(f"Failed to send log to SQS: {str(e)}")
            
        # Validate user data against the database
        validation_error = validate_request_data(cursor, session_id)
        if validation_error:
            logger.info(f"Request validation error")
            return validation_error
        else:
            logger.info(f"Request validation successful")
        
        try:
            # Execute the SQL query to get session details
            cursor.execute("SELECT * FROM user_data.get_user_session(%s)", (session_id,))
            session_details = cursor.fetchone()

            # Initialize empty session details dict
            session_details_dict = {}
            if session_details:
                session_details_dict['user_session_id'] = session_details[0]
                session_details_dict['email'] = session_details[1]
                session_details_dict['role_name'] = session_details[2]
                session_details_dict['is_valid_session'] = session_details[3]
                session_details_dict['subject_area'] = session_details[4]
                logger.info('Session details fetched successfully for session id : %s', session_id)
            else:
                logger.error(f"Session ID not found  : %s", session_id)
                raise Exception("Session not found")

        except psycopg2.Error as db_error:
            # Handle specific psycopg2 errors (like SQL syntax errors, constraint violations, etc.)
            error_message = str(db_error)
            logger.error(f"Database execution error: {error_message}")
            
            # Rollback any uncommitted transactions
            if conn:
                conn.rollback()
            
            return {
                "statusCode": 500,
                "body": {
                    "code": "database_execution_error",
                    "message": "An error occurred while executing the database query.",
                    "error_details": error_message
                }
            }

        end_time = datetime.utcnow()  # Record the end time
        execution_time = (end_time - start_time).total_seconds()  # Calculate the execution time

        # Log successful session creation with execution time
        log_message = {
            "session_id": session_id,
            "message": "Session details fetched successfully",
            "log_category": "status",
            "execution_time_seconds": execution_time,
            "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
            "level": "INFO",
            "module_name": "Session Details Retriever"
        }
        try:
            send_log_to_sqs(log_message)
            logger.info(f"Logs updated successfully for session : %s", session_id)
        except Exception as e:
            logger.info(f"Failed to send log to SQS: {str(e)}")

        # Log metrics
        log_message["log_category"] = "metrics"
        try:
            send_log_to_sqs(log_message)
            logger.info(f"Logs updated successfully for session : %s", session_id)
        except Exception as e:
            logger.error(f"Failed to send log to SQS: {str(e)}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "code": "success",
                "message": "Session details fetched successfully",
                # "session_id": session_id,
                "session_details": session_details_dict
            }),
            "headers": {
                "Content-Type": "application/json"
            },
        }

    except psycopg2.OperationalError as e:
        # Handle database connection errors
        error_message = str(e)
        logger.error(f"Database connection error: {error_message}")
        
        # Rollback any uncommitted transactions
        if conn:
            conn.rollback()

        return {
            "statusCode": 500,
            "body":  json.dumps({
                "code": "database_connection_error",
                "message": "Failed to connect to the database.",
                "field_errors": [
                    {
                        "field": "database",
                        "error": f"Unable to establish connection. Exception: {error_message}"
                    }
                ]
            })
        }

    except Exception as e:
        # Handle General error messages
        error_message = str(e)
        logger.error(f"An unexpected error occurred: {error_message}")

        if conn:
            conn.rollback()

        return {
            "statusCode": 500,
            "body":  json.dumps({
                "code": "internal_error",
                "message": "An unexpected error occurred.",
                "field_errors": error_message
            })
        }

    finally:
        # Ensure cursor and connection are closed properly
        if cursor:
            cursor.close()
        if conn:
            conn.close()
