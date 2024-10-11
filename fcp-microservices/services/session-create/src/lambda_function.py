import psycopg2
import os
import json
# from dotenv import load_dotenv
from datetime import datetime, timezone
from lib.utils import send_log_to_sqs,retrieve_secret,validate_request_data
import logging

def validate_input(event):
    """
    Validate the input event.

    Parameters:
        event (dict): The input event containing user data.

    Returns:
        dict: A response dict with validation errors if any, else None.
    """
    required_fields = ['user_email', 'subject_area']
    field_errors = []

    # Check for missing or empty required fields, excluding 'role'
    for field in required_fields:
        if field not in event or not event[field]:
            field_errors.append({
                "field": field,
                "error": f"{field} is required"
            })

    # Check if 'role' exists (it can be empty, but the key must exist)
    if 'role' not in event:
        event['role'] = None  # Assign role to None if not present

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
            print(f"Failed to send log to SQS: {str(e)}")

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
    validation_error = validate_input(event)
    if validation_error:
        logger.error(f"validation_error")
        return validation_error
    else :
        logger.info(f"validation successful")

    # Extract input parameters
    user_email = event['user_email']
    role = event['role']
    subject_area = event['subject_area']

    # # Load environment variables from .env file
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
        
        logger.info(f"successfully connected to secrets")
    else:
        logger.error("Failed to retrieve the secret.")
        
    
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
        logger.error(f"Missing database connection environment variables: {missing_vars_str}")
        return {
            "statusCode": 500,
            "body": {
                "code": "environment_variable_error",
                "message": f"One or more database connection environment variables are missing: {missing_vars_str}"
            }}
    
    # initialize the conn and cursor objects
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
            "user_email": user_email,
            "role": role,
            "subject_area": subject_area,
            "log_category": "status",
            "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
            "level": "INFO",
            "message": "Database connection successful",
            "module_name": "Session Create"
        }
        try:
            send_log_to_sqs(log_message)
            logger.info("Database connection successful")
        except Exception as e:
            logger.error(f"Failed to send log to SQS: {str(e)}")
        
        # Validate user data against the database
        validation_error = validate_request_data(cursor, user_email, role, subject_area)
        if validation_error:
            logger.error(f"Request validation error")
            return validation_error
        else:
            logger.info(f"Request validation successful")
    

        # Call the stored procedure to create a session and get the session ID
        cursor.execute("CALL user_data.usp_create_user_session(%s, %s, %s, %s, %s, %s, %s, %s)",
                       (role, subject_area, user_email, None, None, None, None, None))
   
        # Commit the transaction after executing the stored procedure
        conn.commit()

         # Fetch the output values after the procedure call
        session_id, sf_conn_required, user_modules, out_user_role, out_subject_area = cursor.fetchone()
        logger.info(f"New session created : {session_id, sf_conn_required, user_modules, out_user_role, out_subject_area}")
        
        # Validate session_id
        if not session_id:
            logger.error(f"Session ID not found  : {session_id}")
            raise Exception("Session ID not found")

        
        end_time = datetime.utcnow()  # Record the end time
        execution_time = (end_time - start_time).total_seconds()  # Calculate the execution time

        # Log successful session creation with execution time
        log_message = {
            "user_email": user_email,
            "role": role,
            "subject_area": subject_area,
            "log_category": "status",
            "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
            "level": "INFO",
            "message": "Session created successfully",
            "session_id": session_id,
            "execution_time_seconds": execution_time,
            "sf_conn_required": sf_conn_required,
            "module_name": "Session Create"
        }
        try:
            send_log_to_sqs(log_message)
            logger.info(f"Session updated successfully  : {session_id}")
        except Exception as e:
            logger.error(f"Failed to send log to SQS: {str(e)}")

        # Log metrics
        log_message["log_category"] = "metrics"
        try:
            send_log_to_sqs(log_message)
        except Exception as e:
            logger.error(f"Failed to send log to SQS: {str(e)}")

        return {
            "statusCode": 200,
            "body": {
                "code": "success",
                "message": "Session created successfully.",
                "session_id": session_id,
                "sf_conn_required": sf_conn_required,
                "user_modules":user_modules, 
                "out_user_role":out_user_role, 
                "out_subject_area":out_subject_area 
                
            }
        }

    except psycopg2.OperationalError as e:
        # Handle database connection errors
        error_message = str(e)
        print(e)
        log_message = {
            "user_email": user_email,
            "role": role,
            "subject_area": subject_area,
            "log_category": "status",
            "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
            "level": "ERROR",
            "message": f"Database connection error: {error_message}",
            "module_name": "Session Create"
        }
        try:
            send_log_to_sqs(log_message)
            logger.error( f"Database connection error: {error_message}")
        except Exception as e:
            logger.error(f"Failed to send log to SQS: {str(e)}")

        if conn:
            conn.rollback()
        return {
            "statusCode": 500,
            "body": {
                "code": "database_connection_error",
                "message": "Failed to connect to the database.",
                "field_errors": [
                    {
                        "field": "database",
                        "error": f"Unable to establish connection. Exception: {error_message}"
                    }
                ]
            }
        }

    except Exception as e:
        # Handle specific error messages from stored procedure
        error_message = str(e)
        print(error_message)
        if "User  is not Active" in error_message:
            log_message = {
                "user_email": user_email,
                "role": role,
                "subject_area": subject_area,
                "log_category": "status",
                "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
                "level": "ERROR",
                "message": f"User is not active: {error_message}",
                "module_name": "Session Create"
            }
            try:
                send_log_to_sqs(log_message)
                logger.error(f"User {user_email} is not active.")
            except Exception as e:
                logger.error(f"Failed to send log to SQS: {str(e)}")

            if conn:
                conn.rollback()
            return {
                "statusCode": 401,  # Change to 401 for unauthorized
                "body": {
                    "code": "user_inactive",
                    "message": "User is not active.",
                    "field_errors": []
                }
            }
        elif "Session Already Exists" in error_message:
            log_message = {
                "user_email": user_email,
                "role": role,
                "subject_area": subject_area,
                "log_category": "status",
                "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
                "level": "ERROR",
                "message": f"Session already exists: {error_message}",
                "module_name": "Session Create"
            }
            try:
                send_log_to_sqs(log_message)
                logger.error( f"Session already exists: {error_message}")
            except Exception as e:
                print(f"Failed to send log to SQS: {str(e)}")

            if conn:
                conn.rollback()
            return {
                "statusCode": 409,
                "body": {
                    "code": "session_exists",
                    "message": "Session already exists for this user."
                }
            }

        # Log and handle other exceptions
        log_message = {
            "user_email": user_email,
            "role": role,
            "subject_area": subject_area,
            "log_category": "status",
            "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
            "level": "ERROR",
            "message": f"Error: {error_message}",
            "module_name": "Session Create"
        }
        try:
            logger.info( f"Error: {error_message}")
            send_log_to_sqs(log_message)
        except Exception as e:
            logger.error(f"Failed to send log to SQS: {str(e)}")

        if conn:
            conn.rollback()
        return {
            "statusCode": 500,
            "body": {
                "code": "internal_error",
                "message": "An unexpected error occurred.",
                "field_errors": []
            }
        }

    finally:
        # Ensure cursor and connection are closed properly
        if cursor:
            cursor.close()
        if conn:
            conn.close()
