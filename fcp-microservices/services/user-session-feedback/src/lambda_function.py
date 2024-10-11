import psycopg2
import os
import json
from datetime import datetime, timezone
from lib.utils import send_log_to_sqs,retrieve_secret,validate_request_data,validate_feedback_type,validate_feedback_source
import logging
from lib.exception import UserFeedbackException
from lib.exception_codes import Reason

# # Load environment variables from .env file
# from dotenv import load_dotenv
# load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO) 
    
def validate_input(event):
    """
    Validate the input event.

    Parameters:
        event (dict): The input event containing user data.

    Returns:
        dict: A response dict with validation errors if any, else None.
    """
    required_fields = ["user_session_id", "prompt_id","conversation_id","feedback_source", "feedback_type"]
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

        return field_errors

    return None


#------------------------------------------------------------------------------------------
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

     # initialize the conn and cursor objects
    conn = None
    cursor = None   
    
    try:
        # Extract user fields
        user_session_id = event.get("user_session_id", None)
        prompt_id = event.get("prompt_id", None)
        conversation_id = event.get("conversation_id", None)
        feedback_source = event.get("feedback_source", None)
        feedback_type_input = event.get("feedback_type", None)
        feedback_text = event.get("feedback_text", None)
            
        # Validate input
        validation_error = validate_input(event)
        if validation_error:
            logger.info(f"validation_error")
            # return validation_error
            raise UserFeedbackException(message='One or More required fields are missing.',reason=Reason.MISSING_REQUIRED_FIELD, field_errors= validation_error )
        else :
            logger.info(f"validation successful")
            
        # # Extract user fields
        #     user_session_id = event.get("user_session_id", None)
        #     prompt_id = event.get("prompt_id", None)
        #     conversation_id = event.get("conversation_id", None)
        #     feedback_source = event.get("feedback_source", None)
        #     feedback_type_input = event.get("feedback_type", None)
        #     feedback_text = event.get("feedback_text", None)
            
            # parse the feedback type for multiple values
            if feedback_type_input:
                # Split the comma-separated string into a list
                feedback_type_list = [feedback_type.strip() for feedback_type in feedback_type_input.split(',') if feedback_type]
                
            

            # Database connection parameters from environment variables
            db_host = os.environ.get('DB_HOST')
            db_name = os.environ.get('DB_NAME')
            db_schema = os.environ.get('DB_SCHEMA')
            db_port = os.environ.get('DB_PORT')
                    
            
            # Retrieve secret
            secret_name = os.environ.get('secret_name')
            secret, status_code = retrieve_secret(secret_name)
            
            if status_code != 200:
                # return {
                #     "statusCode": status_code,
                #     "body": json.dumps(secret)
                # }
                raise UserFeedbackException(message="Secret retrieval failed.", reason=Reason.SECRET_RETRIEVAL_FAILURE, field_errors=[])
                    
            if secret:
                # Access individual secret values
                db_user = secret.get('username')
                db_password = secret.get('password')
                logger.info(f"successfully connected to secrets")
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
                raise UserFeedbackException(message='One or more required database connection parameters are missing',reason=Reason.MISSING_DB_PARAM, field_errors= missing_vars)
            
           
            # Connect to the PostgreSQL database
            try :
                conn = psycopg2.connect(
                        host=db_host,
                        database=db_name,
                        user=db_user,
                        password=db_password,
                        port=db_port,
                        options="-c search_path=dbo,"+db_schema
                    )
                cursor = conn.cursor()
            except psycopg2.OperationalError as e:
                raise UserFeedbackException(
                    message="Database connection error.",
                    reason=Reason.DATABASE_CONNECTION_FAILURE,
                    field_errors=str(e)
                )
            except psycopg2.Error as e:
                raise UserFeedbackException(
                    message="Database operation error.",
                    reason=Reason.DATABASE_CONNECTION_FAILURE,
                    field_errors=str(e)
    )    
            # Log successful connection
            log_message = {
                "user_session_id": user_session_id,
                "prompt_id": prompt_id,
                "conversation_id": conversation_id,
                "log_category": "status",
                "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
                "level": "INFO",
                "message": "Database connection successful",
                "module_name": "User Session Feedback"
            }
            
            # print('log_message-',log_message)
            try:
                send_log_to_sqs(log_message)
                logger.info("Database connection successful")
            except Exception as e:
                logger.info(f"Failed to send log to SQS: {str(e)}")
                raise UserFeedbackException(message="Failed to send log to SQS.",reason=Reason.LOG_SQS_FAILURE, field_errors= str(e))
            
            
             # Validate user data against the database
            validation_error = validate_request_data(cursor, user_session_id, prompt_id, conversation_id, feedback_source, feedback_type_input, feedback_text)
            if validation_error:
                logger.info(f"Request validation error")
                # return validation_error
                raise UserFeedbackException(message="Request validation error.",reason=Reason.INVALID_INPUT_TYPE, field_errors= validation_error)
            else:
                logger.info(f"Request validation successful")    
                
            
            # Validate feedback category
            is_valid_feedback_type, invalid_feedback_types_str=validate_feedback_type(feedback_type_list)
            if not is_valid_feedback_type:
                raise UserFeedbackException(message="Invalid Feedback Type.",reason=Reason.INVALID_FEEDBACK_TYPE, field_errors= invalid_feedback_types_str)
            
            # Validate feedback source
            if not validate_feedback_source(feedback_source):
                raise UserFeedbackException(message="Invalid Feedback Source.",reason=Reason.INVALID_FEEDBACK_SOURCE, field_errors= feedback_source)
            
            
              
            # Define the SQL queries
            select_user_id_query = """
            SELECT user_id 
            FROM user_data.user_session 
            WHERE user_session_id = %s
            """

            # Define the SQL INSERT statement
            insert_query = """
            INSERT INTO user_data.user_session_feedback (
                user_session_id, 
                prompt_id, 
                conversation_id, 
                feedback_source, 
                feedback_type, 
                feedback_text,
                user_id
            ) VALUES (%s, %s, %s, %s, %s, %s,%s) RETURNING id;
            """

            # Fetch the user_id from user_data.user_session
            cursor.execute(select_user_id_query, (user_session_id,))
            user_id = cursor.fetchone()
            
            # Check if user_id was found
            if user_id is None:
                # raise ValueError(f"No user found with user_session_id: {user_session_id}")
                raise UserFeedbackException(message="Invalid Session Id.",reason=Reason.INVALID_SESSION_ID, field_errors= user_session_id)

            user_id = user_id[0]  # Extract user_id from the result tuple
            
            
            # Tuple of values to be inserted
            values = (user_session_id, prompt_id, conversation_id, feedback_source, feedback_type_input, feedback_text, user_id)
            
            # Execute the INSERT statement
            cursor.execute(insert_query, values)   
            
            # Fetch the returned feedback_id
            session_feedback_id = cursor.fetchone()[0]  # Extract the ID from the result
            
            
            # Commit the transaction
            conn.commit()  

            # Fetch the feedback_id from user_session_feedback
            logger.info(f"New feedback id  created : {str(session_feedback_id)}")    
            
            # Validate session_id
            if not session_feedback_id:
                logger.error(f"session_feedback_id ID not found  : ",session_feedback_id)
                raise Exception("session_feedback_id ID not found")

            end_time = datetime.utcnow()  # Record the end time
            execution_time = (end_time - start_time).total_seconds()  # Calculate the execution time

            # Log successful session creation with execution time
            log_message = {
                "user_session_id": user_session_id,
                "prompt_id": prompt_id,
                "conversation_id": conversation_id,
                "log_category": "status",
                "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
                "level": "INFO",
                "message": "Feedback has been successfully recorded.",
                "session_feedback_id": session_feedback_id,
                "execution_time_seconds": execution_time,
                "module_name": "User Session Feedback"
                
            }
            try:
                send_log_to_sqs(log_message)
                logger.info(f"Feedback has been successfully recorded with session_feedback_id {session_feedback_id}.")
            except Exception as e:
                logger.info(f"Failed to send log to SQS: {str(e)}")
                raise UserFeedbackException(message="Failed to send log to SQS.",reason=Reason.LOG_SQS_FAILURE, field_errors= str(e))

            # Log metrics
            log_message["log_category"] = "metrics"
            try:
                send_log_to_sqs(log_message)
            except Exception as e:
                logger.error(f"Failed to send log to SQS: {str(e)}")
                raise UserFeedbackException(message="Failed to send log to SQS.",reason=Reason.LOG_SQS_FAILURE, field_errors= str(e))

            return {
                "statusCode": 200,
                "body": {
                    "code": "success",
                    "message": "Feedback has been successfully recorded.",
                    "session_feedback_id": session_feedback_id
                    
                }
            }
            
    except UserFeedbackException as e:
            logger.error(f"User feedback error: {e.message}")
            log_message = {
                "user_session_id": user_session_id,
                    "prompt_id": prompt_id,
                    "conversation_id": conversation_id,
                    "log_category": "status",
                    "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
                    "level": "ERROR",
                    "message": e.message,
                    "module_name": "User Session Feedback"
                }
            try:
                send_log_to_sqs(log_message)
                logger.error( e.message)
            except Exception as e:
                logger.error(f"Failed to send log to SQS: {str(e)}")
                raise UserFeedbackException(message="Failed to send log to SQS.",reason=Reason.LOG_SQS_FAILURE, field_errors= str(e))
            # return e.get_response_data()
            return {
                "statusCode": e.get_response_data()['code'],
                "body": {
                    "code": e.get_response_data()['status'],
                    "message": e.get_response_data()['message'],
                    "error_info": e.get_response_data()['error_info']
                }
            }

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        log_message = {
                "user_session_id": user_session_id,
                    "prompt_id": prompt_id,
                    "conversation_id": conversation_id,
                    "log_category": "status",
                    "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
                    "level": "ERROR",
                    "message": e,
                    "module_name": "User Session Feedback"
                }
        try:
            send_log_to_sqs(log_message)
            logger.error( e.message)
        except Exception as e:
            logger.error(f"Failed to send log to SQS: {str(e)}")
            raise UserFeedbackException(message="Failed to send log to SQS.",reason=Reason.LOG_SQS_FAILURE, field_errors= e)
        
        return {
            "statusCode": 500,
            "body": {
                "code": "error",
                "message": "An unexpected error occurred.",
                "error_info": str(e)
            }
        }

    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
