import json
import time
import logging
from lib.utils import exchange_authorization_code_for_token, connect_to_snowflake,update_session_table

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    """
    Lambda function handler for generating a Snowflake OAuth token using the provided authorization code.
    
    Args:
        event (dict): Incoming request data containing 'user_email' and 'authorization_code'.
        context (object): AWS Lambda context object (not used).
    
    Returns:
        dict: API response containing the Snowflake OAuth token or an error message.
    """
    user_email = event.get('user_email')
    authorization_code = event.get('authorization_code')
    session_id= event.get('session_id')
    
    logger.info("Received request for user_email: %s", user_email)

    if not user_email or not authorization_code or not session_id:
        logger.error("Missing required parameters.")
        return {
            "statusCode": 400,
            "status": "error",
            "message": "Missing required parameters.",
            "data": {
                "user_email": user_email,
                "error_code": "missing_parameters",
                "error_description": "Required parameters are missing in the request."
            }
        }
    
    try:
        # Exchange the authorization code for an OAuth token.
        logger.info("Generating Snowflake access")
        oauth_token = exchange_authorization_code_for_token(authorization_code)
        
        # Connect to Snowflake using the OAuth token.
        logger.info("Connecting to Snowflake.")
        snowflake_conn = connect_to_snowflake(user_email,oauth_token)
        
        # Execute a query to verify the connection.
        logger.info("Executing query to verify Snowflake connection.")
        query_result = snowflake_conn.cursor().execute("SELECT CURRENT_TIMESTAMP()").fetchone()
        # issued_at = query_result[0]
        

        try:
            update_session_table(user_email, session_id)
            logger.info("Session table updated.")
        except Exception as e:  
            logger.info("Failed to update session table.")
            logger.error("Snowflake connection failed: %s", str(e))
            return {
                "statusCode": 500,
                "status": "error",
                "message": "Snowflake connection failed.",
                "data": {
                    "user_email": user_email,
                    "error_code": "Failed to update session table for Snowflake connection issue.",
                    "error_description": str(e)
                }
            }
        
        # Return success response .
        logger.info("Snowflake access successfully generated for user_email: %s", user_email)
        return {
            "statusCode": 200,
            "status": "success",
            "message": "Snowflake access generated successfully.",
            "data": {
                "user_email": user_email,
                "snowflake_oauth_token": oauth_token,
                # "issued_at": issued_at
            }
        }
    
    except ValueError as ve:
        # Handle invalid authorization code
        logger.error("Invalid authorization code provided: %s", str(ve))
        return {
            "statusCode": 400,
            "status": "error",
            "message": "Invalid authorization code provided.",
            "data": {
                "user_email": user_email,
                "error_code": "Invalid authorization code",
                "error_description": str(ve)
            }
        }
    
    except Exception as e:
        # Handle general errors
        logger.error("Snowflake connection failed. : %s", str(e))
        return {
            "statusCode": 500,
            "status": "error",
            "message": "Snowflake connection failed.",
            "data": {
                "user_email": user_email,
                "error_code": "Invalid OAuth access token",
                "error_description": str(e)
            }
        }
