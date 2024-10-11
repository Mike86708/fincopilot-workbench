import requests
import snowflake.connector
import logging
# from dotenv import load_dotenv
import os
import boto3
import json
import psycopg2
from datetime import datetime, timezone
import base64
import botocore.exceptions

# Configure logging
logger = logging.getLogger(__name__)



def exchange_authorization_code_for_token(authorization_code):
    """
    Exchanges the provided authorization code for an OAuth token using the OAuth 2.0 flow.
    
    Args:
        authorization_code (str): Authorization code received from the OAuth provider.
    
    Returns:
        str: OAuth token.
    
    Raises:
        ValueError: If the authorization code is invalid or expired.
    """
    
    
    
    try:
        logger.info("Attempting to exchange authorization code for OAuth token.")
        # load_dotenv()
        scope = os.environ.get('scope')
        token_endpoint = os.environ.get('token_endpoint')
        redirect_uri = os.environ.get('redirect_uri')
       
               
         # Retrieve secret
        secret_name = os.environ.get('sf_secret_name')
        secret, status_code = retrieve_secret(secret_name)
        
        if status_code != 200:
            return {
                "statusCode": status_code,
                "body": secret
            }
        
        if secret:
            # Access individual secret values
            # authorization_header = secret.get('authorization_header')
            client_id = secret.get('client_id')  
            client_secret = secret.get('client_secret')  
            logger.info(f"successfully connected to secrets")
            
            access_token_string = client_id + ":" + client_secret
            authorization_header = base64.b64encode(bytes(access_token_string, 'utf-8')).decode('utf-8')
        else:
            logger.info("Failed to retrieve the secret.")
            
    
       
        saml_headers = {
            'Authorization': f'Basic {authorization_header}',
            'Accept': 'application/json',
            'cache-control': 'no-cache',
            "content-type": "application/x-www-form-urlencoded"
        }
        data_params = {
            'grant_type':'authorization_code',
            'code':f'{authorization_code}',     
            'scope':f'{scope}',
            'redirect_uri': f'{redirect_uri}',
           }
        response = requests.post(token_endpoint, headers=saml_headers, data=data_params)
        response_data = response.json()
        

        if response.status_code != 200:
            logger.error("OAuth token exchange failed: %s", response_data.get('error_description'))
            raise ValueError(f"{response_data.get('error')}: {response_data.get('error_description')}")

        logger.info("OAuth token exchange successful.")
        return response_data.get('access_token')
    
    except requests.exceptions.RequestException as e:
        logger.exception("Failed to exchange authorization code due to a request exception.")
        raise Exception(f"Failed to exchange authorization code: {str(e)}")



def connect_to_snowflake(user_email,oauth_token):
    """
    Establishes a connection to Snowflake using the provided OAuth token.
    
    Args:
        oauth_token (str): OAuth token for Snowflake authentication.
    
    Returns:
        snowflake.connector.connection.SnowflakeConnection: Snowflake connection object.
    
    Raises:
        Exception: If the connection to Snowflake fails.
    """
    
    
    try:
        # load_dotenv()
        snowflake_account = os.environ.get('snowflake_account')
        snowflake_warehouse = os.environ.get('snowflake_warehouse')
        snowflake_database = os.environ.get('snowflake_database')
        snowflake_schema = os.environ.get('snowflake_schema')
        role = os.environ.get('role')
        
        logger.info("Attempting to connect to Snowflake with the provided OAuth token.")
        conn = snowflake.connector.connect(
            user=user_email,
            account=snowflake_account,
            authenticator="oauth",
            token=oauth_token,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema,
            role=role
        )
        logger.info("Connected to Snowflake successfully.")
        return conn
    
    except snowflake.connector.errors.Error as e:
        logger.exception("Failed to connect to Snowflake due to a Snowflake connector error.")
        raise Exception(f"Failed to connect to Snowflake: {str(e)}")



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
        
        
def update_session_table(user_email, session_id):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)  # Set log level 
    # Load environment variables from .env file
        # load_dotenv()

        # Database connection parameters from environment variables
        db_host = os.environ.get('DB_HOST')
        db_name = os.environ.get('DB_NAME')
        db_schema = os.environ.get('DB_SCHEMA')
        db_port = os.environ.get('DB_PORT')
        
        # Retrieve secret
        secret_name = os.environ.get('pg_secret_name')
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
                "status": "error",
                "message": "Failed to generate Snowflake OAuth token.",
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
            logger.info("Database connection successful")
    
            # Call the stored procedure to update the toekn isssu status in session table
            cursor.execute("CALL user_data.usp_update_user_session(%s, %s, %s)",
                        (user_email, session_id, True))
            
            # Commit the transaction after executing the stored procedure
            conn.commit()

            logger.info(f"Session table updated for session id {session_id}: ")
            
            # return {
            #     "statusCode": 200,
            #     "body": {
            #         "code": "success",
            #         "message": "Session created successfully.",
            #         "session_id": session_id,
            #         "sf_conn_required": sf_conn_required
            #     }
            # }

        except psycopg2.OperationalError as e:
            # Handle database connection errors
            error_message = str(e)
            logger.error(e)
            log_message = {
                "user_email": user_email,
                "session_id": session_id,
                "log_category": "status",
                "timestamp": datetime.utcnow().replace(tzinfo=timezone.utc),
                "level": "ERROR",
                "message": f"Database error: {error_message}",
                "module_name": "Snowflake toke retriever"
            }
            try:
                # send_log_to_sqs(log_message)
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


        finally:
            # Ensure cursor and connection are closed properly
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        