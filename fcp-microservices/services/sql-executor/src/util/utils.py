import json
import logging
import re
import csv
import io
import boto3
import botocore.exceptions
import os
from exceptions.exception import SQLExecutionException
from exceptions.exception_codes import Reason

environment = os.getenv('ENVIRONMENT',None)
# print('environment-',environment)
if environment == 'development':
    from dotenv import load_dotenv
    load_dotenv()

def get_config():
    """
    Load required environment variables for configuration.

    Returns:
    - dict: Dictionary containing all required environment variables.

    Raises:
    - SQLExecutionException: If any required environment variables are missing.
    """
    
  # List of required environment variables
    required_vars = ["service_authentication", "db_type", "snowflake_account", "snowflake_database", "snowflake_schema", "snowflake_warehouse", "log_sqs_url", "enforce_query_row_limit", "max_query_row_count", "enforce_response_row_limit", "max_response_rows", "s3_bucket", "s3_path", "rule_group_name", "formatting_json_filename", "query_tag", "service_secret_name","user_name"]

    # Load environment variables into a dictionary and check for missing ones
    config = {}
    missing_vars = []

    for var in required_vars:
        value = os.getenv(var)
        if value is None:
            missing_vars.append(var)
        else:
            config[var] = value
    
    # Create a string containing all missing environment variables
    missing_vars_str = ', '.join(missing_vars)
    
    # Raise an exception if any variables are missing
    if missing_vars:
        # raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")
        raise SQLExecutionException(message="Missing required environment variables.", reason=Reason.MISSING_CONFIGURATION, field_errors=missing_vars_str)
    return config
    

# def get_config():
#     """
#     Load configuration from dal_sf_config.json file.

#     Returns:
#     - dict: Configuration dictionary.
#     """
#     try:
#         with open('dal_sf_config.json', 'r') as f:
#             config_data = json.load(f)
#         return config_data

#     except FileNotFoundError:
#         logging.error("Config file 'dal_sf_config.json' not found.")
#         # raise RuntimeError("Config file 'dal_sf_config.json' not found.")
#         raise SQLExecutionException(message="Config file 'dal_sf_config.json' not found.", reason=Reason.MISSING_CONFIGURATION,field_errors='FileNotFoundError')
    
#     except KeyError as e:
#         logging.error(f"Missing key in config.json: {e}")
#         # raise RuntimeError(f"Missing key in config.json: {e}")
#         raise SQLExecutionException(message="Missing key in config.json.", reason=Reason.MISSING_CONFIGURATION, field_errors=str(e))
    
#     except json.JSONDecodeError as e:
#         logging.error(f"Error decoding JSON from config file: {e}")
#         # raise RuntimeError("Error decoding JSON from config file.")
#         raise SQLExecutionException(message="Error decoding JSON from config file.", reason=Reason.INVALID_CONFIGURATION, field_errors=str(e))



def get_user_creds():
    """
    Retrieve Snowflake user credentials from the configuration file.

    Returns:
    - dict: Dictionary containing Snowflake user credentials.

    Raises:
    - SQLExecutionException: If the config file is not found or there is an error in retrieving user credentials.
    """
    try:
        # Open and load the configuration file
        with open('dal_sf_config.json', 'r') as f:
            config_data = json.load(f)

        # Extract user credentials from the config data
        user_creds = {
            "client_id": config_data["client_id"],
            "client_secret": config_data["client_secret"],
            "jws_key": config_data["jws_key"],
            "token_endpoint": config_data["token_endpoint"],
            "snowflake_account": config_data["snowflake_account"],
            "snowflake_database": config_data["snowflake_database"],
            "snowflake_schema": config_data["snowflake_schema"],
            "snowflake_warehouse": config_data["snowflake_warehouse"],
        }

        return user_creds

    except FileNotFoundError:
        logging.error("Config file 'dal_sf_config.json' not found.")
        # raise RuntimeError("Config file 'dal_sf_config.json' not found.")
        raise SQLExecutionException(message="Config file 'dal_sf_config.json' not found.", reason=Reason.MISSING_CONFIGURATION, field_errors="FileNotFoundError")

    
    except KeyError as e:
        logging.error(f"Missing key in config.json: {e}")
        # raise RuntimeError(f"Missing key in config.json: {e}")
        raise SQLExecutionException(message="Missing key in config.json.", reason=Reason.MISSING_CONFIGURATION, field_errors=str(e))

    
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from config file: {e}")
        # raise RuntimeError("Error decoding JSON from config file.")
        raise SQLExecutionException(message="Error decoding JSON from config file.", reason=Reason.INVALID_CONFIGURATION, field_errors=str(e))

    
    except Exception as e:
        logging.error(f"Error retrieving user credentials: {str(e)}")
        raise RuntimeError("Error retrieving user credentials")


def get_logging_sqs_url():
    """
    Retrieve the SQS URL for logging from environment variables.

    Returns:
    - str: The SQS URL for logging.

    Raises:
    - SQLExecutionException: If the environment variable for SQS URL is missing.
    """
    log_sqs_url=os.getenv("log_sqs_url")
    
    # Check if the SQS URL is present
    if not log_sqs_url:
        raise SQLExecutionException(message="Missing required environment variables.", reason=Reason.MISSING_CONFIGURATION, field_errors="log_sqs_url")
    
    return log_sqs_url
# def get_logging_sqs_url():
#     """
#     Function to retrieve the logging sqs url  from config.json file.

#     Returns:
#     - str: Logging  sqs url .

#     Raises:
#     - RuntimeError: If the config file is not found or there is an error in retrieving the logging Lambda function name.
#     """
#     try:
#         with open('dal_sf_config.json', 'r') as f:
#             config_data = json.load(f)
#         return config_data["log_sqs_url"]
    
#     except FileNotFoundError as e:
#         logging.error("Config file 'dal_sf_config.json' not found.")
#         # raise RuntimeError("Config file 'dal_sf_config.json' not found.")
#         raise SQLExecutionException(message="Config file 'dal_sf_config.json' not found.", reason=Reason.MISSING_CONFIGURATION,field_errors='FileNotFoundError')

    
    # except KeyError as e:
    #     logging.error(f"Missing key in config.json: {e}")
    #     # raise RuntimeError(f"Missing key in config.json: {e}")
    #     raise SQLExecutionException(message="Missing key in config.json.", reason=Reason.MISSING_CONFIGURATION, field_errors=str(e))

    
    # except json.JSONDecodeError as e:
    #     logging.error(f"Error decoding JSON from config file: {e}")
    #     # raise RuntimeError("Error decoding JSON from config file.")
    #     raise SQLExecutionException(message="Error decoding JSON from config file.", reason=Reason.INVALID_CONFIGURATION, field_errors=str(e))

    
    # except Exception as e:
    #     logging.error(f"Error retrieving log_sqs_url: {str(e)}")
    #     # raise RuntimeError("Error retrieving log_sqs_url")
    #     raise SQLExecutionException(message="Error retrieving log_sqs_url.", reason=Reason.MISSING_CONFIGURATION, field_errors=str(e))



def enforce_query_limit(sql_query, max_query_row_count):
    """
    Enforce a row limit on a SQL SELECT query by adding or modifying the LIMIT clause.

    Args:
    - sql_query (str): SQL query to enforce the limit on.
    - max_query_row_count (int): Maximum number of rows allowed in the query result.

    Returns:
    - str: Modified SQL query with the enforced LIMIT clause, or the original query if not applicable.
    """
   
    # Check if the query is a SELECT query
    if sql_query.strip().lower().startswith("select"):
        # Search for an existing LIMIT clause
        limit_match = re.search(r'limit\s+(\d+)', sql_query, flags=re.IGNORECASE)
        current_limit = int(limit_match.group(1)) if limit_match else None

        if current_limit is None:
            # Remove any trailing semicolon
            sql_query = sql_query.rstrip(';')
            # Add LIMIT clause if not present
            sql_query += f" LIMIT {max_query_row_count}"
        elif current_limit > int(max_query_row_count):
            # Modify existing LIMIT clause if it exceeds the max_query_row_count
            sql_query = re.sub(r'limit\s+\d+', f'LIMIT {max_query_row_count}', sql_query, flags=re.IGNORECASE)

    return sql_query

import decimal
from datetime import datetime,timezone
class CustomJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle Decimal and datetime objects.
    """
    def default(self, o):
        # Convert Decimal objects to strings
        if isinstance(o, decimal.Decimal):
            return str(o)
        # Convert datetime objects to ISO 8601 format
        elif isinstance(o, datetime):
            return o.replace(tzinfo=timezone.utc).isoformat()
        return super().default(o)



def send_log_to_sqs(sqs_client,log_data):
    """
    Send log data to an SQS queue.

    Args:
    - sqs_client (boto3.client): Boto3 SQS client object.
    - log_data (dict): Log data to be sent to the queue.

    Returns:
    - dict: Response from SQS service.

    Raises:
    - SQLExecutionException: If the SQS send message operation fails.
    """
    try:
        sqs_url = get_logging_sqs_url()
        response = sqs_client.send_message(
            QueueUrl=sqs_url,
            MessageBody=json.dumps(log_data, cls=CustomJSONEncoder)
        )
        return response
    except Exception as e:
        logging.error(f"Failed to send log to SQS: {e}")
        # raise RuntimeError("Failed to send log to SQS")
        raise SQLExecutionException(message="Failed to send log to SQS.", reason=Reason.SQS_SEND_MESSAGE_FAILED, field_errors=str(e))

    
    
def save_to_s3_presigned_url(s3_client, data, bucket,subject_area, user_name,prompt_id, s3_path, expiration=18000):
    """
    Save data as a CSV file to an S3 bucket and return a presigned URL for access.

    Args:
    - s3_client (boto3.client): Boto3 S3 client object.
    - data (dict): Data to be saved, with columns and rows.
    - bucket (str): S3 bucket name.
    - user_name (str): User name to be included in the file name.
    - prompt_id (str): Identifier to be included in the file name.
    - s3_path (str): Path in the S3 bucket where the file will be saved.
    - expiration (int): URL expiration time in seconds (default 5 hours).

    Returns:
    - tuple: (presigned_url, file_name) - The presigned URL and the filename of the saved CSV.

    Raises:
    - SQLExecutionException: If saving to S3 fails.
    """
    try:
        # Create a CSV in memory
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        
        # Write the column headers
        csv_writer.writerow(data['columns'])
        
        # Write the rows
        csv_writer.writerows(data['rows'])
        
        # Get the CSV string
        csv_string = csv_buffer.getvalue()
        
        # Generate today's date folder name in yyyy-mm-dd format
        today_date_folder = datetime.now().strftime('%Y-%m-%d')
        
        # # Create the filename pattern <conversion_id>_datetimestamp.csv
        # timestamp = datetime.now().strftime('%Y-%m-%dT%H%M%S')
        # user_name=user_name[:4] #limit username to first 8 chars
        # file_name = f"fincopilot_{user_name}_{prompt_id}_{timestamp}.csv"
        
        # Generate today's date in YYYYMMDD format
        today_date = datetime.now().strftime('%Y%m%d')
        # Create the filename pattern <subject_area>-<prompt_id>-YYYYMMDD.csv
        file_name = f"fincopilot-{subject_area}-{prompt_id}-{today_date}.csv"
        
        # Construct the S3 key
        s3_key = f"{s3_path.strip('/')}/{today_date_folder}/{file_name}"
        
        # Save to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_string.encode('utf-8'),
            ContentType='text/csv'
        )
        
        # Generate presigned URL for download
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': s3_key},
            ExpiresIn=expiration
        )
        
        return presigned_url,file_name
    
    except Exception as e:
        logging.error(f"Failed to save CSV to S3: {e}")
        # raise RuntimeError("Failed to save CSV to S3")
        raise SQLExecutionException(message="Failed to save CSV to S3.", reason=Reason.S3_UPLOAD_FAILED, field_errors=str(e))




import pandas as pd
import numpy as np

def format_data_for_ui(data):
    """
    Replace all NaN, nan, and None values in the data with empty strings.

    Parameters:
        data (dict): The data dictionary to process.

    Returns:
        dict: The processed data dictionary with NaN, nan, and None values replaced by empty strings.
    """
    # Convert the data to a DataFrame
    df = pd.DataFrame(data['rows'], columns=data['columns'])
    
    # Replace NaN, None, and nan values with empty strings
    df.replace([np.nan, None], '', inplace=True)
    
    # Convert the DataFrame back to dictionary format
    processed_data = {
        'columns': df.columns.tolist(),
        'rows': df.values.tolist()
    }
    
    return processed_data


#----------------------------------

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
        logging.error(f"Error retrieving secret: {e}")
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

#-------------------------------------------------
def build_error_response(s3_export,execution_mode,e=None):
    """
    Constructs an error response dictionary based on the provided exception.

    Args:
    - s3_export (bool): A flag indicating whether the error response involves S3 export.
    - execution_mode (str): The current execution mode, which may affect the error response.
    - e (Exception, optional): The exception object that triggered the error response. 
                            If not provided, a generic error response is returned.

    Returns:
    - dict: A dictionary containing the error response. 
    """
    # Check if the exception is an instance of SQLExecutionException
    if isinstance(e, SQLExecutionException):
        # Prepare error response from the custom exception
        error_response = {
        "status_code": 400,
        "body": {
            "status": "error",
            "message": "Query execution failed.",
            "error": {
                "code": e.reason.value,  # Assuming 'reason' is an Enum
                "message": e.message,#str(e.field_errors)
            },
            "data": None,
            "counts": None,
            "s3Export": s3_export,
            "execution_mode": execution_mode
            }
        }
    else:  # Prepare a generic error response
        error_response = {
        "status_code": 400,
        "body": {
            "status": "error",
            "message": "Query execution failed.",
            "error": {
                "code": getattr(e, 'code', "UNKNOWN_ERROR"),
                "message": str(e)
            },
            "data": None,
            "counts": None,
            "s3Export": s3_export,
            "execution_mode": execution_mode
        }
        }
        
    
    return error_response


import json

def create_query_tag(prompt_id,subject_area):
    """
    Generates a query tag for Snowflake queries in JSON format.

    This function creates a structured query tag containing details about the 
    application, prompt ID, subject area, and component responsible for the query.
    The query tag is returned as a JSON string.

    Args:
        prompt_id (str or int): The unique identifier for the prompt, which helps 
                                track and categorize the query.

    Returns:
        str: A JSON string representing the query tag with key details about 
             the application, prompt, subject area, and component.
    """
    
    # Define the query tag details in a dictionary
    query_tag = {
        "application_name": "fincopilot",  # Name of the application issuing the query
        "prompt_id": prompt_id,     # Unique identifier for the prompt
        "subject_area": subject_area,       # Subject area related to the query, e.g., Accounts Receivable (AR)
        "component": "SQL Executor" # Component responsible for executing the query
    }

    # Convert the dictionary to a JSON string and return it
    return json.dumps(query_tag)


def normalize_subject_area(subject_area):
    """
    Normalize the subject_area to 'AR' or 'AP' based on predefined values.
    
    Args:
    - subject_area (str or None): The input subject area to be normalized.
    
    Returns:
    - str: Normalized subject area. Returns 'AR' if input matches 'accounts receivable' or 'AR',
           'AP' if input matches 'accounts payable' or 'AP', and returns 'AR' if input is None or null.
    """
    # Check if the input is None or null
    if subject_area is None:
        return 'AR'
    
    # Normalize the input by converting to lower case and removing extra spaces
    normalized_area = ' '.join(subject_area.lower().strip().split())
    
    # Define the valid subject areas for AR and AP
    ar_subject_areas = ['accounts receivable', 'ar']
    # ap_subject_areas = ['accounts payable', 'ap']
    
    # Check if the normalized input matches AR subject areas
    if normalized_area in ar_subject_areas:
        return 'AR'
    
    # # Check if the normalized input matches AP subject areas
    # if normalized_area in ap_subject_areas:
    #     return 'AP'
    
    # Return the original subject_area if no match is found
    return subject_area