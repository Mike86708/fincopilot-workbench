import logging
import json
import time
from datetime import datetime, timezone
import boto3
from snowflake.connector import errors as snowflake_errors
import snowflake.connector
from db.repository_factory import RepositoryFactory
from util.utils import get_config, get_user_creds, enforce_query_limit,send_log_to_sqs, save_to_s3_presigned_url,format_data_for_ui,build_error_response,create_query_tag,normalize_subject_area
from util.data_formatter import DataFormatter
import os

from util.aws_logging_utils import set_api_level_logs, LogLevel, LogType, log_cloudwatch
from util.aws_logging_utils import config as sqs_config

from exceptions.exception import SQLExecutionException
from exceptions.exception_codes import Reason



# Initialize the AWS SQS client
sqs_client = boto3.client('sqs')
# Initialize the S3 client
s3_client = boto3.client('s3')


# service_authentication from environment variables (If True use service account, if False use snowflake token)
service_authentication = os.environ.get('service_authentication')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Set log level 
    
    # Set API Level Logs
    set_api_level_logs(args={
            "service_name": "SnowFlake Executor",
            "source_name": "query_controller",
            "prompt_id": "",
            "conversation_id": "",
        })
        

    start_time = time.time()
    execution_time = 0  # Initialize execution_time 
    error_message = None  # Initialize error_message
    s3_export = {  # Initialize s3_export 
        "exported": False,
        "fileName": None,
        "presignedUrl": None
    }

    try:
        # Extract SQL Query and User Info from API request
        input_data = event.get('input', {})
        sql_query = input_data.get('query_string')
        user_info = input_data.get('user_info', {})
        session = event.get('session',{})
        prompt_id = session.get('prompt_id') 
        execution_mode = input_data.get('execution_mode', '')  
        oauth_token=user_info.get('snowflake_oauth_token') # to be used with okta api
        role=user_info.get('scope')# to be used with okta api
        user_email=user_info.get('username')# to be used with okta api
        
        subject_area = input_data.get('subject_area','')
        subject_area=normalize_subject_area(subject_area)
        
        # push to cloudwatch
        event_copy = event.copy() # Create a copy of the original event
        event_copy['input']['user_info'].pop('snowflake_oauth_token', None) # Remove the key-value pair for 'snowflake_oauth_token' from the copy
        log_cloudwatch(log_type=LogType.SERVICE_INPUT, message="Input received", args=event_copy)
        

        # Retrieve User Credentials (external function call)
        config = get_config()
                
        config["max_query_row_count"]=int(config["max_query_row_count"])
        config["max_response_rows"]=int(config["max_response_rows"])        
        
        # Log user info for tracking purposes
        logger.info(f"User info: {user_info}")

        # Enforce query row limit if required and if execution_mode is not 'sql_only'
        if config["enforce_query_row_limit"] and execution_mode != 'sql_only':
            sql_query = enforce_query_limit(sql_query, config["max_query_row_count"])

        # Measure time to retrieve configuration and credentials
        config_creds_time = int((time.time() - start_time) * 1000)
        logger.info(f"Time to retrieve config and creds: {config_creds_time} ms")

        # Instantiate the appropriate repository using the factory
        db_type = config["db_type"]  # Assuming Snowflake for now; can be parameterized
        repository_start_time = time.time()
        repository = RepositoryFactory.create_repository(db_type, config)
        repository_creation_time = int((time.time() - repository_start_time) * 1000)
        logger.info(f"Time to create repository: {repository_creation_time} ms")
        
        # Execute Query
        execute_query_start_time = time.time()
        # print('service_authentication->',service_authentication)
        
        # create query tag
        query_tag=create_query_tag(prompt_id,subject_area)
        # print('query_tag-',query_tag)
                
        if service_authentication in ['True','true']:
            columns, rows,query_id = repository.execute_query_service_account(sql_query,query_tag) 
            logger.info(f"Using service account to connect to snowflake")
        else:    
            columns, rows,query_id = repository.execute_query_okta(sql_query,oauth_token,role,user_email,query_tag) #with okta api
            logger.info(f"Using users details to connect to snowflake")
            
        execute_query_time = int((time.time() - execute_query_start_time) * 1000)
        logger.info(f"Time to execute query: {execute_query_time} ms")
        logger.info(f"query_id is : {query_id}")
        # print('Result set rows: ',rows)
        
        # Close the database connection
        close_connection_start_time = time.time()
        repository.close_connection()
        close_connection_time = int((time.time() - close_connection_start_time) * 1000)
        logger.info(f"Time to close connection: {close_connection_time} ms")

        # Prepare data for response
        prepare_data_start_time = time.time()
        data = {
            "columns": columns,
            "rows": rows
        }

        
        # Prepare Log data
        log_data = {
            "session_id": event['session']['session_id'],
            "prompt_id": event['session']['prompt_id'],
            "conversation_id": event['session']['conversation_id'],
            "user_id": user_info.get("username", ""),
            "log_category": "status",
            "user_input": sql_query,
            "log_message": "",
            "query_id":"",
            "execution_time_in_ms":"",
            "module_name": "SnowFlake Executor",
            "execution_mode": execution_mode
        }
        
        
        # Data formatting if execution_mode is not 'sql_only'
        if execution_mode != 'sql_only':
            data_formatter = DataFormatter(config["formatting_json_filename"])
            group_name = config["rule_group_name"]
            logger.info(f"Formatting Applied : {group_name}")
            try:
                data = data_formatter.apply_formatting(data, group_name)
                logger.info(f"Formatting completed")
            except Exception as e:
                logger.error(f"Failed to apply formatting: {e}")
                # raise e  # Raise the exception to propagate the error
                raise SQLExecutionException(message='Failed to apply formatting.',reason=Reason.DATA_FORMATTING_ERROR, field_errors= str(e) )

        prepare_data_time = int((time.time() - prepare_data_start_time) * 1000)
        logger.info(f"Time to prepare data: {prepare_data_time} ms")

        # Save data to S3 if execution_mode is not 'sql_only'
        s3_link = None
        if execution_mode != 'sql_only':
            save_to_s3_start_time = time.time()
            try:
                # Save to S3
                s3_link, file_name = save_to_s3_presigned_url(
                    s3_client=s3_client,
                    data=data,
                    bucket=config["s3_bucket"],
                    subject_area=subject_area,
                    user_name=event['input']['user_info']['username'],
                    prompt_id=event['session']['prompt_id'],
                    s3_path=config["s3_path"]
                )
               # Update s3_export
                s3_export["exported"] = True
                s3_export["fileName"] = file_name
                s3_export["presignedUrl"] = s3_link
                logger.info(f"File uploaded successfully to S3: {s3_link}")

                save_to_s3_time = int((time.time() - save_to_s3_start_time) * 1000)
                logger.info(f"Time to save data to S3: {save_to_s3_time} ms")
                
                # Update log
                log_data["query_id"] = query_id if 'query_id' in locals() else None
                log_data["execution_time_in_ms"] = execution_time if 'execution_time' in locals() else None
                log_cloudwatch(log_type=LogType.STATUS, message="S3 Data Upload Successful", args=log_data)
            except RuntimeError as e:
                error_message = str(e)
                logger.error(f"Error: {error_message}")
                
                # update log data
                log_data["log_message"] = error_message if 'error_message' in locals() else None
                log_data["query_id"] = query_id if 'query_id' in locals() else None
                log_data["execution_time_in_ms"] = execution_time if 'execution_time' in locals() else None
                log_cloudwatch(log_type=LogType.STATUS, message="S3 Data Upload Failed", args={"payload": str(e.get_response_data()) },LogLevel=LogLevel.ERROR)
                # print('args :',{"payload": str(e.get_response_data())})
                
                    # Raise the exception to propagate the error
                # raise e
                raise SQLExecutionException(message="Error occurred while saving to S3.", reason=Reason.S3_SAVE_ERROR, field_errors=str(e))
            
        # Enforce response row limit if execution_mode is not 'sql_only'
        response_rows = data["rows"]
        columns = data["columns"]
        if config["enforce_response_row_limit"] and execution_mode != 'sql_only':
            if len(rows) > int(config["max_response_rows"]):
                response_rows = response_rows[:int(config["max_response_rows"])]
                

        # Calculate execution time
        execution_time = int((time.time() - start_time) * 1000)
        logger.info(f"Total execution time: {execution_time} ms")

        # Prepare counts for response (update only if execution_mode is not 'sql_only')
        if execution_mode != 'sql_only':
            counts = {
                "databaseRowCount": "",  # Placeholder for database_row_count
                "s3RowCount": len(data["rows"]),  # Actual count in S3
                "rowsForUI": len(response_rows)
            }
        else:
            counts = {
                "databaseRowCount": "",  # Placeholder for database_row_count
                "s3RowCount": None,
                "rowsForUI": None
            }

        # Prepare response in specified format
        rawdata={}
        rawdata['columns']=columns
        rawdata['rows']=response_rows
        formattedData=format_data_for_ui(rawdata)
        
        response = {
            "status": "success",
            "message": "Query executed successfully.",
            "error": {
                "code": None,
                "message": None
            },
            # "data": {
            #     "columns": columns,
            #     "rowsForUI": response_rows
            # },
            "data": {
                "columns": formattedData['columns'],
                "rowsForUI": formattedData['rows']
            },
            "counts": counts,
            "s3Export": s3_export,
            "execution_mode": execution_mode
        }

       

        # Update to cloud watch
        
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Output generated", args={
            "latency_ms": execution_time, # convert to ms
            "payload": {
            "status_code": 200,
            "body": response,
            "headers": {"Content-Type": "application/json"}
            }
        })
        
        # print('response ',response)   
        # print('response type ',type(response))       
        return {
            "status_code": 200,
            "body": response,
            "headers": {"Content-Type": "application/json"}
            }

    except SQLExecutionException as e:
        # Update to cloud watch
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Output generated", args={
            "latency_ms": execution_time, # convert to ms
            "payload": {
                "status": "error",
                "message": "Query execution failed.",
                "error": {
                    "code": e.get_response_data()['status'],
                    "message": e.get_response_data()['message'],
                    "metadata":e.get_response_data()['error_info']['metadata'],
                },
                "data": None,
                "counts": None,
                "s3Export": s3_export,
                "execution_mode": execution_mode}
        })

        # Log error
        log_cloudwatch(log_level=LogLevel.ERROR,log_type=LogType.SERVICE_OUTPUT, message="Lambda function error", args={"payload": e.get_response_data()})
        
        response = {
            "status": "error",
            "message": "Query execution failed.",
            "error": {
                "code": e.get_response_data()['status'],
                "message": e.get_response_data()['message'],
                "metadata":e.get_response_data()['error_info']['metadata'],
            },
            "data": None,
            "counts": None,
            "s3Export": s3_export,
            "execution_mode": execution_mode
        }
        
        

        # Return  response
        return {
            "status_code": e.get_response_data()['code'],
            "body": response
            }
    
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        error_message = f"Unexpected error : {e} "
        error_code = "UnexpectedError"
        # Update logs
        # log_data["log_message"] = error_message if 'error_message' in locals() else None
        # log_data["query_id"] = query_id if 'query_id' in locals() else None
        # log_data["execution_time_in_ms"] = execution_time if 'execution_time' in locals() else None
        
        log_cloudwatch(log_level=LogLevel.ERROR,log_type=LogType.SERVICE_OUTPUT, message="Lambda function error", args={"payload": str(e)})


        
    # Prepare response in specified format
    response = {
        "status": "error",
        "message": "Query execution failed.",
        "error": {
            "code": error_code,
            "message": error_message
        },
        "data": None,
        "counts": None,
        "s3Export": s3_export,
        "execution_mode": execution_mode
    }

    # Return  response
    return {
        "status_code": 500,
        "body": response
        }    