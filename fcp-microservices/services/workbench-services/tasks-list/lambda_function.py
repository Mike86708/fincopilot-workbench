import psycopg2
import json
import os
import boto3
import traceback
import datetime
from botocore.exceptions import ClientError
import configparser
from tasks_list_exceptions import TasksListException, Reason
from psycopg2 import Error

"""
Lambda entry point for getting list of tasks

Note: Please use Lambda Proxy integration , else event will not 
be triggered.
Environment Vars
    postgres_database  
    postgres_host 
    postgres_port 
    region 
    secret_arn 
Layers Used
   Layer for psycopg2 on python 3.8    
"""
def lambda_handler(event, context):
    try:
        # Initialize to default values
        version = 1
        entity_id = -1000
        period_id = -1000
        folder_id = -1000
        description = ''
        task_status = -1000
        assigned_performer_id = -1000
        tags = ''
        # Check if required fields are received
        entity_id = CheckIfRequiredInputFieldExists(event, 'entity_id', "integer")
        period_id = CheckIfRequiredInputFieldExists(event, 'period_id', "integer")
        folder_id = CheckIfRequiredInputFieldExists(event, 'folder_id', "integer")
        description = CheckIfRequiredInputFieldExists(event, 'description', "string")
        task_status = CheckIfRequiredInputFieldExists(event, 'task_status', "integer")
        approval_status = CheckIfRequiredInputFieldExists(event, 'approval_status', "integer")
        assigned_performer_id = CheckIfRequiredInputFieldExists(event, 'assigned_performer_id', "integer")
        tags = CheckIfRequiredInputFieldExists(event, 'tags', "string")
        taskList = get_tasks_with_filters(entity_id, period_id, folder_id, description, task_status, approval_status, assigned_performer_id, tags)
        return taskList
    except TasksListException as e:
        print(f"Error in lambda_handler: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps(e.get_response_data(), indent=2),
        }
    except Exception as e:
        # Catch any other unforeseen exceptions
        print(f"Unexpected error in lambda_handler: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "An unexpected error occurred.",
                    "details": str(e),
                },
                indent=2,
            ),
        }
    return response

"""
Method returns if a required request field exists, if not defaults to None
"""
def CheckIfRequiredInputFieldExists(event, fieldName, fieldTypeDescription):
    try:
        fieldNameVal = event.get(fieldName)
        # if fieldNameVal is None:
        #      raise TasksListException(
        #             f"Invalid or missing '{fieldName}'. It must be of type {fieldTypeDescription}.",
        #             reason=Reason.INVALID_INPUT,
        #             subcomponent="lambda_handler",
        #         ) 
        if(fieldTypeDescription == "integer") and fieldNameVal == -1000:
             return None
        elif fieldNameVal == '':
             return None
        return fieldNameVal       
    except Exception as e:
        raise TasksListException(
                    f"Invalid or missing '{fieldName}'. It must be of type {fieldTypeDescription}.",
                    reason=Reason.INVALID_INPUT,
                    subcomponent="lambda_handler",
                ) 
            
"""
Method returns the credentials for postgres
"""
def getCredentials():  
    credential = {}

    secret_name = GetEnvironmentVariableValue('secret_arn')
    region_name = GetEnvironmentVariableValue('region')
    credential['host'] = GetEnvironmentVariableValue('postgres_host')
    credential['db'] = GetEnvironmentVariableValue('postgres_database')
    credential['port'] = GetEnvironmentVariableValue('postgres_port')

    try:
        client = boto3.client(
            service_name='secretsmanager',
            region_name=region_name
            )      
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)     
        secret = json.loads(get_secret_value_response['SecretString'])
        #capture all the connection string parameters
        credential['username'] = secret['username']
        credential['password'] = secret['password']
    except ClientError as e:
        raise TasksListException(
                "Failed to retrieve secret from AWS Secrets Manager",
                reason=Reason.SECRETS_MANAGER_ERROR,
                subcomponent="get_balancesummary"
            ) from e
    return credential

"""
Method returns the value of the passed environment variable.
It throws an exception if key is missing
"""
def GetEnvironmentVariableValue(required_env_key):
        env_key_val = os.getenv(required_env_key)
        if env_key_val is None:
            raise TasksListException(
                f"Missing '{required_env_key}' in environment variables",
                reason=Reason.MISSING_KEY,
                subcomponent="getCredentials"
            )
        return env_key_val
    
    

"""
This method gets the workbench tasks based on the 
filters provided. By default all tasks are returned
"""
def get_tasks_with_filters(entity_id, period_id, folder_id, description, task_status, approval_status, assigned_performer_id, tags):
    credential = getCredentials()
    #connect to postgres
    connection = psycopg2.connect(user=credential['username'], 
                                  password=credential['password'], 
                                  host=credential['host'], 
                                  database=credential['db'],
                                  port = credential['port'])    
    try:
        with connection.cursor() as cursor:
            cursor.callproc('workbench.get_tasks_with_filters',(entity_id, period_id, folder_id, description, task_status, approval_status, assigned_performer_id, tags)) 
            column_names = []
            taskList = []
            column_names = [desc[0] for desc in cursor.description]
            for row in cursor:
                task_data = {}
                for count, column_name in enumerate(column_names):
                    if isinstance(row[count], datetime.date):
                        task_data[column_name] = row[count].strftime('%Y-%m-%d')
                    else:
                        task_data[column_name] = row[count]
                taskList.append(task_data)  
            return taskList
    except Exception as e:
         raise TasksListException(
            "An unexpected database error occurred while getting tasks list",
            reason=Reason.DATABASE_EXECUTION_ERROR,
            subcomponent="get_trial_balance_summary",
        ) from e                                 
