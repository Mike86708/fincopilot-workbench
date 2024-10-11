import psycopg2
import json
import os
import boto3
import traceback
import datetime
from botocore.exceptions import ClientError
import configparser
from tasks_filters_exceptions import TasksFiltersException, Reason
from psycopg2 import Error
"""
Lambda entry point for getting task filters.

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
        response = get_filters()   
    except TasksFiltersException as e:
        print(f"Error in lambda_handler: {e}")
        response = {
            'statusCode': 500,
            'body': json.dumps(e.get_response_data(), indent=2)
        }
    except Exception as e:
        # Catch any other unforeseen exceptions
        print(f"Unexpected error in lambda_handler: {e}")
        response = {
            'statusCode': 500,
            'body': json.dumps({
                "message": "An unexpected error occurred.",
                "details": str(e)
            }, indent=2)
        }

    return response

            
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
        raise TasksFiltersException(
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
            raise TasksFiltersException(
                f"Missing '{required_env_key}' in environment variables",
                reason=Reason.MISSING_KEY,
                subcomponent="getCredentials"
            )
        return env_key_val
    
    
"""
This method gets the workbench tasks based on the 
filters provided. By default all tasks are returned
"""
def get_filters():
    credential = getCredentials()
    #connect to postgres
    connection = psycopg2.connect(user=credential['username'], 
                                  password=credential['password'], 
                                  host=credential['host'], 
                                  database=credential['db'],
                                  port = credential['port'])    
    cursor = connection.cursor()
    lookupCodeList = []

    try:
        with connection.cursor() as cursor:
            cursor.callproc('workbench.get_lookup_code_values', ())
            # Extract column names
            column_names = [desc[0] for desc in cursor.description]
            # Iterate over rows and populate filter lists
            for row in cursor:
                filter_data = {column_name: row[count] for count,column_name in enumerate(column_names)}
                lookupCodeList.append(filter_data)
    except Error as e:
        raise TasksFiltersException(
            f"Database error occurred : {e}",
            reason=Reason.DATABASE_EXECUTION_ERROR,
            subcomponent="get_filters"
        ) from e
    except TasksFiltersException:
        raise  # Re-raise already defined LookupCodesException
    except Exception as e:
        raise TasksFiltersException(
            "An unexpected error occurred while getting lookup codes",
            reason=Reason.UNEXPECTED_ERROR,
            subcomponent="get_filters"
        ) from e

    return lookupCodeList

def get_filters():
    credential = getCredentials()
    #connect to postgres
    connection = psycopg2.connect(user=credential['username'], 
                                  password=credential['password'], 
                                  host=credential['host'], 
                                  database=credential['db'],
                                  port = credential['port'])    
    # Initialize filter lists
    filters = {
        'entity': [],
        'period': [],
        'folder': [],
        'user': [],
        'tags': [],
        'descriptions': []
    }

    try:
        with connection.cursor() as cursor:
            cursor.callproc('workbench.get_tasks_filters_values', ())
            # Extract column names
            column_names = [desc[0] for desc in cursor.description]
            # Iterate over rows and populate filter lists
            for row in cursor:
                filter_type = row[0]
                filter_data = {column_name: row[count] for count,column_name in enumerate(column_names) if column_name != 'type'}
                filters[filter_type.lower()].append(filter_data)

        with connection.cursor() as cursor:
            cursor.callproc('workbench.get_tags_values', ())
            # Extract column names
            column_names = [desc[0] for desc in cursor.description]
            # Iterate over rows and populate filter lists
            for row in cursor:
                filter_type = row[0]
                filter_data = {column_name: row[count] for count,column_name in enumerate(column_names) if column_name != 'type'}
                filters['tags'].append(filter_data)

        with connection.cursor() as cursor:
            cursor.execute("select distinct(description) from workbench.tasks")
            # Extract column names
            column_names = [desc[0] for desc in cursor.description]
            # Iterate over rows and populate filter lists
            for row in cursor:
                filter_type = row[0]
                filter_data = {column_name: row[count] for count,column_name in enumerate(column_names) if column_name != 'type'}
                filters['descriptions'].append(filter_data)
    except Error as e:
        raise TasksFiltersException(
            f"Database error occurred : {e}",
            reason=Reason.DATABASE_EXECUTION_ERROR,
            subcomponent="get_filters"
        ) from e
    except TasksFiltersException:
        raise  # Re-raise already defined LookupCodesException
    except Exception as e:
        raise TasksFiltersException(
            "An unexpected error occurred while getting lookup codes",
            reason=Reason.UNEXPECTED_ERROR,
            subcomponent="get_filters"
        ) from e

    return filters

                                  
