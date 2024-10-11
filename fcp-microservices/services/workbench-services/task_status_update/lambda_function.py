import os
import traceback
import datetime
import json
import configparser
import psycopg2
from psycopg2 import Error
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv, dotenv_values 
load_dotenv()

"""
Lambda entry point for updating task and approval status. 

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
    """Custom lambda handler function. This is the main module that handles Lambda requests on AWS"""
    try:
        task_id = event['task_id']
    except KeyError:
        return {"status_code":400, "message":"Field task_id is required!"}
    
    try:
        task_status = event['task_status']
    except KeyError as e:
        # optional field, defaulted to 0
        print(str(e)+"task_status not provided, defaulting to 0")
        task_status = 0
    try:
        approval_status = event['approval_status']
    except KeyError as e:
        # optional field, defaulted to 0
        print(str(e)+": approval_status not provided, defaulting to 0")
        approval_status = 0

    try:
        actual_performer = event['actual_performer']
    except KeyError as e:
        return {"status_code":400,"message":str(e)+": actual_performer not found"}
    
    try:
        actual_approver = event['actual_approver']
    except KeyError as e:
        return {"status_code":400,"message":str(e)+": actual_performer not found"}
    

    try:
        performer_actual_completion_date = event['performer_actual_completion_date']
        approver_actual_completion_date = event['approver_actual_completion_date']
    except KeyError as e:
        return {"status_code":400,
                "message":str(e)+
                ": performer_actual_completion_date or approver_actual_completion_date not found"}

    if is_valid_date(performer_actual_completion_date) and is_valid_date(approver_actual_completion_date):
        pass
    else:
        print(performer_actual_completion_date)
        return {"status_code": 400,
                "message":"Either performer_actual_completion_date or performer_actual_completion_date is not a valid date in yyyy-mm-dd"}

    # Run the actual query
    return update_task_info(task_id=task_id,task_status=task_status,
                     approval_status=approval_status,
                     actual_performer=actual_performer,
                     performer_actual_completion_date=performer_actual_completion_date,
                    actual_approver=actual_approver,
                    approver_actual_completion_date=approver_actual_completion_date)

def is_valid_date(date_str):
    """
    This method checks if a string is a valid date in yyyy-mm-dd format
    """
    try:
        var = bool(datetime.datetime.strptime(date_str, '%Y-%m-%d'))
        return True
    except ValueError:
        return False
    


def update_task_info(task_id, task_status, approval_status,
                     actual_performer, performer_actual_completion_date,
                     actual_approver, approver_actual_completion_date):
    """Helper function that instantiates a DB Connection, creates a cursor and runs the procedure for us"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("HOST"),
            user=os.getenv("DB_USER"),
            port=os.getenv("PORT"),
            password=get_db_creds(),
            database=os.getenv("DATABASE")
        )
    except ConnectionError as e:
        return{"status_code":500,"message":"Couldn't connect to the DB"}

    with conn.cursor() as cursor:
        try:
            print("Parameters:", actual_performer, performer_actual_completion_date, actual_approver, approver_actual_completion_date, task_id, task_status, approval_status)
            cursor.execute("CALL workbench.usp_update_user_tasks(%s, %s, %s, %s, %s, %s, %s);",
                       (actual_performer, performer_actual_completion_date, actual_approver,approver_actual_completion_date,task_id,task_status,approval_status))
            # cursor.execute("CALL workbench.usp_update_user_tasks('jane.doe@doordash.com', '2024-10-2', 'lisa.clay@doordash.com','2024-09-09', 92, 1, 1)")
            conn.commit()
            print("usp_update_user_tasks has been ran.")
        except Exception as e:
            print("An error occurred:", e)
            conn.rollback()  # Rollback in case of error
            return {"status_code":500, "message":"Error trying to call Procedure workbench.usp_update_user_tasks: "+str(e)}
        
        return {"status_code":200, "message":"Succses!"}        
       
def get_db_creds():
    """This function reads env variables and obtains the DB password from secrets manager. 
    The secret name and region name are in the .env file but the password is from Secrets manager"""
    try:
        secret_name = os.getenv("SECRET_NAME") 
        region_name = os.getenv("REGION_NAME")
    except KeyError as e:
        return {"status_code":500, "message":"Could not obtain an ENV variable: "+str(e)}

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
        # Ensure the secret string is available
        if 'SecretString' in get_secret_value_response:
            username_and_pass = json.loads(get_secret_value_response['SecretString'])
            return username_and_pass['password']
        else:
            return {"status_code":500, "message": "SecretString not found in the response"}
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    username_and_pass = json.loads(get_secret_value_response['SecretString'])
    print("Obtained Username and Password for DB")
    return username_and_pass['password']