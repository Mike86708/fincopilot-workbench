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
    try:
        task_id = event['task_id']
    except:
        return {"status_code":400, "message":"Field task_id is required!"}
    
    try:
        task_status = event['task_status']
    except Exception as e:
        # optional field, defaulted to 0
        print(str(e)+"task_status not provided, defaulting to 0")
        task_status = 0
    try:
        approval_status = event['approval_status']
    except Exception as e:
        # optional field, defaulted to 0
        print(str(e)+": approval_status not provided, defaulting to 0")
        approval_status = 0

    try:
        actual_performer_id = event['actual_performer_id']
    except:
        return {"status_code":400,"message":str(e)+": actual_performer_id not found"}
    
    try:
        actual_approver_id = event['actual_approver_id']
    except:
        return {"status_code":400,"message":str(e)+": actual_performer_id not found"}
    

    try:
        performer_actual_completion_date = event['performer_actual_completion_date']
        approver_actual_completion_date = event['approver_actual_completion_date']
    except Exception as e:
        return {"status_code":400,"message":str(e)+": performer_actual_completion_date or approver_actual_completion_date not found"}
    
    if is_valid_date(performer_actual_completion_date) and is_valid_date(approver_actual_completion_date):
        pass
    else:
        print(performer_actual_completion_date)
        return {"status_code": 400, "message":"Either performer_actual_completion_date or performer_actual_completion_date is not a valid date in yyyy-mm-dd"}
    


    # Run the actual query
    return update_task_info(task_id=task_id,task_status=task_status,
                     approval_status=approval_status,
                     actual_performer_id=actual_performer_id,
                     performer_actual_completion_date=performer_actual_completion_date,
                    actual_approver_id=actual_approver_id,approver_actual_completion_date=approver_actual_completion_date)
    

    
    
"""
This method checks if a string is a valid date in yyyy-mm-dd format
"""
def is_valid_date(date_str):
    try:
        var = bool(datetime.datetime.strptime(date_str, '%Y-%m-%d'))
        return True
    except ValueError:
        return False
    


def update_task_info(task_id, task_status, approval_status,
                     actual_performer_id, performer_actual_completion_date,
                     actual_approver_id, approver_actual_completion_date):
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        user=os.getenv("DB_USER"),
        port=os.getenv("PORT"),
        password=get_db_creds(),
        database=os.getenv("DATABASE")
    ) 

    with conn.cursor() as cursor:

        # Case 1: Task Status = 1
        if task_status == 1:
            if actual_performer_id < 0:
                print(actual_performer_id)
                return {"status_code": 400, "message": "Invalid request - actual_performer_id < 0"}
            else:
                try:
                    print("LINE 115")
                    print("actual_performer_id " +str(actual_approver_id))
                    print("performer_actual_completion_date "+performer_actual_completion_date)
                    print("task_id "+str(task_id))
                    cursor.callproc('workbench.update_completed_task_info',(actual_performer_id, performer_actual_completion_date, task_id))
                    conn.commit()
                    print("update_completed_task_info has been ran.")
                except Exception as e:
                    print("An error occurred:", e)
                    conn.rollback()  # Rollback in case of error
                    return {"status_code":500, "message":"Error trying to call Procedure workbench.update_completed_task_info: "+str(e)}


        # Case 2: Approval Status = 1
        # We are first going to see if 
        test_valid_query = "SELECT task_status FROM workbench.tasks where id = "+str(task_id)
        if approval_status == 1:
            cursor.execute(test_valid_query)
            current_task_status = -1
            for data in cursor:
                current_task_status = data[0]
            if current_task_status != 1:
                return {"status_code":400, "message":"Invalid Task Approval request. Task is not yet completed(The task status of the selected id is != 1)"}   

            if actual_approver_id < 0:
                return {"status_code":400 , "message":"Invalid actual_approvar_id, it is < 0"}

            try:
                cursor.callproc('workbench.update_approved_task_info',(actual_approver_id, approver_actual_completion_date, task_id)) 
                conn.commit()
                print("workbench.update_approved_task_info has been ran.")
            except Exception as e:
                return {"status_code":500, "message":"Error occured when calling function workbench.update_approved_task_info: "+str(e)}       
    
        return {"status_code":200, "message":"Succses!"}        
       
def get_db_creds():
    secret_name = os.getenv("SECRET_NAME") 
    region_name = os.getenv("REGION_NAME")

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