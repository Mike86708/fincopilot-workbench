'''
    This file is where we host the actual calls in. The functions below make the requests and return what we need.
'''
import json
import requests
import boto3
from botocore.exceptions import ClientError
from fastapi import HTTPException


def get_filters(): # Get's task filters
    try:
        r = invoke_lambda_function(function_name="fincopilot_workbench_tasks_filters_dev_autodeploy_lambda")
        # ^ Simply make a request to the API Endpoint for filters
        filters = json.loads(r)
    except requests.exceptions.HTTPError as errh:
        return errh.args[0]
    return filters

def get_tasks(filters): # Get's Tasks
    # Make a id system for task statuses, One unique int for each. Also make it so that it can accept the simply task status as a text,make the API me able to take both task_status, and approval_status
    try:
        body = filters
        body['description'] = body['description'].lower()
        print(body)
        r = invoke_lambda_function(function_name="fincopilot_get_workbench_tasks_dev_autodeploy_lambda",payload=json.dumps(body))
        return json.loads(r)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
def get_lookups(): # Get's lookups
    try:
        r = invoke_lambda_function(function_name="fincopilot_get_workbench_lookups")
        lookups = json.loads(r)
    except requests.exceptions.HTTPError as errh:
        return errh.args[0]
    return lookups

# Helper function for calling lambda functins that do not have an API Endpoint
def invoke_lambda_function(function_name, payload=None):
    # Create a Lambda client
    client = boto3.client('lambda')

    # Convert payload to bytes if it's not None
    if payload is not None:
        payload = bytes(payload, 'utf-8')

    try:
        # Invoke the Lambda function
        response = client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',  # 'Event' for async invocation
            Payload=payload if payload is not None else b''
        )

        # Read and decode the response payload
        response_payload = response['Payload'].read().decode('utf-8')
        return response_payload

    except ClientError as e:
        # Handle errors related to the Lambda invocation
        return(f"ClientError: {e}")

    except Exception as e:
        # Handle any other exceptions
        return(f"Exception: {e}")