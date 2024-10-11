'''
    This file is where we host the actual calls in. The functions below make the requests and return what we need.
'''
import json
import requests
import boto3
from botocore.exceptions import ClientError


def getFilters(): # Get's task filters
    try:
        r = requests.get("https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/workbench/get_task_filters") 
        # ^ Simply make a request to the API Endpoint for filters 
    except requests.exceptions.HTTPError as errh:
        return errh.args[0]

    return r.json()

def getTasks(filters): # Get's Tasks
    # Make a id system for task statuses, One unique int for each. Also make it so that it can accept the simply task status as a text,make the API me able to take both task_status, and approval_status
    if not (filters): # This means empty filters, there's none provided
        try:
            body = { # This body sets everything to null or doesn't matter
                "version": 1,
                "entity_id": -1000,
                "period_id": -1000,
                "folder_id": -1000,
                "description": "",
                "task_status": -1000,
                "assigned_performer_id": -1000,
                "approval_status": -1000,
                "tags": ""
            }
            r = requests.post("https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/workbench/task", json=body)
            return r.json()
        except requests.exceptions.HTTPError as errh:
            return errh.args[0]
    
    else:
        try:
            body = filters
            r = requests.post("https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/workbench/task", json=body)
            return r.json()
        except requests.exceptions.HTTPError as errh:
            return errh.args[0]
        

def getLookups(): # Get's lookups
    try:
        r = requests.get("https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/workbench/getlookups")
    except requests.exceptions.HTTPError as errh:
        return errh.args[0]

    rdict = r.json()
    
    return rdict


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