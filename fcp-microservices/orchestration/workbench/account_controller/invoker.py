import boto3
from botocore.exceptions import ClientError

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
