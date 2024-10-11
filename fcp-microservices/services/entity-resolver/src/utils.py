import boto3
from botocore.exceptions import ClientError
from exceptions import *
import json

def get_secret():

    try:
        secret_name = os.environ.get('SECRET_NAME')
        region_name = os.environ.get('REGION_NAME')
    except FileNotFoundError as e: 
        raise EntityResolverException(f"Environment variable not found", Reason.MISSING_MODEL_CONFIG_FILE)

   

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise EntityResolverException(f"Failed to retrieve secret: {secret_name}", Reason.FAILED_TO_RETRIEVE_SECRET)

    secret = json.loads(get_secret_value_response['SecretString'])
    return secret
    