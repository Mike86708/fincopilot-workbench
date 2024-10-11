import boto3
from botocore.exceptions import ClientError
import json
import os 
from dotenv import load_dotenv, dotenv_values 
# loading variables from .env file
load_dotenv()



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
    return username_and_pass['password']