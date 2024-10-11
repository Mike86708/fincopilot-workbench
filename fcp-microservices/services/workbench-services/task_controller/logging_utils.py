import os
from time import time
import json
from enum import Enum 
import logging
import boto3
from dotenv import load_dotenv 
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def log_to_sqs(service_name,source_name,log_level,log_type,message,log_info):
    # Start session
    session = boto3.session.Session()
    # Initialize the SQS client
    client = session.client(
        service_name='sqs',
        region_name=os.getenv('REGION_NAME')  # Replace with your desired region
    )
    message_body = {
        "service_name": service_name,
        "source_name": source_name,
        "timestamp": time(),
        "log_level": log_level,
        "log_type": [
            log_type
        ],
        "message": message,
        "log_info": log_info
    }
    str_message = json.dumps(message_body)
    queue_url = os.getenv('SQS_URL')
    try:
        response = client.send_message(
            QueueUrl=queue_url,
            MessageBody=str_message,
        )
        logger.info(f'Message sent! Message ID: {response["MessageId"]}')
        return response  # Return the response for further processing
    except Exception as e:
        logger.error(f'Failed to send message: {e}')
