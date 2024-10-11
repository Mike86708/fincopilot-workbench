import json
import time
import boto3
# from snowflake_testing import log_snowflake
from util.utils import get_secrets, get_data



data = get_data()
# secrets_dict = get_secrets()
# secret_name = data['aws']['name']
# region_name = data['aws']['region']

# aws_access_key_id = secrets_dict['aws_access_key_id']
# aws_secret_access_key = secrets_dict['aws_secret_access_key']


# session = boto3.Session(region_name=region_name)
def get_exec_time(start_time):
    end_time = time.time()
    total_exec_time = end_time - start_time
    
    return total_exec_time


# def log_data(logging_data,justification, logging_type, log_category, msg, testing_data ):
    
#     prompt=logging_data["prompt"]            
#     model_name=logging_data["model_name"]      
#     model_version=logging_data["model_version"]    
#     prompt_id=logging_data["prompt_id"]       
#     user_id= logging_data["user_id" ]         
#     conversation_id= logging_data["conversation_id"]     
#     start_time= logging_data["start_time"]          
#     region_name = data["aws"]["region"] 
    
#     if testing_data and data["testing"] == "True": # and config is true
        
#         log_snowflake(logging_data, testing_data )
    
#       #  ,aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
#     sqs = session.client('sqs')
    
#     message = {
#         "prompt_id" :prompt_id,
#         "conversation_id": conversation_id,
#         "user_id": user_id ,
#         "log_category" : log_category, #metrics, status
#         "execution_time_in_ms":get_exec_time(start_time),  
#         "timestamp": time.time(),
#         "user_input": prompt,
#         "model_output": justification,
#         "model_name": model_name,
#         "model_version": model_version,
#         "tokens_used" : "N/A",
#         "log_type" : logging_type, # info err debug
#         "log_message": msg,
#         "module_name": "validateprompt"
#     }
    
#     msg_body = json.dumps(message)
#     queue_url = data["logging"]["queue url"] 
   
#     if data["logging"]["send_log"] == "True":
#         responses = sqs.send_message(QueueUrl= queue_url, MessageBody=msg_body)
#         print ("Message send response : {} ".format(responses) )
    
    
    
#     return 
