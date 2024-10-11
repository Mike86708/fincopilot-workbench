import json
from api_util import *
from aws_logging_utils import set_api_level_logs, LogLevel, LogType, log_cloudwatch
from time import time


def lambda_handler(event, context):
    '''
    Lambda function handler.    
            
    Args:
        event (dict): The Lambda function event input.
        context (LambdaContext): The Lambda function context.   
                
                
    Returns:
        dict: The Lambda function response.
    '''
    try:
        
        
        # Log function input to SQS
        time_start = time()
        set_api_level_logs(args={
            "service_name": "entity_resolver",
            "source_name": "query_controller",
            "prompt_id": "",
            "conversation_id": "",
        })
        # push to cloudwatch
        log_cloudwatch(log_type=LogType.SERVICE_INPUT, message="Input received", args=event)


        # input ingestion
        data = event.get("data")  # Retrieve the "data" dictionary from event

        

        if data:
            input_data = data.get("input")  # Retrieve the "input" dictionary from data

            if input_data:
                user_prompt = input_data.get("user_prompt")  # Retrieve "user_prompt" from input_data
                domain = input_data.get("domain")  # Retrieve "domain" from input_data
                subject_area = input_data.get("subject_area")  # Retrieve "subject_area" from input_data
                language = input_data.get("language")  # Retrieve "language" from input_data

            session_data = data.get("session")  # Retrieve the "session" dictionary from data

            if session_data:
                user_id = session_data.get("user_id")  # Retrieve "user_id" from session_data
                prompt_id = session_data.get("prompt_id")  # Retrieve "prompt_id" from session_data
                conversation_id = session_data.get("conversation_id")  # Retrieve "conversation_id" from session_data
        
        
        _input = {'user_prompt': user_prompt, 'domain': domain, 'subject_area': subject_area, 'language': language, 'user_id': user_id, 'prompt_id': prompt_id, 'conversation_id': conversation_id}

        #validate input for presence of required fields
        input_validator(_input)
        
        
        # model repsonse
        api_response = response_builder(_input)

        time_end = time()

        api_latency = time_end - time_start
            
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Output generated", args={
            "latency_ms": api_latency * 1000, # convert to ms
            "payload": api_response
        })


        return api_response
    
    except EntityResolverException as e:
        
        constructed_response = e.get_response_data()

        log_cloudwatch(
            log_level=LogLevel.ERROR,
            log_type=LogType.SERVICE_OUTPUT,
            message="Lambda function error",
            args={
                "payload": constructed_response
            }
        )
        
        return constructed_response
    
   
    pass

  
    
        