
from src.llms.sql_generator.model_beta import SQLGeneratorOpenAI
# from src.llms.sql_generator.model_response import ChatBot
# from src.evaluation.hardcoded_prompt_model import SQLGeneratorHardcodedPromptModel
from src.utils.api_utils import *
from src.utils.aws_logging_utils import set_api_level_logs, log_cloudwatch, LogLevel, LogType

from time import time

cb = SQLGeneratorOpenAI()

def lambda_handler(event: dict, context) -> dict:
    '''
    Lambda function handler.
    Handles the API calls and returns the response.

    
    @param event (dict): The Lambda function event input.
    @param context (LambdaContext): The Lambda function context.

    @return (dict): The Lambda function response.
    '''

    try:
        METRICS = True
        start = time()

        session_data = event.get("Input", {}).get("session", {})
        user_data = event.get("Input", {}).get("user_info", {})

        # Set logging api level TODO: Get from AWS settings
        set_api_level_logs(args={
            'source_name': "query_controller",
            "prompt_id": "" if 'prompt_id' not in session_data else session_data['prompt_id'],
            "conversation_id": "" if 'conversation_id' not in session_data else session_data['conversation_id'],
            "session_id": "" if 'session_id' not in session_data else session_data['session_id'],
            # "user_id": "" if 'user_id' not in session_data else session_data['user_id'],
            
        })

        log_cloudwatch(log_level=LogLevel.INFO, log_type=LogType.SERVICE_INPUT, message="Lambda function input", args={
            "payload": event
        })
        

        # Validated event, 
        # model_context is the prepared input for the model
        parser_response = event
        
        cb.set_user(user_id=session_data.get('session_id', None))
        response = cb.ask(question=parser_response['question'])
        
        
        payload = response

        # response = {
        #     "status_code": 200
        #     "query_string": "SELECT * FROM table",
        #     "query_metadata": {
        #         "tables_used": [],
        #         "databases_used": []
        #     }
        # }
        
        
        # Passed without errors
        # response['status_code'] = 200

        stop = time()
        log_cloudwatch(log_type=LogType.SERVICE_OUTPUT, message="Lambda function output", args={
            "latency_ms": stop - start,
            "payload": payload
        })
        return payload
    except SQLGenerationException as e:
        logger.exception(e)

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

    # return {
    #     'status_code': 400,
    #     'error_message': f'Lambda function exception: {e}'
    # }



