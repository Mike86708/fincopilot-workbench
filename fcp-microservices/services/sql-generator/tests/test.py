from exceptions import *
import json

def parse(): # used by multiple subcomponents
    try:
        # Code for subsidiary search
        raise ValueError("Error parsing LLM output")  # Simulate an error for demonstration
    except ValueError as e:
        # Catch and wrap in a more specific exception
        raise SQLGenerationException("specific error msg", reason=Reason.PARSING_ERROR, e=e) from e

def sql_generator(): 
    try: 
        parse()
    except SQLGenerationException as e:
        # e.append_metadata({
        #     'error_info': {
        #         "subject_area": "cloudsearch"
        #     }
        # })
        raise SQLGenerationException(e.message, reason=e.reason, subcomponent="sql_generator") from e

def perform_entity_resolution():
    try: 
        sql_generator()
    except SQLGenerationException as e:
        # Catch and wrap in the base exception
        return json.dumps(e.get_response_data(), indent=2)
    
    # sql_generator()
    

print (perform_entity_resolution())



