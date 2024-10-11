import json
from main import *
from aws_logging_utils import log_cloudwatch, LogLevel, LogType, sqs_logging_enabled

# Function to validate input data
def input_validator(parsed_input_data):
    '''
    Validates the input data

    @param parsed_input_data: Parsed input data
    @return: None
    '''
    if 'user_prompt' not in parsed_input_data:
        raise EntityResolverException('Missing required field: user_prompt', Reason.INVALID_API_CALL)

# Function to build the final response
def response_builder(parsed_input_data):
    '''
    Builds the final response

    @param parsed_input_data: Parsed input data
    @return: Final response
    '''
    
    # Validate input
    try:
        input_validator(parsed_input_data)
        entities_extracted, search_results = extract_and_search_main(parsed_input_data['user_prompt'])

        if not any(entities_extracted.values()):
            final_response = {
                "user_prompt": parsed_input_data['user_prompt'],
                "domain": "accounting",
                "subject_area": "AR",
                "entities_extracted": entities_extracted,
                "features_extracted": []
            }
            return final_response

        filtered_search_results = [lst for lst in search_results if lst]

        # Construct final response
        final_response = {
            "user_prompt": parsed_input_data['user_prompt'],
            "domain": "accounting",  
            "subject_area": "AR",  
            "entities_extracted": entities_extracted,
            "features_extracted": filtered_search_results
        }

    except Exception as e:
        raise EntityResolverException("Error building response", Reason. API_OUTPUT_BUILDING_ERROR, subcomponent="api_util") from e
    
    if sqs_logging_enabled:
        log_cloudwatch(log_level=LogLevel.INFO, log_type=LogType.FUNCTION_OUTPUT, message="Lambda function output", args=final_response)
        
    return final_response


# For testing purposes
if __name__ == "__main__":
    example_input = "List all invoices for brand Subway last month"
    _input = {'user_prompt': example_input}
    input_validator(_input)
    response = response_builder(_input)
    print (response)
    