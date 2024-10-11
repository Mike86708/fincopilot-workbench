from src.utils.main import logger, SETTINGS
from typing import List
import requests
from src.utils.exceptions import SQLGenerationException, Status, Reason

DEPENDENT_APIS = SETTINGS['dependent_apis']

def get_resolver_api(user_prompt: str) -> dict:
    '''
    Get context API call. Primarily only used for testing.

    @param user_prompt: User prompt
    @return: Context API response
    '''
    url = DEPENDENT_APIS['prompt_extractor']['api_url']
    body = {
        "data": {
            "input": {
                "user_prompt": user_prompt,
                "domain": "Accounting",
                "subject_area": "AR",
                "language": "engUS"
            },
            "session": {
                "user_id": "123",
                "prompt_id": "123",
                "conversation_id": "123"
            }
        }
    }

    response = requests.post(url, json=body)
    if response.status_code != 200:
        logger.error(f'Error resolving prompt: {response.text}')

    return response.json()




def get_table_info_from_table_name(table_names: List[str], sample_row_count: int = 3) -> str:
    '''
    Get table info from table names

    @param table_names: List of table names
    @param sample_row_count: Sample row count
    @return: Table info
    '''
    url = DEPENDENT_APIS['table_selector']['api_url']
        
    body = {
        "subject_area":"AR",
        "table_list": ','.join(list(map(str.upper,table_names))),
        "sample_num_of_rows": sample_row_count
    }

    response = requests.post(url, json=body)

    logger.debug("status code: " + str(response.status_code))
    if response.status_code == 200:
        try:
            result = response.json()
            body = result['body']
            if body['result'] == True:
                data = body['data']
                
                # TODO:Process the data here if needed
                schemas = '\n\n'.join(x['metadata'] for x in data)

                return schemas
        except:
            raise SQLGenerationException(f"Failed to parse table info", Reason.INVALID_API_OUTPUT)
    else:
        raise SQLGenerationException("Failed to get table info from table names", Reason.API_ERROR)    










def schema_validator(api_call: dict) -> bool:
    '''
    Check if all necessary keys are present in the input
    
    @param api_call: Input dictionary
    @param necessary_keys: List of necessary keys

    @return: True if all necessary keys are present
    @raises: KeyError if any key is missing
    '''


    # TODO: Maybe return a list of all the keys and values
    # and raise ValueError if any key is missing


    return True




def input_parser(event: dict) -> dict:
    '''
    Parse the input from the API call

    @param event: The API call input

    @return: A dictionary with the parsed and restructured input
    @raises: ValueError if any key is missing
    '''

    valid_schema = schema_validator(event)

    if not valid_schema:
        return

    if 'user_prompt' not in event:
        logger.error("No user prompt in API call")
        raise SQLGenerationException("Api call missing 'user_prompt'", Reason.INVALID_API_CALL)
    
    question = event['user_prompt']

    if 'features_extracted' not in event:
        logger.warning("No features in API call. Hence, no additional context when generating the SQL")
        return {
            'question': question, 
            'context': ""
        }
    elif len(event['features_extracted']) == 0:
        logger.warning("No features in API call. Hence, no additional context when generating the SQL")
        return {
            'question': question,
            'context': ""
        }

    
    features_extracted = event['features_extracted']
    
    
    
    # Construct feature list for model ingestion.
    try:
        feature_list = []
        for feature in features_extracted:
            if type(feature['matches']) == list:
                if len(feature['matches']) == 0:
                    continue

            new_feature = {}
            new_feature['entity_type'] = feature['type']
            new_feature['entity_name'] = feature['matched_on'] # Or matched on
            
            feature_keys = []
            for match in feature['matches']:
                feature_key = {}
                feature_key['key'] = match['query_by_value']
                feature_keys.append(feature_key)


            new_feature['primary_keys'] = [d['key'] for d in feature_keys]

            feature_list.append(new_feature)
    except Exception as e:
        raise SQLGenerationException("Api call missing valid 'features_extracted'", Reason.MISSING_KEY) from e

    types = {}


    
    # Return empty context if there are no features
    if len(feature_list) == 0:
        return {
            "question": question,
            "context": ""
        }

    for entity in feature_list:
        types[entity['entity_type']] = len(entity['primary_keys'])
    
    # Constructing number of keys
    types_string = ''
    for k, v in types.items():
        types_string += f"{v} {k} primary_keys, "

    # Putting the context together
    context = f'''
    
    Added_Context

    The following filtration_criteria has been extracted from the database. 
    Use only the given filtration_criteria for any of the SQL commands that involve filtering. 
    
    Do not use the anything but the "primary_keys" in the following filtration_criteria for the SQL filter condition.

    Do not skip any keys. 

    <filteration_criteria>
    {feature_list}
    </filteration_criteria>
    '''
    #{json.dumps(feature_list, indent=2, ensure_ascii=True)}
    
    
    return {
        "question": question,
        "context": context
    }
    


def input_validator(input) -> bool:
    '''
    TODO: To be implemented

    @param input: Input dictionary

    @return: True if input is valid

    @raises: ValueError if any key is missing
    '''
    # Validator needs to call __key_checker and make sure all the keys are in place
    # After that the input parser can be called knowing that the input is valid

    return True


def output_validator(output) -> int:
    '''
    TODO: To be implemented 

    @param output: Output dictionary
    @return: 0 if output is valid
    @raises: ValueError if any key is missing
    '''
    pass


def response_builder(output):
    '''
    TODO: To be implemented

    @param output: Output dictionary
    @return: Response dictionary
    '''

    return {
        "query_string": output,
        "query_metadata": {
            "tables_used": [],
            "databases_used": []
        }
    }
    