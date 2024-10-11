import json
from concurrent.futures import ThreadPoolExecutor
from exceptions import * 
from pydantic_user_prompt import get_entities
from external_api_connector import get_keyword_search_api
from search_formatter import format_search_results
from validate_entities import validate_entities
from utils import *
from temp_timer_context import *


def search_request_and_format_response(entity_type, query): 
    '''
    Performs the search request and formats the response
    
    Args:
    - entity_type (str): The type of entity to search for.
    - query (str): The search query.

    Returns:
    - list: A list of search results.
    '''
    if not query:
        return []

    # Perform the search API request
    with timer("Search API call"):
        results = get_keyword_search_api(entity_type, query)

    # Check for failure scenarios and raise exceptions accordingly
    if results['statusCode'] != 200: 
        raise EntityResolverException(f"{results['body']}", Reason.API_ERROR, status_code=results['statusCode'])
    
    # Assuming status codes other than 400 or 500 are considered successful
    if 'Hits' in results['body']:
        with timer('Search formatting'):
            return format_search_results(entity_type=entity_type, search_results=results["body"]["Hits"], keyword=query)
    else:
        # Return an empty list if no hits are found
        return []


def extract_and_search_main(user_prompt):
    '''
    Extracts entities from user prompt and performs search request
    
    Args:
    - user_prompt (str): The user prompt to extract entities from.

    Returns:
    - dict: A dictionary containing the extracted entities and search results.
    '''

    # Extract entities from user prompt
    # Access configuration values
    try:
        with timer("Secret retrieval"):
            secrets = get_secret()
            api_key = secrets['OPENAI_API_KEY']
    except KeyError as e:
        raise EntityResolverException(f"Missing key in config.json: {e}", Reason.MISSING_API_KEY) from e
    
    with timer("LLM call"):
        structured_entities = get_entities(api_key, user_prompt)

    #Gather entities extracted from user prompt
    with timer("Entity extraction"):        
        entities_extracted = {
            "customer_name": structured_entities.customer_name,
            "customer_id": structured_entities.customer_id, 
            "customer_email" : structured_entities.customer_email,
            "subsidiary": structured_entities.subsidiary,
            "brand":structured_entities.brand, 
            "business_unit": structured_entities.business_unit,
        }
    
    try:
        with timer("Entity validation"):
            entities_extracted = validate_entities(entities_extracted=entities_extracted)
    except Exception as e: 
        raise EntityResolverException(f"Error validating entities: {str(e)}", Reason.TYPE_VALIDATION_ERROR, subcomponent="entity_validation") from e

    #print (entities_extracted)
    # Check if any entities are extracted
    if not any(entities_extracted.values()):
        return entities_extracted, []


    # Using ThreadPoolExecutor for concurrent execution
    params = list(entities_extracted.items())
    #print ("PARAMS",params)

    # Using ThreadPoolExecutor
    try:
        with timer("Search requests(API call + formatting)"):
            with ThreadPoolExecutor(max_workers= len(entities_extracted)) as executor:
                futures = [executor.submit(search_request_and_format_response, *p) for p in params]
                # Gather the results from the futures
                results = [f.result() for f in futures]
    except Exception as e:
        raise EntityResolverException(f"Error executing search_request_and_format_response: {str(e)}", Reason.THREAD_POOL_EXECUTION_ERROR, subcomponent="search_request_and_format_response") from e

    return entities_extracted, results

if __name__ == "__main__":
    # Test retrieval_main function
    message = "show me all invoices for customer 73422 Conopco, Inc. d/b/a Unilever Food Solutions"
    entities_extracted, results = extract_and_search_main(message)
    #print (results)