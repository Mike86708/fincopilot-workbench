import requests
import json
import os

def get_keyword_search_api(entity_type, query) -> dict:
    '''
    Calls the search request API and returns the response
    Args:
    - entity_type (str): The type of entity to search for.
    - query (str): The search query.

    Returns:
    - dict: A dictionary containing the search results.
    '''

    url = os.environ.get('KEYWORD_SEARCH_API_URL')

    queryOptions = os.environ.get('KEYWORD_SEARCH_LOOKUP_VALUE')
    if entity_type == "subsidiary":
        type = "ix_subsidiary" 
    if entity_type == "brand":
        type = "ix_brand"
    if entity_type == "customer_name" or entity_type =="customer_id" or entity_type == "customer_email":
        type = "ix_customer"
        if entity_type == "customer_id": 
            queryOptions = os.environ.get('KEYWORD_SEARCH_LOOKUP_ID')
    if entity_type == "business_unit":
        type = "ix_business_unit"

    body = {
        "type": type,
        "query": query,
        "size": int(os.environ.get('KEYWORD_SEARCH_RESULT_SIZE')),
        "sort":os.environ.get('KEYWORD_SEARCH_SORT'),
        "queryOptions": queryOptions
    }
    #print ("KEYWORD SEARCH BODY: ", body)

    response = requests.post(url, json=body)
    return response.json()

