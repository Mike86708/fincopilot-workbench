import json
from lib.search_helper import QueryManager
from typing import List
import traceback

def lambda_handler(event, context):
    errorjson= {
        "statusCode": 400,
        "body": "Invalid input: {0} is mandatory"
    }
    # Get the user input
    query = event.get("query")
    sort = event.get("sort")
    size = event.get("size")
    queryOptions = event.get("queryOptions")
    type = event.get("type")

    if query=='' or query is None :
        errorjson["body"]='"Invalid input: query is mandatory"'
        return errorjson
    
    if sort==''  or sort is None:
        errorjson["body"]='"Invalid input: sort is mandatory"'
        return errorjson
       
    if size=='' or size is None:
        errorjson["body"]='"Invalid input: size is mandatory"'
        return errorjson

    if queryOptions=='' or queryOptions is None:
        errorjson["body"]='"Invalid input: queryOptions is mandatory"'
        return errorjson

    if type=='' or type is None:
        errorjson["body"]='"Invalid input: type is mandatory"'
        return errorjson
    
    #TODO : Add the validation 
    """
    Type has to be one of the following
    ix_brand
    ix_business
    ix_customer
    ix_subsidiary
    """
    try:
       qm = QueryManager ()
       return qm.query_index(queryOption=queryOptions,
                               query=query,
                               sort=sort,
                               size=size,
                               querytype=type)
       
    except Exception as e:
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
