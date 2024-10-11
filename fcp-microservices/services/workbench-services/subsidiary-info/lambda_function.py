import json
from subsidiary_exceptions import SubsidiaryException, Reason
from snowflake_common import *
from sqlalchemy import text

"""
Lambda entry point for getting subsidiaries

Note: Please use Lambda Proxy integration , else event will not 
be triggered.
Environment Vars
    postgres_database  
    postgres_host 
    postgres_port 
    region 
    secret_arn 
Layers Used
   Layer for psycopg2 on python 3.8    
"""
def lambda_handler(event, context):
    try:
        filterList = get_subsidiaries()
        status_code = 200   
        if(filterList is None): 
            status_code = Reason.NO_DATA_FOUND
            message='{"status":"failure","message": "Subsidiaries does not exist"}'
        else:   
            return filterList  
    except SubsidiaryException as e:
        print(f"Error in lambda_handler: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps(e.get_response_data(), indent=2),
        }
    except Exception as e:
        # Catch any other unforeseen exceptions
        print(f"Unexpected error in lambda_handler: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "An unexpected error occurred.",
                    "details": str(e),
                },
                indent=2,
            ),
        }
    return response 
            
"""
Method to build parent-child tree mapping of all subsidiaries
"""    
def build_mapping(keyItems, keyItemsVal, dictKeyMaps, dictKeyVals):
    keys = keyItems.split(':')
    finalKey = keys[0].strip()
    for count, key in enumerate(keys):
        trimmedKey = key.strip()
        nextkey = ''
        if count < len(keys)-1:
            nextkey = keys[count + 1].strip()
        if trimmedKey in dictKeyMaps:
            trimmedKeyVal = dictKeyMaps[trimmedKey]  
            if  nextkey not in trimmedKeyVal:
                trimmedKeyVal.append(nextkey)
        # if next key exists
        if count < len(keys)-1:
            finalKey = finalKey + ":" + nextkey
            if nextkey not in dictKeyMaps:
                if trimmedKey not in dictKeyMaps:
                    dictKeyMaps[trimmedKey] = [nextkey]
        if(count == len(keys) - 1):
            dictKeyVals[finalKey] = keyItemsVal

"""
Method to generate json from the parent tree mapping structures
"""    
def build_json(dictKeyMaps, dictKeyVals, startKey, value_lookup_key):
    return_json = {}
    if value_lookup_key in dictKeyVals:
        return_json = { "label" : startKey, "value": dictKeyVals[value_lookup_key]}

    if startKey in dictKeyMaps:
        sub_keys = dictKeyMaps[startKey]
        children_elements = []
        for sub_key in sub_keys:
            composite_lookup_key = value_lookup_key + ':' + sub_key
            sub_json = build_json(dictKeyMaps, dictKeyVals, sub_key, composite_lookup_key)
            children_elements.append(sub_json)
            return_json["children"] = children_elements
    return return_json
    
"""
This method gets the workbench tasks based on the 
filters provided. By default all tasks are returned
"""
def get_subsidiaries():
    engine = get_snowflake_engine()

    sqlQuery = ''
    try:
        with engine.connect() as connection:
            try:
                with open('subsidiary_info.sql', 'r') as file:
                    sqlQuery = file.read()
            except FileNotFoundError as e:
                raise SubsidiaryException(
                    "SQL query file not found",
                    reason=Reason.SQL_FILE_NOT_FOUND,
                    subcomponent="get_subsidiaries"
                ) from e
            except Exception as e:
                raise SubsidiaryException(
                    "Error reading SQL query file",
                    reason=Reason.FILE_READ_ERROR,
                    subcomponent="get_subsidiaries"
                ) from e
            
            if sqlQuery is None or not sqlQuery.strip():
                raise SubsidiaryException(
                    "SQL query is empty",
                    reason=Reason.INVALID_SQL_QUERY,
                    subcomponent="get_subsidiaries"
                )
            subsidiaries_data = connection.execute(text(sqlQuery), None).fetchall()

            dictKeyMaps = dict()
            dictKeyVals = dict()
            # Process each row returned by the cursor
            for row in subsidiaries_data:
                subsidiary_id = row[0]
                subsidiaries = row[1]
                build_mapping(subsidiaries, subsidiary_id, dictKeyMaps, dictKeyVals)

        result_json = []
        for key in dictKeyVals:
            if ':' not in key:
                json_str = build_json(dictKeyMaps, dictKeyVals, key, key)
                result_json.append(json_str)

        return result_json
    except Exception as e:
        raise SubsidiaryException(
            "An error occurred while getting Subsidiaries.",
            reason=Reason.DATABASE_EXECUTION_ERROR,
            subcomponent="get_subsidiaries"
        ) from e