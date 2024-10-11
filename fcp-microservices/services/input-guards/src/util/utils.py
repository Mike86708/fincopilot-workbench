import json
import time
import boto3
import logging
import requests
import os
from botocore.exceptions import ClientError
from exceptions.exception import InputGuardException
from exceptions.exception_codes import Reason


environment = os.getenv('ENVIRONMENT',None)
print('environment-',environment)
if environment == 'development':
    from dotenv import load_dotenv
    load_dotenv()
   
    
    
def get_data():
     with open('config/input_guards_config.json') as config_file:
        data = json.load(config_file)
        data["system config"]["system config url"]=os.getenv("system_config_url")
        data["logging"]["queue url"]=os.getenv("log_sqs_url")
        data["models"]["model"]=os.getenv("model")
        data["models"]["version"]=os.getenv("version")
        data["aws"]["region"]=os.getenv("region")
        data["aws"]["name"]=os.getenv("secret_name")
        
        # Assuming FILTERS environment variable is set
        filters = os.getenv("FILTERS", "").split(",")
        # List of all possible guards
        all_guards = ["nsfw", "off_topic", "sql_injection", "pii"]
        # Create the dictionary with True/False values based on presence in FILTERS
        filters_dict = {
            "filters": {guard: "True" if guard in filters else "False" for guard in all_guards}
        }
            
        data["filters"]= filters_dict["filters"]
        
        # Adding 
        data['openai_organization']=os.getenv("openai_organization",'')
               
        return data
     
session = boto3.session.Session()

def get_secrets() -> dict:
        '''
        Get the secrets from AWS Secrets Manager
        '''
        data = get_data()
        secret_name = data['aws']['name']
        region_name = data['aws']['region']

        # Create a Secrets Manager client
        
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)

            if 'SecretString' in get_secret_value_response:
                return json.loads(get_secret_value_response['SecretString'])

            raise KeyError(f"Missing SecretString in AWS secrets: {get_secret_value_response}")
        except ClientError as e:
            print(f"Error getting secrets from aws: {e}")
            raise e 
        

def get_filters():
        '''
        Gets the filters from the system config
        @return: apply_pii_guard, apply_nsfw_guard, apply_topic_guard, apply_sql_injection, company_name, language, pii_configs
        '''
     
        data = get_data()
        
        apply_pii_guard = False
        apply_nsfw_guard = False
        apply_topic_guard = False
        apply_sql_injection = False
        
        if data['filters']["pii"] == "True":
            apply_pii_guard = True
        if data['filters']["nsfw"] == "True":
            apply_nsfw_guard = True
        if data['filters']["off_topic"] == "True":
            apply_topic_guard = True
        if data['filters']["sql_injection"] == "True":
            apply_sql_injection = True
        # company_name = data["company_name"]    
        language = data["language"]
        pii_configs = data["pii_entities"]
        # return apply_pii_guard, apply_nsfw_guard, apply_topic_guard,apply_sql_injection, company_name, language, pii_configs
        return apply_pii_guard, apply_nsfw_guard, apply_topic_guard,apply_sql_injection, language, pii_configs



def get_return_msg(msg_code):
        '''
        Gets the return message from the system config
        @param: msg_code
        @return: return message
        msg_codes:

        VP_1000 = "PII"
        VP_2000 = "NSFW"
        VP_3000 = "OFF TOPIC"
        VP_4000 = "SQL INJECTION"
        APP-1001 = 'TIME OUT'
        APP-1002 = 'ERROR GENERAL'

        '''
        data = get_data()
        system_config_url = data["system config"]["system config url"] + data["system config"][msg_code]
        msg = json.loads(requests.get(system_config_url).json())
    
        return msg["message"]
        
        

def get_guard_messages(guard_run_result: dict, guard_list: list):
    """
    Extracts the failure justification and message from the guard run results.

    Args:
        guard_run_result (dict): The result returned by the GuardManager after running guards.
        guard_list (list): List of guards that were run.

    Returns:
        tuple: (justification, message) if a guard failed, otherwise (None, None).
    """
    if guard_run_result.get('result', True):
        return None, None  # If the overall result is True, return None

    for guard in guard_list:
        if guard in guard_run_result['details']:
            detail = guard_run_result['details'][guard]
            if not detail.get('result'):
                return detail.get('justification'), detail.get('message')
    
    return None, None  # Return None if no guard failure is found        


# Function to validate input data
def input_validator(parsed_input_data):
    '''
    Validates the input data

    @param parsed_input_data: Parsed input data
    @return: None
    '''
    if 'user_prompt' not in parsed_input_data or parsed_input_data.get('user_prompt') is None:
        raise InputGuardException('Missing required field: user_prompt', reason=Reason.INVALID_INPUT)
    

def get_prompt_guard_blocked_list(guard_run_result):
    """
    This function takes the result of a guard run and returns a list of guards
    that were blocked based on their results. It also accounts for missing keys
    in the input dictionary.
    
    Parameters:
    guard_run_result (dict): A dictionary containing the details of the guard 
                             run, including the results for each guard. Keys like 
                             'pii', 'off_topic', 'nsfw', and 'sql_injection' may
                             or may not be present.

    Returns:
    list: A list of strings representing the names of the guards that failed.
          The list includes "PII", "OFF_TOPIC", "NSFW", or "SQL_INJECTION" if
          the respective guard's result is False and the key exists.
          Returns an empty list if all guards pass or if none of the keys are present.
    
    Logic:
    1. If 'pii' exists and 'pii.result' is False, "PII" is included in the list.
    2. If 'off_topic' exists and 'off_topic.result' is False, "OFF_TOPIC" is included in the list.
    3. If 'nsfw' exists and 'nsfw.result' is False, "NSFW" is included in the list.
    4. If 'sql_injection' exists and 'sql_injection.result' is False, "SQL_INJECTION" is included in the list.
    5. If all guards pass or no relevant keys are present, an empty list is returned.
    """
    
    blocked_list = []
    
    if "pii" in guard_run_result["details"] and not guard_run_result["details"]["pii"]["result"]:
        blocked_list.append("PII")
    
    if "off_topic" in guard_run_result["details"] and not guard_run_result["details"]["off_topic"]["result"]:
        blocked_list.append("OFF_TOPIC")
    
    if "nsfw" in guard_run_result["details"] and not guard_run_result["details"]["nsfw"]["result"]:
        blocked_list.append("NSFW")
    
    if "sql_injection" in guard_run_result["details"] and not guard_run_result["details"]["sql_injection"]["result"]:
        blocked_list.append("SQL_INJECTION")
    
    return blocked_list
