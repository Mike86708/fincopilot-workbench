import psycopg2
import json
import os
import boto3
import traceback

"""
Lambda entry point for messages and configuration function,
We are using resource from http_method  to identify the calling
REST endpoint. 

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
    http_method = event['httpMethod']
    resource = event['resource']
    message=""   
    try:
        #method call for messages hence get the relevent message
        if resource == '/configurations/message/{code}' and http_method == 'GET':
            message_code = event['pathParameters']['code']
            message_object = get_message(message_code)
            
            #handle message if message does not exist
            if(message_object is None): 
                message='{"status":"failure","message": "message does not exist"}'
            elif(len(message_object)>0):
                message='{"status":"success","message":"'+message_object[0][0]+'"}'
            else: 
                message='{"status":"failure","message": "message does not exist"}'

        #handle configuration based on the endpoint        
        elif resource == '/configurations/{code}' and http_method == 'GET':
            config_code = event['pathParameters']['code']
            config_object = get_configuration(config_code)
            if(len(config_object)>0):
                if(config_object[0][0] is None): 
                   message='{"status":"failure","message": "configuration does not exist"}'
                else:
                   message='{"status":"success","message":"'+config_object[0][0].__str__()+'"}'
            else: 
                message='{"status":"failure","message": "configuration does not exist"}'
        else : 
           message='{"status":"failure","message": "unknown configuration"}'
            
        #return the result
        print(json.dumps(message))
        return {
            "isBase64Encoded": "false",
            "statusCode": 200,
            "headers": { 
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*" 
            },
            "body": json.dumps(message)
        }        
    except Exception as e:
          #traceback.print_exc() #only for testing 
          message='{"status":"failure","message":"failed to retrieve message:'+ str(e)+'"}'
          return {
            "isBase64Encoded":"false",  
            "statusCode": 200,
            "headers": { 
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*" 
            },
            "body": json.dumps(message)
            }  
            
"""
Method returns the credentials for postgres
"""
def getCredentials():
    
    credential = {}
    #GET THE SECRETS
    secret_name = os.getenv('secret_arn')
    region_name = os.getenv('region')
    
    client = boto3.client(
      service_name='secretsmanager',
      region_name=region_name
    )
    
    get_secret_value_response = client.get_secret_value(
      SecretId=secret_name
    )
    
    secret = json.loads(get_secret_value_response['SecretString'])
    #capture all the connection string parameters
    credential['username'] = secret['username']
    credential['password'] = secret['password']
    credential['host'] = os.getenv('postgres_host')
    credential['db'] = os.getenv('postgres_database')
    credential['port'] = os.getenv('postgres_port')
    return credential

"""
This method gets the relevent message based on the 
message code provided
"""
def get_message(message_code):
    credential = getCredentials()
    schema = os.getenv('postgres_schema')
    #connect to postgres
    connection = psycopg2.connect(user=credential['username'], 
                                  password=credential['password'], 
                                  host=credential['host'], 
                                  database=credential['db'],
                                  port = credential['port'])    
    cursor = connection.cursor()
    #call the function
    lang = "en-US"
    cursor.callproc(schema+'.get_app_message',(message_code,lang))  # No parameters in this example
    
    # Fetch the results
    results = cursor.fetchall()
    return results
"""
This method gets the relevent configuration based on the 
config code provided
"""
def get_configuration(config_code):
    credential = getCredentials()
    schema = os.getenv('postgres_schema')
    connection = psycopg2.connect(user=credential['username'], 
                                  password=credential['password'], 
                                  host=credential['host'], 
                                  database=credential['db'],
                                  port = credential['port'])    
    cursor = connection.cursor()
    
    # Call the stored procedure
    lang = "en-US"
    cursor.callproc(schema+'.get_app_configuration',(config_code,lang))  # No parameters in this example
    
    # Fetch the results
    results = cursor.fetchall()
    return results
                                  