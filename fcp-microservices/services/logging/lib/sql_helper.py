import psycopg2
import json
import os
import boto3
class SqlHelper():
    """
    Method returns the credentials for postgres
    """
    def getCredentials(self):
        
        # Create a Secrets Manager client
        #not required for lambda
        #remove after local testing
        #ACCESS_KEY=""
        #SECRET_KEY=""
        session = boto3.Session(
            #aws_access_key_id=ACCESS_KEY,
            #aws_secret_access_key=SECRET_KEY
        )
        credential = {}
        #GET THE SECRETS
        secret_name = os.getenv('secret_arn')
        region_name = os.getenv('region')

       
        client = session.client(
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
        credential['schema'] = os.getenv('postgres_schema')
        
        return credential

    """
    This method gets the relevent message based on the 
    message code provided
    """
    def add_user_prompt_history(self,
                                session_id,
                                prompt_id,
                                conversation_id,
                                prompt_message):
        credential = self.getCredentials()
        schema_name = os.getenv('postgres_schema')
        #connect to postgres
        connection = psycopg2.connect(user=credential['username'], 
                                    password=credential['password'], 
                                    host=credential['host'], 
                                    database=credential['db'],
                                    port = credential['port'],
                                    options="-c search_path=dbo,"+schema_name)    
        try:
            cursor = connection.cursor()
            #call the function
            procedure_name=f'CALL {schema_name}.usp_add_user_session_history(%s,%s,%s,%s)'
            cursor.execute(procedure_name,( session_id,
                                            prompt_id,
                                            conversation_id,
                                            json.dumps(prompt_message))) 
        finally:
            connection.commit()
            cursor.close()
            connection.close()
        