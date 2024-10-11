import os
import json
import snowflake.connector
import boto3
import os
import traceback
from botocore.exceptions import ClientError

from dataclasses import  asdict
from lib.entities.response_data import Hit,SearchResult 
from typing import List

from cryptography.hazmat.backends import default_backend 
from cryptography.hazmat.primitives import serialization 


class QueryManager:
   def __init__(self):
       return 

   """
    Method to return the meta data for the given tables
   """
   def query_index(self,query, queryOption, sort, size, querytype ):
      try:
        sqlQuery = self.__build_indexsql__(queryOption=queryOption,
                                            sort=sort,
                                            size=size,
                                            type=querytype,
                                            query=query)   
        # print(sqlQuery)                                    
        
        sf_access_key = self.getCredentials()
        
        sf_access_key="-----BEGIN PRIVATE KEY-----\n"+sf_access_key+"\n-----END PRIVATE KEY-----"
        private_key= serialization.load_pem_private_key(
            sf_access_key.encode('utf-8'),
            password=None,
            backend=default_backend()
        )

        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        sf_account = os.getenv('account')
        sf_warehouse = os.getenv('warehouse')
        sf_database = os.getenv('database')
        sf_schema = os.getenv('schema')
        
        # Snowflake connection parameters
        conn = snowflake.connector.connect(
            user= os.getenv("user_name"), 
            private_key=private_key_bytes,
            account=sf_account,
            warehouse=sf_warehouse,
            database=sf_database,
            schema=sf_schema
        )

        # Execute a query
        cursor = conn.cursor()
        cursor.execute(sqlQuery)
        rows = cursor.fetchall()

        # Map the results to a list of Customer data class instances
        Hits: List[Hit] = [
            Hit(*row) for row in rows
        ]

        
        result_data= [
            SearchResult(Hit) for Hit in Hits
        ]
        count = len(Hits)
        # Convert the list of data class instances to JSON
        #result = json.dumps([asdict(hit) for hit in Hits])
        result =  json.dumps([asdict(data) for data in result_data])
        return {
            "statusCode": 200,
            "body": { 
               "Hits" : {
                  "found" : count,
                  "start" : 0,
                  "hit" :result
               }
            }
        }

      except Exception as e:
           traceback.print_exc()
           return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
      #finally:
        #cursor.close()
        #conn.close()

   """
   Method returns the credentials for postgres
   """
   def getCredentials(self):
        
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
        credential['fincopilot_user'] = secret['fincopilot_user'] 
        
        return  credential['fincopilot_user']



    # Your code goes here.       

   """
    Method to execute the query
    #TODO : replace the query with lambda execute
   """
   def __build_indexsql__(self,query, queryOption, sort, size, type):
        sqlQuery= ""
        filter_value=""
        orderby_filter=""
        
        if(type.lower()=="ix_customer"):
            sqlQuery = "Select LOOKUP_ID,LOOKUP_VALUE,LINKED_RECORDS "
        else :          
            sqlQuery = "Select 0 as LOOKUP_ID,LOOKUP_VALUE,LINKED_RECORDS "

        if(queryOption.lower()=="lookup_id"):
            filter_value= f"{queryOption}={query}"
            orderby_filter=queryOption
        else:
            query = query.replace("'","''")
            filter_value=f"ilike({queryOption} ,'%{query}%')"
            orderby_filter = f"JAROWINKLER_SIMILARITY('{query}',{queryOption})"
            
        sqlQuery+=f" From {type} Where {filter_value} order  by {orderby_filter} {sort} limit {size};"
        return sqlQuery

