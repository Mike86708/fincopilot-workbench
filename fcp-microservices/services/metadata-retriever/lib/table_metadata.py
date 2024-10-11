import json
import snowflake.connector
import boto3
import os
import traceback
from botocore.exceptions import ClientError

from cryptography.hazmat.backends import default_backend 
from cryptography.hazmat.primitives import serialization 

"""
This class is used for retrieving metadata associated with the table. this
class is used within the API wihich is later used by the SQLGenerator for
getting the metadata for the required tables
Input
    subject_area
    table_list
    sample_num_of_rows

Output
   {
     "dialect" :"en-US" ,
     "max_rows_to_be_returned":"10".
     "table_metadata" :[

     {
       "table_name":"DIM_CUSTOMERS,DIM_SUBSIDIES"
       "metadata" :"" 
     }
     ],
    "sample_rows" :"0"
"""
class TableMetaDataManager():

   def __init__(self,subject_area,tables, sample_rows):
      self.subject_area = subject_area
      self.tables = tables
      self.sample_rows = sample_rows

   """
    Method to return the meta data for the given tables
   """
   def get_metadata(self):
      try:      
         database_name= os.getenv('DATABASE')
         schema_name=os.getenv('SCHEMA')
         
         
         sql_string =f"Select view_name, meta_data from {database_name}.{schema_name}.VIEW_INFO "
         sql_string+= "where view_name in('"+ self.tables.replace(",","','")+"');"
         #print(sql_string)
         result = self.execute_query(sql_query=sql_string)
         #print(json.dumps(result))
         return True, result
      except Exception as e:
         #if(type(e)!=ResourceAlreadyExistsException):
         print(f"Failed while retrieving metadata error : {e}")
         return False, f"Failed while retrieving metadata error : {e}"
         

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
        return credential['fincopilot_user']



   """
    Method to execute the query
    #TODO : replace the query with lambda execute
   """
   def execute_query(self, sql_query):
        sf_account = os.getenv('account')
        sf_warehouse = os.getenv('warehouse')
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
        conn = snowflake.connector.connect(
            user= os.getenv("user_name"),
            private_key=private_key_bytes,
            warehouse=sf_warehouse,
            account=sf_account,
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        result = cur.fetchall()
        # jsonData="[\n"
        # for data in result:
        #     jsonData+='{"table_name":"'+data[0]+'","metadata":"'+data[1]+"'},"
        # jsonData = jsonData[:-1]+"]"
        # return jsonData

        metadata_list = []
        for data in result:
            metadata_list.append({"table_name": data[0], "metadata": data[1]})
        
        return metadata_list       

#tm = TableMetaDataManager(subject_area="AR",tables="DIM_MARKET,AR_INVOICE,AR_CUSTOMER", sample_rows="0")
#tm.get_metadata() 