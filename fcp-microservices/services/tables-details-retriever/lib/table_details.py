import json
import snowflake.connector
import os

"""
This class is used for retrieving subject aire tables and its  associated table details. this
class is used within the API wihich is later used by the SQLClassifierr for
getting the tables and its details for a given subdomain
Input
    subject_area

Output
   {
     "table_metadata" :[

     {
       "table_name":"DIM_CUSTOMERS,DIM_SUBSIDIES"
       "details" :"" 
     }
     ]
"""
class TableDetailsService():

   def __init__(self,subject_area):
      self.subject_area = subject_area
 
   """
    Method to return the meta data for the given tables
   """
   def get_subjectarea_tables(self):
      try:      
         database_name= os.getenv('DATABASE')
         schema_name=os.getenv('SCHEMA')
         #database_name= "FINCOPILOT_CDM"
         #schema_name="DATA_CATALOG"
         
         sql_string =f"Select view_name, view_description from {database_name}.{schema_name}.VIEW_INFO "
         sql_string+= "where subject_area in ('"+self.subject_area+"','COMMON')"
         result = self.execute_query(sql_query=sql_string)
         return True, result
      except Exception as e:
         #if(type(e)!=ResourceAlreadyExistsException):
         print(f"Failed while retrieving table details for given subject area error : {e}")
         return False, f"Failed while retrieving table details for given subject area error : {e}"
         

   """
    Method to execute the query
    #TODO : replace the query with lambda execute
   """
   def execute_query(self, sql_query):
        conn = snowflake.connector.connect(
            user="0oafloefyca2uPtLI697",
            account="rla01593",
            password="Criticalriver@123",
            warehouse="Compute_WH"
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        result = cur.fetchall()
        # jsonData="[\n"
        # for data in result:
        #     jsonData+='{"table_name":"'+data[0]+'","table_detail":"'+data[1]+"'},"
        # jsonData = jsonData[:-1]+"]"
        # return jsonData
        
        jsonData = []
        for data in result:
            jsonData.append({
                "table_name": data[0],
                "table_detail": data[1]
            })
        return jsonData


#tm = TableMetaDataManager(subject_area="AR",tables="DIM_MARKET,AR_INVOICE,AR_CUSTOMER", sample_rows="0")
#tm.get_metadata() 