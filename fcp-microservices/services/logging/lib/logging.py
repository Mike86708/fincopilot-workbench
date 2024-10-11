import json
import boto3
import time
from .sql_helper import SqlHelper
import botocore.exceptions 
import traceback
"""
This class is used for logging the SQS messages sent
be various applications. The logginc component creates
the required group and streams if it does not exist
and log the messages.
"""
class LogManager():

   def __init__(self,log_group,log_stream):
      self.log_group_name = log_group
      self.log_stream_name = log_stream

   """
    Method to create the log group
    #TODO : handle the execption if group already exist
   """
   def create_log_group(self):
      try:      
         cloudwatchlogs = boto3.client('logs')         
         cloudwatchlogs.create_log_group(logGroupName=self.log_group_name)
         print(f"created the cloudwatch log group:{self.log_group_name}")
      except Exception as e:
         #if(type(e)!=ResourceAlreadyExistsException):
         print(f"Failed creating log_group")
         print(e)
         print(type(e))

   """
    Method to create the log stream
    #TODO : handle the execption if stream already exist
   """
   def create_log_stream(self):
      try:
         cloudwatchlogs = boto3.client('logs')
         response = cloudwatchlogs.create_log_stream(logGroupName=self.log_group_name,
                                                          logStreamName=self.log_stream_name)
      except Exception as e:
         #if(type(e)!=ResourceAlreadyExistsException):
         print(f"Failed creating log stream :str({e})")      

   """
    Method to send the SQS message
   """
   def send_logmessages(self,log_message):
      try:
         if(self.__add_user_prompt_history__(log_message)):
            return   
         cloudwatchlogs = boto3.client('logs')
         cloudwatchlogs.put_log_events(
            logGroupName=self.log_group_name,
            logStreamName=self.log_stream_name,
            logEvents = [
               {
                  "timestamp":int(round(time.time()*1000)),
                  "message" : json.dumps(log_message)
               }
            ]
         )
         print(f"logged the message to log group:{self.log_group_name}")
      except Exception as e:
         print(f"Failed creating event:str({e})")

   """
      method to add the user prompt to history table, this will
      be triggered only if the source_name=query_controller
      and log_type is PROMPT_HISTORY
   """
   def __add_user_prompt_history__(self,log_message):
      try:
         #print(log_message)
         json_data = json.loads(log_message)
         log_type = json_data["log_type"]
         source_name = json_data["source_name"]

         if(log_type!="PROMPT_HISTORY" and source_name!="Query Controller" ):         
            return False
         
         if 'session_id' not in json_data["payload"]["input"]["session"]:
            print("Session_id does not exist,logging to cloudwatch...")
            return False
         session_id = json_data["payload"]["input"]["session"]["session_id"]
         prompt_id=json_data["payload"]["input"]["session"]['prompt_id']
         conversation_id=json_data["payload"]["input"]["session"]['conversation_id']
         helper = SqlHelper()
         helper.add_user_prompt_history(session_id=session_id,
                                        prompt_id=prompt_id,
                                        conversation_id=conversation_id,
                                        prompt_message = log_message)
         return True
      except Exception as e:
         print(traceback.format_exc())         
         print(f"Failed in add prompt function:str({e})")
         
         return False
