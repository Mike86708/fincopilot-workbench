import json 
import os
from lib.logging import LogManager

def lambda_handler(event, context):
   try:
      log_group_name= os.getenv('LOG_GROUP')
      log_stream_name=os.getenv('LOG_STREAM')

      logmanager = LogManager(log_group = log_group_name,
                              log_stream = log_stream_name)
      print (event['Records'])
      #Get the log message message 
      for record in event['Records']:
            body = json.loads(record["body"])

      #create the log group if does not exist
      logmanager.create_log_group()
      #Create the Log stream if does not exist
      logmanager.create_log_stream()
      #Log the message
      logmanager.send_logmessages(log_message=body)
   except Exception as e:
      # Send some context about this error to Lambda Logs
      print(e)
      # throw exception, do not handle. Lambda will make message visible again.
      raise e

log_data = open('.\data.json')
data=json.load(log_data)
logmanager = LogManager(log_group = "TEST",
                              log_stream = "TEST")
logmanager.send_logmessages(log_message=json.dumps(data))
 