import os
import sys
from datetime import datetime
from traceback import format_exception
from .exception_codes import Status, Reason
from .config import SETTINGS

class FincopilotException(Exception):
    """Base exception for all app related errors."""
    message = "An error occurred in Fincopilot app"
    status_code = 400
    component = "unknown"
    metadata = {}


    def __init__(self, message:str, reason: Reason, e: Exception = None,  status_code: int = None, **kwargs):
        '''
        Each exception an:
            e (Exception)
            message (str)
            reason (Reason)

        From the reason, we get status and status_code. 
        The passthrough status_codes from external APIs can be added through the status_code parameter

        @param e: The original exception
        @param message: The message for the exception
        @param reason: The reason for the exception
        @param status_code: The status code for the exception 
        @param metadata: Metadata for the exception
        

        '''

        
        self.message = message
        self.reason = reason
        self.status_message = self.__get_status_message()
        

        # TODO: add a way to derive status codes 
        # self.status_code = self.status_code or  self.status_message.value['status_code']  or self.reason.value['status_code'] or status_code
        self.__update_status_code(status_code)
        


        self.append_metadata(kwargs)


        

        super().__init__(self.message)

    def __update_status_code(self, hardcoded_status_code: int = None) -> None:
        '''
        Update the status code

        @param hardcoded_status_code: Hardcoded status code
        '''
        if hardcoded_status_code:
            self.status_code = hardcoded_status_code
        elif self.reason and 'status_code' in self.reason.value:
            self.status_code = self.reason.value['status_code']
        elif self.status_message and 'status_code' in self.status_message.value:
            self.status_code = self.status_message.value['status_code']

    def append_metadata(self, kwargs):
        '''
        Append metadata to the exception

        @param kwargs: Metadata
        '''
        
        
        # self.metadata = self.metadata | kwargs # for python 3.9
        # For python 3.8
        if kwargs:
            self.metadata.update(kwargs)
            
        if 'subcomponent' in kwargs:
            self.subcomponent = kwargs['subcomponent']

            self.metadata = {
                "subcomponent": f"{self.component}.{self.subcomponent}",
                "component": self.component,
            }

        

    def __get_status_message(self) -> Status:
        return self.reason.value['status']
    
    
    def __get_exception_traceback(self, exception: Exception) -> str:
        return ''.join(format_exception(type(exception), exception, exception.__traceback__))
    
    def __get_exception_message(self) -> str:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        if exc_value is not None:
            return str(exc_value)

        return self.message

    def get_response_data(self):
        """
        Method to get structured response data for this exception.
        
        """


        return {
            "code": self.status_code,
            "message": f"{self.status_message.value['status']} - {self.__get_exception_message()}",
            "status": self.status_message.value['status'], # enum
            "error_info": {
                "reason": self.reason.value['reason'], #enum table
                "subject_area": "AR",
                "metadata": self.metadata
            },
            "debug_info": {
                "exception_type": type(self).__name__,
                "trace":  self.__get_exception_traceback(self),
                "timestamp": str(datetime.now()),
                "environment": os.getenv("ENVIRONMENT", "production"),
                "request_id": "",
                "user_id": "",
            }
        }
    
#change init 
#inherit entity resolver exception from Fincopilot exception 
#Enum comes from a config table


class UserFeedbackException(FincopilotException):
    """Exception class specifically for User Feedback API."""
    message = f"An error occurred in the {SETTINGS['app']['name']} API"
    component = f"{SETTINGS['app']['name']}"
    