import os
import sys
from datetime import datetime
from traceback import format_exception
from .exception_codes import Status, Reason
from config.config import SETTINGS


class FincopilotException(Exception):
    """Base exception for all app related errors."""

    # Default message, status code, component, and metadata for the exception
    message = "An error occurred in Fincopilot app"
    status_code = 400
    component = "unknown"
    metadata = {}

    def __init__(
        self,
        message: str,
        reason: Reason,
        e: Exception = None,
        status_code: int = None,
        **kwargs,
    ):
        """
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
        """
        # Set initial values for the exception
        self.message = message
        self.reason = reason
        self.status_message = self.__get_status_message()

        # Update the status code based on provided parameters and reason
        # self.status_code = self.status_code or  self.status_message.value['status_code']  or self.reason.value['status_code'] or status_code
        self.__update_status_code(status_code)

        # Append any additional metadata provided in kwargs
        self.append_metadata(kwargs)

        # Initialize the base Exception class with the message
        super().__init__(self.message)

    def __update_status_code(self, hardcoded_status_code: int = None) -> None:
        """
        Update the status code

        @param hardcoded_status_code: Hardcoded status code
        """
        # Determine the status code to use
        if hardcoded_status_code:
            self.status_code = hardcoded_status_code
        elif self.reason and "status_code" in self.reason.value:
            self.status_code = self.reason.value["status_code"]
        elif self.status_message and "status_code" in self.status_message.value:
            self.status_code = self.status_message.value["status_code"]

    def append_metadata(self, kwargs):
        """
        Append metadata to the exception

        @param kwargs: Metadata
        """

        # self.metadata = self.metadata | kwargs # for python 3.9
        # For python 3.8
        
        # Update metadata with provided kwargs
        if kwargs:
            self.metadata.update(kwargs)

        if "subcomponent" in kwargs:
            self.subcomponent = kwargs["subcomponent"]

        # Update metadata with subcomponent information
            self.metadata = {
                "subcomponent": f"{self.component}.{self.subcomponent}",
                "component": self.component,
            }

    def __get_status_message(self) -> Status:
        '''
        Retrieve the status message associated with the reason.

        @return: Status enum associated with the reason
        '''
        # Access the status message from the reason
        return self.reason.value["status"]

    def __get_exception_traceback(self, exception: Exception) -> str:
        '''
        Generate a formatted string of the exception traceback.

        @param exception: The original exception object
        @return: Formatted traceback string
        '''
        # Format the traceback for the given exception
        return "".join(
            format_exception(type(exception), exception, exception.__traceback__)
        )

    def __get_exception_message(self) -> str:
        '''
        Retrieve the exception message.

        @return: Exception message string
        '''
        exc_type, exc_value, exc_traceback = sys.exc_info()
        # Return the string representation of the exception value if present
        if exc_value is not None:
            return str(exc_value)

        return self.message

    def get_response_data(self):
        """
        Generate structured response data for this exception.

        @return: Dictionary containing structured exception information
        """
        # Compile response data including status, message, and debug info

        return {
            "code": self.status_code,
            "message": f"{self.status_message.value['status']} - {self.__get_exception_message()}",
            "status": self.status_message.value["status"],  # enum
            "error_info": {
                "reason": self.reason.value["reason"],  # enum table
                "subject_area": "AR",
                "metadata": self.metadata,
            },
            "debug_info": {
                "exception_type": type(self).__name__,
                "trace": self.__get_exception_traceback(self),
                "timestamp": str(datetime.now()),
                "environment": os.getenv("ENVIRONMENT", "production"),
                "request_id": "",# Placeholder for request ID
                "user_id": "",# Placeholder for user ID
            },
        }


# change init
# inherit entity resolver exception from Fincopilot exception
# Enum comes from a config table


class SQLExecutionException(FincopilotException):
    """Exception class specifically for User Feedback API."""

    # Custom message and component specific to SQL execution    
    message = f"An error occurred in the {SETTINGS['app']['name']} API"
    component = f"{SETTINGS['app']['name']}"
