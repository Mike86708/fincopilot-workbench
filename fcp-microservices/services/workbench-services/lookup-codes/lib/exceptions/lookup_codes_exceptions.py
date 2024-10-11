
from workbench_exception_codes import *
from workbench_exceptions import *

class LookupCodesException(FincopilotException):
    message = f"An error occurred in Lookup Codes API"
    component = f"Lookup codes general"