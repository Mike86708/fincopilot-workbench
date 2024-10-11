
from workbench_exception_codes import *
from workbench_exceptions import *

class SubsidiaryException(FincopilotException):
    message = f"An error occurred in Lookup Codes API"
    component = f"Lookup codes general"