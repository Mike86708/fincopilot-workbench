import logging
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi.responses import JSONResponse
import json
from datetime import datetime
from mangum import Mangum
from invoker import invoke_lambda_function
from utils import send_log_to_sqs
from lib.exception.exception_codes import Reason
from lib.exception.exceptions import AccountControllerException
from fastapi.exceptions import RequestValidationError
import os

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

class InfoRequest(BaseModel):
    get_filters: bool
    get_data: bool
    default_filter: bool
    payload: Optional[Dict[str, Any]] = None

lambda_function_mapping = {
    'balance_summary': "fincopilot_workbench_get_balance_summary",
    'trial_balance': "fincopilot_workbench_get_trial_balance_summary_dev_autodeploy",
    'account_activity': "fincopilot_workbench_get_accounting_activity",
    'income_statement': "fincopilot_workbench_get_income_statement"
}


# Global exception handler for JSONDecodeError
@app.exception_handler(RequestValidationError)
async def json_decode_exception_handler(request: Request, exc: RequestValidationError):
    logger.error(f"JSON decode error: {str(exc)}")
    # Custom exception: throw an AccountControllerException with specific information
    raise AccountControllerException(
        message="Invalid JSON format or missing value in the request.",
        reason=Reason.INVALID_INPUT,
        metadata={"details": f"JSON decode error: {str(exc)}"}
    )

# Custom Exception Handler for AccountControllerException
@app.exception_handler(AccountControllerException)
async def account_controller_exception_handler(request: Request, exc: AccountControllerException):
    logger.error(f"Exception occurred: {exc.message}")
    send_log_to_sqs(f"AccountControllerException: {exc.get_response_data()}")
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.message
    )
def validate_request(info_request: InfoRequest):
    # Ensure only one of get_data, default_filter, or get_filters is true
    truthy_fields = sum([bool(info_request.get_data), bool(info_request.default_filter), bool(info_request.get_filters)])
    
    if truthy_fields != 1:
        logger.error("Only one of 'get_data', 'default_filter', or 'get_filters' should be true.")
        raise AccountControllerException(
            message="Only one of 'get_data', 'default_filter', or 'get_filters' should be true.",
            reason=Reason.INVALID_INPUT
        )

    # Validate that payload exists when required
    if not info_request.payload:
        logger.error("Payload is required but was not provided.")
        raise AccountControllerException(
            message="Payload is required but was not provided.",
            reason=Reason.INVALID_INPUT
        )
    
    # Extract payload
    payload = info_request.payload
    
    # Check that 'type' is present in payload
    if 'type' not in payload:
        logger.error("Payload must contain a 'type' field.")
        raise AccountControllerException(
            message="Missing 'type' in payload.",
            reason=Reason.INVALID_INPUT
        )
    
    # Valid types for 'type' field
    valid_types = ["balance_summary", "trial_balance", "income_statement", "account_activity"]
    data_to_retrieve = payload.get('type')
    
    # Validate that the type is valid
    if data_to_retrieve not in valid_types:
        logger.error(f"Invalid 'type' in payload: {data_to_retrieve}")
        raise AccountControllerException(
            message=f"Invalid 'type': {data_to_retrieve}.",
            reason=Reason.INVALID_INPUT,
            metadata={"allowed_values": valid_types}
        )

    # Determine which operation is being performed
    operation_type = 'get_data' if info_request.get_data else \
                     'default_filter' if info_request.default_filter else 'get_filters'

    # Define required fields for each operation type
    if operation_type == 'get_data':
        required_params = ['subsidiary_id']
        if data_to_retrieve == 'balance_summary':
            required_params.extend(['period_id'])
        if data_to_retrieve == 'trial_balance':
            required_params.extend(['period_id'])
        if data_to_retrieve == 'income_statement':
            required_params.extend(['from_period_id', 'to_period_id'])
        if data_to_retrieve == 'account_activity':
            required_params.extend(['type', 'from_period_id', 'to_period_id', 'account_id'])
        
    elif operation_type == 'default_filter':
        required_params = []
        if data_to_retrieve == 'account_activity':
            required_params = ['type']  # If it's account activity, type under parameters is needed.
        
    elif operation_type == 'get_filters':
        required_params = []
        if data_to_retrieve == 'income_statement':
            required_params = ['subsidiary_id']
        if data_to_retrieve == 'account_activity':
            required_params = ['type', 'subsidiary_id']  # Account activity may require additional parameters.

    # Check if parameters are required and validate them if present
    if required_params:
        if 'parameters' not in payload:
            logger.error(f"Missing 'parameters' in payload for {data_to_retrieve}.")
            raise AccountControllerException(
                message=f"Missing 'parameters' in payload for {data_to_retrieve}.",
                reason=Reason.INVALID_INPUT
            )
        
        parameters = payload['parameters']
        for param in required_params:
            if param not in parameters:
                logger.error(f"Missing required parameter '{param}' in payload for {data_to_retrieve}.")
                raise AccountControllerException(
                    message=f"Missing required parameter '{param}' in payload for {data_to_retrieve}.",
                    reason=Reason.INVALID_INPUT
                )

        # Additional validation: Ensure 'type' in parameters is 'balance_summary' or 'trial_balance' if data_to_retrieve is 'account_activity'
        if data_to_retrieve == 'account_activity':
            param_type = parameters.get('type')
            if param_type not in ["balance_summary", "trial_balance"]:
                logger.error(f"Invalid 'type' in parameters for 'account_activity'. Must be 'balance_summary' or 'trial_balance', got: {param_type}")
                raise AccountControllerException(
                    message=f"Invalid 'type' in parameters for 'account_activity'. Must be 'balance_summary' or 'trial_balance'.",
                    reason=Reason.INVALID_INPUT,
                    metadata={"allowed_values": ["balance_summary", "trial_balance"]}
                )

    # Log successful validation
    logger.info("Request validation successful")


def fetch_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        data_to_retrieve = payload.get("type")
        if not data_to_retrieve:
            logger.error("Missing 'type' in payload")
            raise AccountControllerException(
                message="Missing 'type' in payload.",
                reason=Reason.INVALID_INPUT
            )

        payload_for_lambda = payload.get("parameters", {})
        if not payload_for_lambda:
            logger.error("Missing 'parameters' in payload")
            raise AccountControllerException(
                message="Missing 'parameters' in payload.",
                reason=Reason.INVALID_INPUT
            )
        
        type = payload_for_lambda.get("type")
        type_for_lambda = "BALANCE_SUMMARY" if type == "balance_summary" else "TRIAL_BALANCE"
        payload_for_lambda["type"] = type_for_lambda

        logger.info(f"Fetching data for {data_to_retrieve}")
        
        # Invoke backend service
        response_raw = invoke_lambda_function(lambda_function_mapping[data_to_retrieve], payload=json.dumps(payload_for_lambda))
        
        # Parse the response
        response_dict = json.loads(response_raw)
        
        # Check if the response contains an error
        if "error" in response_dict or response_dict.get("statusCode", 200) != 200:
            
            body = json.loads(response_dict["body"])
            
            error_message = body.get("message", "Backend service error")
            print(error_message)
            error_code = response_dict.get("statusCode", 500)
            logger.error(f"Backend service returned an error: {error_message}")
            
            # Raise an exception based on backend error
            raise AccountControllerException(
                message=f"Error from backend service: {error_message}",
                reason=Reason.RETRIEVE_DATA_ERROR,
                metadata={"status_code": error_code}
            )

        logger.info(f"Data successfully retrieved for {data_to_retrieve}")
        return {"requested_data": response_dict["body"]}
    
    except AccountControllerException as ace:
        # FastAPI will catch this and route it to the custom exception handler
        raise ace
    
    except Exception as e:
        logger.error(f"Error retrieving data: {str(e)}")
        send_log_to_sqs(f"Error retrieving data: {str(e)}")
        raise AccountControllerException(
            message="Error retrieving data.",
            reason=Reason.RETRIEVE_DATA_ERROR,
            e=e
        )


def fetch_filters(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    try:
        response_data = {}


        filters_dict = json.loads(invoke_lambda_function("fincopilot_get_workbench_filters"))
        response_data["filters"] = filters_dict.get("entity", [])

        subsid_json = json.loads(invoke_lambda_function("fincopilot_workbench_get_subsidiary_dev_autodeploy"))
        response_data["subsidiaries"] = subsid_json if subsid_json else []

        acc_periods = json.loads(invoke_lambda_function("fincopilot_workbench_get_accounting_period_dev_autodeploy"))
        response_data["accounting_periods"] = acc_periods if acc_periods else []

        logger.info("Filters, subsidiaries, and accounting periods successfully retrieved")

        # If payload is None, log and return an empty response
        if payload is None:
            logger.warning("No payload provided for fetching filters.")
            return response_data
        
        type = payload.get("type")
        if type in ["account_activity", "income_statement"]:
            parameters = payload.get("parameters", {})
            subsidiary_id = parameters.get("subsidiary_id")
            sub_type = "income_statement" if type == "income_statement" else parameters.get("type", "")

            if not subsidiary_id or not sub_type:
                logger.error(f"Missing subsidiary_id or type in parameters for {type}")
                return response_data  # Returning partial response

            filter_type = "INCOME_STATEMENT" if sub_type == "income_statement" else "BALANCE_SUMMARY" if sub_type == "balance_summary" else "TRIAL_BALANCE"
            account_filter_payload = {"subsidiary_id": subsidiary_id, "data_type": filter_type} 
            account_filter_response  = json.loads(invoke_lambda_function("fincopilot_workbench_get_account_filter", payload=json.dumps(account_filter_payload)))
            account_filter_body = json.loads(account_filter_response.get("body", "[]"))
            response_data["account_filter"] = account_filter_body
            logger.info(f"Account filter successfully retrieved for {type}")

        return response_data

    except Exception as e:
        logger.error(f"Error retrieving filters: {str(e)}")
        send_log_to_sqs(f"Error retrieving filters: {str(e)}")
        raise AccountControllerException(
            message="Failed to retrieve filters.",
            reason=Reason.FAIL_TO_RETRIEVE_FILTERS,
            e=e
        )

def find_value_for_period(accounting_periods: list, period_label: str) -> Optional[int]:
    """
    Find the value associated with a given period label (e.g., 'Jul 2022').
    """
    if not accounting_periods or not period_label:
        return None

    for period in accounting_periods:
        if "children" in period:
            result = find_value_for_period(period["children"], period_label)
            if result is not None:
                return result
        elif period.get("label") == period_label:
            return period.get("value")
    return None

def update_payload_for_default_filter(payload: dict, current_period_int: int, current_period_string: str, default_subsidiary_id: int, default_account_id: int):
    """
    Update the payload based on the type and subsidiary/period information.
    """
    try:
        if payload["type"] == "balance_summary" or payload["type"] == "trial_balance":
            payload["parameters"] = {
                "period_id": current_period_string,
                "subsidiary_id": default_subsidiary_id
            }

        elif payload["type"] == "income_statement":
            payload["parameters"] = {
                "from_period_id": current_period_int,
                "to_period_id": current_period_int,
                "subsidiary_id": default_subsidiary_id
            }

        elif payload["type"] == "account_activity":
            if "parameters" not in payload:
                payload["parameters"] = {}

            payload["parameters"].update({
                "account_id": default_account_id,
                "from_period_id": current_period_int,
                "to_period_id": current_period_int,
                "subsidiary_id": default_subsidiary_id
            })

        logger.info(f"Payload updated for {payload['type']}: {payload['parameters']}")
        return payload

    except KeyError as e:
        logger.error(f"Missing key in payload: {str(e)}")
        raise AccountControllerException(
            message=f"Invalid payload format: missing key {str(e)}.",
            reason=Reason.INVALID_INPUT
        )

def fetch_default_filter(payload: dict) -> Dict[str, Any]:
    try:
        response_dict = {}

        # Get current period ID (integer and string)
        acc_periods = json.loads(invoke_lambda_function("fincopilot_workbench_get_accounting_period_dev_autodeploy"))
        current_period_string = datetime.now().strftime('%b %Y')  # Format current date as 'MMM YYYY'
        current_period_int = find_value_for_period(acc_periods, current_period_string)

        if current_period_int is None:
            logger.error(f"Could not find current period value for {current_period_string}")
            raise AccountControllerException(
                message=f"Invalid period {current_period_string}.",
                reason=Reason.INVALID_INPUT
            )

        # Get first subsidiary ID
        # subsid_json = json.loads(invoke_lambda_function("fincopilot_workbench_get_subsidiary_dev_autodeploy"))
        # first_subsidiary_id = subsid_json[0]['value'] if subsid_json else None

        # if not first_subsidiary_id:
        #     logger.error("Failed to retrieve first subsidiary ID.")
        #     raise AccountControllerException(
        #         message="No subsidiaries found.",
        #         reason=Reason.INVALID_INPUT
        #     )
        default_subsidiary_id = int(os.getenv("DEFAULT_SUBSIDIARY_ID"))
        default_account_id = int(os.getenv("DEFAULT_ACCOUNT_ID"))

        # Update the payload
        payload = update_payload_for_default_filter(payload, current_period_int, current_period_string, default_subsidiary_id, default_account_id)
        response_dict = fetch_filters(payload)

        # Fetch data
        # data_to_retrieve = payload.get("type")
        # parameters = payload.get("parameters", {})
        # logger.info(f"Retrieving default filter data for {data_to_retrieve}")
        # response_data = json.loads(invoke_lambda_function(lambda_function_mapping[data_to_retrieve], payload=json.dumps(parameters)))
        
        # response_dict.update({"data": response_data.get("body")})
        # logger.info(f"Default filter data retrieved for {data_to_retrieve}")
        response_dict.update(fetch_data(payload))

        return {"requested_data": response_dict}
    except AccountControllerException as ace:
        # FastAPI will catch this and route it to the custom exception handler
        raise ace
    
    except Exception as e:
        logger.error(f"Error retrieving default filter data: {str(e)}")
        send_log_to_sqs(f"Error retrieving default filter data: {str(e)}")
        raise AccountControllerException(
            message="Error retrieving default filter data.",
            reason=Reason.FAIL_TO_RETRIEVE_FILTERS,
            e=e
        )

@app.post("/")
def root(info_request: InfoRequest):
    try:
        validate_request(info_request)

        response_data = {}

        if info_request.get_filters:
            response_data = fetch_filters(info_request.payload)

        if info_request.get_data:
            response_data = fetch_data(info_request.payload)

        if info_request.default_filter:
            response_data = fetch_default_filter(info_request.payload)

        return response_data
    # Catch custom AccountControllerException and pass it to the custom handler
    except AccountControllerException as ace:
        # FastAPI will catch this and route it to the custom exception handler
        raise ace
    
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        raise AccountControllerException(
            message="Error processing request.",
            reason=Reason.FAIL_TO_PROCESS_REQUEST,
            e=e
        )

handler = Mangum(app=app)
