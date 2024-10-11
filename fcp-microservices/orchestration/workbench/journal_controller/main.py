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
from lib.exception.exceptions import JournalControllerException
import os

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

class InfoRequest(BaseModel):
    get_filters: bool
    get_data: bool
    filters: Optional[Dict[str, Any]] = None

# Custom Exception Handler for JournalControllerException
@app.exception_handler(JournalControllerException)
async def journal_controller_exception_handler(request: Request, exc: JournalControllerException):
    logger.error(f"Exception occurred: {exc.message}")
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.get_response_data()
    )

def validate_request(info_request: InfoRequest):
    # Ensure that get_data and get_filters are not both true
    if info_request.get_data and info_request.get_filters:
        logger.error("Both 'get_data' and 'get_filters' cannot be true simultaneously.")
        raise JournalControllerException(
            message="Invalid request: 'get_data' and 'get_filters' cannot be true at the same time.",
            reason=Reason.INVALID_INPUT
        )

    logger.info("Request validation successful")



def fetch_filters():
    try:
        response_data = {}

        filters_dict = json.loads(invoke_lambda_function("fincopilot_workbench_journal_filter"))
        response_data["filters"] = filters_dict if filters_dict else []

        return response_data

    except Exception as e:
        logger.error(f"Error retrieving filters: {str(e)}")
        send_log_to_sqs(f"Error retrieving filters: {str(e)}")
        raise JournalControllerException(
            message="Error retrieving filters.",
            reason=Reason.FAIL_TO_RETRIEVE_FILTERS,
            e=e
        )

def fetch_data(filters: Dict[str, Any]) -> Dict[str, Any]:
    try:
        response_raw = invoke_lambda_function(
            "fincopilot_workbench_get_journals", 
            json.dumps(filters) if filters else None
        )
        response_dict = json.loads(response_raw)
        
        return {"requested_data": response_dict["body"]}

    except Exception as e:
        logger.error(f"Error retrieving data: {str(e)}")
        send_log_to_sqs(f"Error retrieving data: {str(e)}")
        raise JournalControllerException(
            message="Error retrieving data.",
            reason=Reason.RETRIEVE_DATA_ERROR,
            e=e
        )


@app.post("/")
def root(info_request: InfoRequest):
    try:
        validate_request(info_request)

        response_data = {}

        if info_request.get_filters:
            response_data = fetch_filters()

        if info_request.get_data:
            response_data = fetch_data(info_request.filters)

        return response_data

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        raise JournalControllerException(
            message="Error processing request.",
            reason=Reason.FAIL_TO_PROCESS_REQUEST,
            e=e
        )

handler = Mangum(app=app)
