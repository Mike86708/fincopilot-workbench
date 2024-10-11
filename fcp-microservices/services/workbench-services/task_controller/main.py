from typing import List, Optional
from datetime import datetime
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from mangum import Mangum
from calls import get_filters, get_tasks, get_lookups, invoke_lambda_function
from logging_utils import log_to_sqs

app = FastAPI()

class TaskRequest(BaseModel):
    version: str = "1.0"
    get_filters: bool
    get_tasks: bool
    get_lookups: bool
    is_default: bool
    update_task_status: Optional[bool] = None
    task_status_fields: Optional[dict] = None
    filter_tasks: Optional[dict] = None

@app.post("/")
async def root(task_request: TaskRequest):
    request_dict = task_request.dict()
    # Print the request body to the console
    print("Received request body:", json.dumps(request_dict, indent=2))

    response = {} # Make a dictionary for the final response, start forming it below
    
    if task_request.is_default and task_request.get_filters:
        # for is_default to be true, get filters needs to be as well

        if task_request.get_tasks:
            raise HTTPException(detail="Cannot have is_default and get_tasks both true", status_code=500)

        filters = get_filters()
        response['task_filters']  = filters # assign the filters to response key "task_filters"

        # Get the current date and time
        now = datetime.now()
        current_month = now.strftime('%b')  # 'Jan', 'Feb', etc.
        current_year = now.strftime('%Y')   # '2024', '2025', etc.

        # Create the display name for the current month and year
        current_display_name = f'{current_month} {current_year}'


        periods = filters['period']
        for item in periods:
            if item['display_name'] == current_display_name:
                current_period_id = item['id']

        filter_tasks = {
                "version": 1,
                "entity_id": -1000,
                "period_id": current_period_id,
                "folder_id": -1000,
                "description": "",
                "task_status": -1000,
                "assigned_performer_id": -1000,
                "approval_status": -1000,
                "tags": ""
                } 
        # This is the default filter, it has -1000(meaning any) value on all parameters and is the curent id
        tasks = get_tasks(filters=filter_tasks)
        if len(tasks) > 0:
            response["tasks"] = tasks
        else:
            response["tasks"] = {"message":"No tasks found for this period."}

    if task_request.get_filters:
        if not task_request.is_default:
            filters = get_filters()
            response["task_filters"] = filters
    
    if task_request.get_tasks:
        if not(task_request.is_default):
            filters_list = task_request.filter_tasks or {}
            tasks = get_tasks(filters=filters_list)
            response["tasks"] = tasks
    
    if task_request.get_lookups:
        lookups = get_lookups()
        response["lookups"] = lookups
    
    if task_request.update_task_status:
        if not task_request.task_status_fields:
            raise HTTPException(status_code=400, detail="You need to have task status fields in order to update task status")
        try:
            r = invoke_lambda_function("new_update_task_status_autodeploy_dev",payload=json.dumps(task_request.task_status_fields))
        except Exception as e:
            print(str(e))
        response["task_update_status"] = r
    
    msg_string = "Request processed - Version "+task_request.version
    return {"message": msg_string, "data": response}


handler = Mangum(app)
