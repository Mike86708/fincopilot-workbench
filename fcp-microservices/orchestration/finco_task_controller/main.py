from typing import List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
from calls import getFilters, getTasks, getLookups, invoke_lambda_function
from mangum import Mangum
from datetime import datetime

app = FastAPI()

class TaskRequest(BaseModel):
    version: str = "1.0"
    get_filters: bool
    get_tasks: bool
    get_lookups: bool
    is_default: bool
    filter_tasks: Optional[dict] = None

@app.post("/")
async def root(task_request: TaskRequest):
    request_dict = task_request.dict()
    
    # Print the request body to the console
    print("Received request body:", json.dumps(request_dict, indent=2))
    
    response = {}
    
    if task_request.is_default and task_request.get_filters:

        if task_request.get_tasks:
            raise HTTPException(detail="Cannot have is_default and get_tasks both true", status_code=500)

        filters = getFilters()
        response['task_filters']  = filters
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
        } #filter with right stuff heres
        tasks = getTasks(filters=filter_tasks)
        if len(tasks) > 0:
            response["tasks"] = tasks
        else:
            response["tasks"] = {"message":"No tasks found for this period."}

    if task_request.get_filters:
        if not task_request.is_default:
            filters = getFilters()
            response["task_filters"] = filters
    
    if task_request.get_tasks:
        if not(task_request.is_default):
            filters_list = task_request.filter_tasks or {}
            tasks = getTasks(filters=filters_list)
            response["tasks"] = tasks
    
    if task_request.get_lookups:
        lookups = getLookups()
        response["lookups"] = lookups
    
    if not (task_request.get_filters or task_request.get_tasks or task_request.get_lookups or task_request.is_default):
        raise HTTPException(status_code=400, detail="No action specified in the request")
    
    msgString = "Request processed - Version "+task_request.version
    return {"message": msgString, "data": response}


handler = Mangum(app)
