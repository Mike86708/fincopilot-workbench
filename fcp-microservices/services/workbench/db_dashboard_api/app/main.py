import os
import json
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from mangum import Mangum
from pydantic import BaseModel
import psycopg2
from cexec import run_personal, run_all
from get_secrets import get_db_creds
from dotenv import load_dotenv
# loading variables from .env file
load_dotenv()

app = FastAPI()

class dashRequest(BaseModel):
    version: str = "1.0"
    entity_id: int
    period_id: int
    assigned_performer_id: int
    assigned_approver_id: int
    email: Optional[str] = None


@app.post("/")
async def root(user_request: dashRequest):
    
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        user=os.getenv("DB_USER"),
        port=os.getenv("PORT"),
        password=get_db_creds(),
        database=os.getenv("DATABASE")
    ) 
    
    user_dict = user_request.dict()

    try:
        entity_id = user_dict["entity_id"]
    except:
        raise HTTPException(status_code=400, detail="Bad request: couldn't find mandatory field 'entity_id' in your request")
    
    try:
        period_id = user_dict["period_id"]
    except:
        raise HTTPException(status_code=400, detail="Bad request: couldn't find mandatory field 'period' in your request")
    
    try:
        assigned_performer_id = user_dict["assigned_performer_id"]
    except:
        raise HTTPException(status_code=400, detail="Bad request: couldn't find mandatory field 'assigned_performer_id' in your request")
    try:
        assigned_approver_id = user_dict["assigned_approver_id"]
    except:
        raise HTTPException(status_code=400, detail="Bad request: couldn't find mandatory field 'assigned_approver_id' in your request")


    if not (user_dict["email"]): # if there is no email included
        try:
            response_output = run_all(entity_id=entity_id,period_id=period_id,assigned_approver_id=assigned_approver_id,assigned_performer_id=assigned_performer_id, conn=conn)
            return {'message': response_output}
        except Exception as e:
            raise HTTPException(status_code=500, detail="Couldn't perform query or queries to DB: "+str(e))
    else:
        try:
            response_output = run_personal(entity_id=entity_id,period_id=period_id,assigned_performer_id=assigned_performer_id,email=user_dict['email'], conn=conn)
            return {"message":response_output}
        except Exception as e:
            raise HTTPException(status_code=500, detail="Couldn't perform query or queries to DB: "+str(e))
    

handler = Mangum(app)