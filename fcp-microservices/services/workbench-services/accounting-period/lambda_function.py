import json
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus          
from sqlalchemy.engine.base import Engine
from snowflake.sqlalchemy import URL
from accounting_period_exceptions import AccountingPeriodException, Reason
from snowflake_common import *
from utils import Utils

"""
Lambda entry point for getting accounting periods

Note: Please use Lambda Proxy integration , else event will not 
be triggered.
Environment Vars
    postgres_database  
    postgres_host 
    postgres_port 
    region 
    secret_arn 
Layers Used
   Layer for psycopg2 on python 3.8    
"""
def lambda_handler(event, context):
    try:
        filterList = get_accounting_periods()
        status_code = 200   
        if(filterList is None): 
            status_code = Reason.NO_DATA_FOUND
            message='{"status":"failure","message": "Accounting periods does not exist"}'
        else:   
            return filterList  
    except AccountingPeriodException as e:
        print(f"Error in lambda_handler: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps(e.get_response_data(), indent=2),
        }
    except Exception as e:
        # Catch any other unforeseen exceptions
        print(f"Unexpected error in lambda_handler: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "An unexpected error occurred.",
                    "details": str(e),
                },
                indent=2,
            ),
        }
    return response 
            
"""
This method gets the workbench tasks based on the 
filters provided. By default all tasks are returned
"""
def get_accounting_periods():
    engine = get_snowflake_engine()
    sqlQuery = ''
    try:
        with engine.connect() as connection:
            try:
                with open('accounting_periods.sql', 'r') as file:
                    sqlQuery = file.read()
            except FileNotFoundError as e:
                raise AccountingPeriodException(
                    "SQL query file not found",
                    reason=Reason.SQL_FILE_NOT_FOUND,
                    subcomponent="get_accounting_periods"
                ) from e
            except Exception as e:
                raise AccountingPeriodException(
                    "Error reading SQL query file",
                    reason=Reason.FILE_READ_ERROR,
                    subcomponent="get_accounting_periods"
                ) from e
            
            if sqlQuery is None or not sqlQuery.strip():
                raise AccountingPeriodException(
                    "SQL query is empty",
                    reason=Reason.INVALID_SQL_QUERY,
                    subcomponent="get_accounting_periods"
                )
            accounting_data = connection.execute(text(sqlQuery), None).fetchall()
            yearIds = []
            quarter_month_ids = []
            year_array = []
            quarter_array = []
            month_array = []
            prior_year = ''
            prior_qtr = ''

            for row in accounting_data:
                id = row[0]
                year = row[1]
                quarter = row[2]
                month = row[3]
                month_ele = { "label" : month, "value": id}

                if(quarter not in quarter_month_ids):
                    quarter_Ele = []
                    quarter_month_ids.append(quarter)
                if quarter != prior_qtr and prior_qtr != '':
                    quarter_Ele = {"label" : prior_qtr,  "children" : month_array}
                    quarter_array.append(quarter_Ele)
                    prior_qtr = quarter
                    month_array = []
                    month_array.append(month_ele)
                else:
                    month_array.append(month_ele)

                if year not in yearIds:
                    yearIds.append(year)
                if year != prior_year and prior_year != '':
                    year_Ele = {"label" : prior_year,  "children" : quarter_array}
                    year_array.append(year_Ele)   
                    quarter_array = []
                    
                prior_qtr = quarter
                prior_year = year

            # last elements
            quarter_Ele = {"label" : prior_qtr,  "children" : month_array}
            quarter_array.append(quarter_Ele)
            year_Ele = {"label" : prior_year,  "children" : quarter_array}
            year_array.append(year_Ele)    

            return year_array

    except Exception as e:
        raise AccountingPeriodException(
            f"An error occurred while getting accounting periods. {e}",
            reason=Reason.DATABASE_EXECUTION_ERROR,
            subcomponent="get_accounting_periods"
        ) from e