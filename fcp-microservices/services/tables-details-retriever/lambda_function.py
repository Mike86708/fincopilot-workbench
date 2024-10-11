import json
from lib.table_details import TableDetailsService

def lambda_handler(event, context):
    try:
        # Get the user input
        subject_area = event.get("subject_area")

        # Check if required fields are present
        if subject_area=='' :
            return {
                "statusCode": 400,
                "body": json.dumps("Invalid input: subject_area is mandatory"),
            }
        
        td = TableDetailsService(subject_area=subject_area)
        result, metadata = td.get_subjectarea_tables()           
        # Return the response
        return {
            "statusCode": 200,
            "body": 
                {
                    
                    "result": result,
                    "data": metadata
                }
            
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error with validating prompt: {str(e)}"),
        }
