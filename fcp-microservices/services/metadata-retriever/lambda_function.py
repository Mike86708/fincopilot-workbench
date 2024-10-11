import json
from lib.table_metadata import TableMetaDataManager

def lambda_handler(event, context):
    try:
        # Get the user input
        subject_area = event.get("subject_area")
        table_list = event.get("table_list")
        sample_num_of_rows = event.get("sample_num_of_rows")

        # Check if required fields are present
        if subject_area=='' or table_list=='' or  sample_num_of_rows=='':
            return {
                "statusCode": 400,
                "body": json.dumps("Invalid input: All the input details are mandatory"),
            }
        
        metadata_manager = TableMetaDataManager(subject_area=subject_area,tables=table_list, sample_rows=sample_num_of_rows)
        result, metadata = metadata_manager.get_metadata()           
        # Return the response
        return {
            "statusCode": 200,
            "body": 
                {
                    
                    "result": result,
                    "data": metadata
                }
            ,
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error with validating prompt: {str(e)}"),
        }
