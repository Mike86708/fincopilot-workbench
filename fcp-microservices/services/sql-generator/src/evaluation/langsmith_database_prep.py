import snowflake.connector
import csv
import os
import json
from src.utils.main import logger
from src.utils.test import TEST_SETTINGS
from src.evaluation.model_response import ChatBot
from src.utils.api_utils import get_resolver_api, input_parser
import pandas as pd
import tqdm


from src.utils.database_utils import *


chat_bot = ChatBot() #chat_bot

# def fetch_data_from_snowflake(conn, query) -> pd.DataFrame:
#     """Fetch data from Snowflake based on the given query."""
#     cur = conn.cursor()
#     cur.execute(query)
#     data = cur.fetchall()
#     cur.close()

#     df = pd.DataFrame(data)

#     return df

def write_to_csv(file_path, header, rows):
    """Write data to a CSV file."""
    with open(file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(rows)



def write_metrics_to_csv(filename, model_inputs):
    """Append model_inputs data to a CSV file."""

    # Check if the file exists
    file_exists = os.path.isfile(filename)
    
    with open(filename, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)

        if not file_exists:
            # Create a header from both metrics and model_inputs keys
            header = list(model_inputs.keys())
            writer.writerow(header)

        # Initialize row with data from metrics and model_inputs
        # row = [metrics.get(key, '') for key in list(metrics.keys())]
        # row += [model_inputs.get(key, '') for key in list(model_inputs.keys())]
        row = [model_inputs.get(key, '') for key in list(model_inputs.keys())]
        writer.writerow(row)

        logger.info(f"Model inputs appended to {filename}: {model_inputs}")





def llm_call(user_prompt) -> dict:
    '''
    Call the LLM and get the response

    @param user_prompt: User prompt
    @return: LLM response
    '''
    global chat_bot
    
    resolver_data = get_resolver_api(user_prompt)
    context_data = input_parser(resolver_data)

    user_prompt = context_data.get('question', '')
    context_str = context_data.get('context', '')

    if not isinstance(context_str, str):
        context_str = json.dumps(context_str)
    try:
        # Get model inputs
        model_inputs = chat_bot.get_model_inputs(user_prompt, context_str)
        model_inputs.pop('query_string', None)
        return model_inputs
    except Exception as e:  
        logger.error(f"error in llm_call: {e}")
        return{}
    # if 'payload' in response:
    #     if 'query_string' in response['payload']:
    #         query_string = response['payload']['query_string']
    #         query_metadata = response['payload'].get('query_metadata', {})
    #         model_inputs = response.get('metadata', {}).get('model_inputs', {})

    #         # metrics = {
    #         #     'tables_used': json.dumps(query_metadata.get('tables_used', [])),
    #         #     'databases_used': json.dumps(query_metadata.get('databases_used', [])),
    #         # }
    #         # metrics = query_metadata

    #         # Add query_string to model_inputs
    #         model_inputs['query_string'] = query_string

    # else:
    #     # metrics = {}
    #     model_inputs['query_string'] = 'error'

    # # # user_prompt and golden_sql in the metrics
    # # metrics['user_prompt'] = user_prompt
    # # metrics['golden_sql'] = golden_sql

    # # data = pd.DataFrame(model_inputs | metrics)
    # # Write metrics and model_inputs to CSV
    # # write_metrics_to_csv('metrics.csv', metrics, model_inputs)
    # # write_metrics_to_csv('metrics.csv', data)

    
    # return model_inputs 

  
def filter_data(starting_data_file: str, ending_data_file: str) -> None:
    '''
    Filter the data

    @param starting_data_file: starting data file
    @param ending_data_file: ending data file
    '''
    ending_data_file = os.path.join(TEST_SETTINGS['evaluation']['intermediate_results_path'], ending_data_file)
    starting_data_file = os.path.join(TEST_SETTINGS['evaluation']['intermediate_results_path'], starting_data_file)

    data = pd.read_csv(starting_data_file)
    

    # Filter the data however you like here

    # create a filter for checking if the "GOLDEN_SQL_QUERY" column has "DATE" in it
    filter = data['GOLDEN_SQL_QUERY'].str.contains('DATEADD', case=False)

    # apply the filter to the dataframe
    data = data[filter]




    data.to_csv(ending_data_file, index=False)
 

def main():

    connect_to_snowflake()


    
    golden_outputs = read_from_database("FINCOPILOT_QA.TESTING.PROMPTS_GOLDEN_SQL_CR", columns=["PROMPT_ID", 'PROMPT', "GOLDEN_SQL_QUERY"])
    golden_outputs.dropna(subset=['GOLDEN_SQL_QUERY'], inplace=True)
    
    disconnect_from_snowflake()

    complete_data = []
    for index, row in tqdm.tqdm(golden_outputs.iterrows(), total=len(golden_outputs)):
        
        # prompt_template_id = row.get('PROMPT_TEMPLATE_ID')
        prompt_id = row.get('PROMPT_ID')
        user_prompt = row.get('PROMPT')
        golden_sql = row.get('GOLDEN_SQL_QUERY')
        # Call ask 
        data_row = llm_call(user_prompt)


        # Add golden_sql to csv data row
        # data_row['PROMPT_TEMPLATE_ID'] = prompt_template_id
        data_row['PROMPT_ID'] = prompt_id
        data_row['GOLDEN_SQL_QUERY'] = golden_sql
        data_row['USER_PROMPT'] = user_prompt
        
        complete_data.append(data_row)

    
    complete_data = pd.DataFrame(complete_data)

    complete_data.to_csv(os.path.join(TEST_SETTINGS['evaluation']['intermediate_results_path'], 'langsmith_preval_inputs.csv'), index=False)

if __name__ == "__main__":
    # filter_data('langsmith_preval_inputs.csv', 'langsmith_preval_inputs_date_prompts.csv')
    main()



    
