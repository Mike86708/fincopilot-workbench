import requests
import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from datetime import datetime

# Define your API URLs
api_urls = {
  'validate_prompt': 'https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/validateprompt',
  'entity_resolver': 'https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/businessobjects/resolver',
  'sql_gen': 'https://qchxfxriu8.execute-api.us-east-1.amazonaws.com/dev/businessobjects/sqlgenerator_test'
}

# Connect to Snowflake
engine = create_engine(URL(
    user='hemanth',
    password='hemanthsnowflake',
    account='rla01593',
    warehouse='COMPUTE_WH',
    database='FINCOPILOT_QA',
    role='DATA_ENGINEER_ROLE'
))


# Parameters
num_iterations = 10  # Set the number of iterations 
selected_template_ids = '9,10,11,13,16,17,20,21,27,30,31,33,44,45,46,47,50,52,53,56,59,61,64,85,87,89,92,94,103,108,121,156,167,177,183,195,207,214,220,221,235' # Time periods

#'1-12,14-24,28,30,32,33,35,37,38,41-43,46-49,51,54-75,87-93,97-100'  # Set the template IDs to process, e.g., 'ALL' or '28,29,30' or '4-12'

# Read the prompts from the Snowflake table
input_data = pd.read_sql('SELECT PROMPT_TEMPLATE_ID, PROMPT_ID, PROMPT FROM TESTING.MASTER_USER_PROMPTS', engine)

# Function to parse the selected_template_ids
def parse_template_ids(template_ids):
    if template_ids == 'ALL':
        return None
    ids = set()
    for part in template_ids.split(','):
        if '-' in part:
            start, end = map(int, part.split('-'))
            ids.update(range(start, end + 1))
        else:
            ids.add(int(part.strip()))
    return ids

# Filter input_data based on selected_template_ids
template_ids = parse_template_ids(selected_template_ids)
print("template_ids -", template_ids)
if template_ids is not None:
    input_data = input_data[input_data['prompt_template_id'].astype(int).isin(template_ids)]

# Function to validate SQL queries
def validate_sql(query,prompt_id):
    try:
        with engine.connect() as connection:
            connection.execute(text(query))
        return True
    except Exception as e:
        print(f"SQL validation error for {prompt_id}: {e}")
        return False

# Function to process each prompt
def process_prompt(row, iteration_num):
    user_prompt = row['prompt']
    prompt_id = row['prompt_id']
    prompt_template_id = row['prompt_template_id']
    
    print(f"Prompt Inprogress - Template: {prompt_template_id}, {prompt_id}, Iteration: {iteration_num}")
    
    # Define the data to be sent with the POST request
    data = {
      "data": {
        "input": {
          "user_prompt": user_prompt,
          "domain": "finance",
          "subject_area": "accounts receivable",
          "language": "engUS"
        },
        "session": {
          "user_id": "123",
          "prompt_id": "123",
          "conversation_id": "123"
        }
      }
    }

    # Initialize DataFrame with default values
    data_df = pd.DataFrame([{
        'PROMPT_TEMPLATE_ID': prompt_template_id,
        'PROMPT_ID': prompt_id,
        'PROMPT': user_prompt,
        'ITERATION_NUM': iteration_num,
        'GUARD_OUTPUT': None,
        'GUARD': False,
        'PROMPT_GUARD_START_TIME': None,
        'PROMPT_GUARD_END_TIME': None,
        'ENTITY_EXTRACTED': None,
        'ENTITY_RESOLVER_API_VALIDATED': False,
        'ENTITY_RESOLVER_START_TIME': None,
        'ENTITY_RESOLVER_END_TIME': None,
        'TABLES_REQUIRED': None,
        'LLM_SQL_QUERY': None,
        'SQL_GEN_API_SUCCESS': False,
        'SQL_GEN_START_TIME': None,
        'SQL_GEN_END_TIME': None,
        'SQL_EXECUTABLE': False,
        'LLM_SQL_START_TIME': None,
        'LLM_SQL_END_TIME': None
    }])

    # Check if the prompt is valid
    prompt_guard_start_time = datetime.now()
    try:
        response = requests.post(api_urls['validate_prompt'], json=data)
    except requests.Timeout:
        print(f"{prompt_id}, Error from validate_prompt API: Endpoint request timed out")
        return None
    prompt_guard_end_time = datetime.now()
    data_df['PROMPT_GUARD_START_TIME'] = prompt_guard_start_time
    data_df['PROMPT_GUARD_END_TIME'] = prompt_guard_end_time

    if response.status_code == 200:
        validate_prompt_data = response.json()
        validate_prompt_data={'status_code': 200, 'body': {'status': True, 'details': 'skipped prompt guard'}}
        data_df['GUARD_OUTPUT'] = str(validate_prompt_data["body"]["details"])
        data_df['GUARD'] = validate_prompt_data["body"]["status"]
        
        # Send a POST request to the entity_resolver API
        entity_resolver_start_time = datetime.now()
        try:
            response = requests.post(api_urls['entity_resolver'], json=data)
        except requests.Timeout:
            print(f"{prompt_id}, Error from entity_resolver API: Endpoint request timed out")
            return None
        entity_resolver_end_time = datetime.now()
        data_df['ENTITY_RESOLVER_START_TIME'] = entity_resolver_start_time
        data_df['ENTITY_RESOLVER_END_TIME'] = entity_resolver_end_time

        if response.status_code == 200:
            entity_resolver_data = response.json()
            data_df['ENTITY_EXTRACTED'] = str(entity_resolver_data.get("entities_extracted", None))
            data_df['ENTITY_RESOLVER_API_VALIDATED'] = True
            
            # Send a POST request to the sql_gen API
            sql_gen_start_time = datetime.now()
            try:
                response = requests.post(api_urls['sql_gen'], json=entity_resolver_data)
            except requests.Timeout:
                print(f"{prompt_id}, Error from sql_gen API: Endpoint request timed out")
                return None
            sql_gen_end_time = datetime.now()
            data_df['SQL_GEN_START_TIME'] = sql_gen_start_time
            data_df['SQL_GEN_END_TIME'] = sql_gen_end_time

            if response.status_code == 200:
                sql_gen_data = response.json()
                data_df['SQL_GEN_API_SUCCESS'] = True                
                data_df['LLM_SQL_QUERY'] = sql_gen_data["query_string"]

                llm_sql_start_time = datetime.now()
                sql_validated = validate_sql(sql_gen_data["query_string"], prompt_id)
                llm_sql_end_time = datetime.now()
                data_df['SQL_EXECUTABLE'] = sql_validated
                data_df['LLM_SQL_START_TIME'] = llm_sql_start_time
                data_df['LLM_SQL_END_TIME'] = llm_sql_end_time

                if 'query_metadata' in sql_gen_data:                    
                    data_df['TABLES_REQUIRED'] = str(sql_gen_data["query_metadata"]["tables_used"])
                else:
                    print(f"'query_metadata' not found in sql_gen API response for {prompt_id} - Iteration: {iteration_num}: {sql_gen_data}")
            else:
                print(f"{prompt_id} - Iteration: {iteration_num}, Error from sql_gen API: {response.json()}")
                data_df['LLM_SQL_START_TIME'] = sql_gen_end_time
                data_df['LLM_SQL_END_TIME'] = sql_gen_end_time
        else:
            print(f"{prompt_id} - Iteration: {iteration_num}, Error from entity_resolver API: {response.json()}")
            data_df['SQL_GEN_START_TIME'] = entity_resolver_end_time
            data_df['SQL_GEN_END_TIME'] = entity_resolver_end_time
            data_df['LLM_SQL_START_TIME'] = entity_resolver_end_time
            data_df['LLM_SQL_END_TIME'] = entity_resolver_end_time
    else:
        print(f"Error from validate_prompt API: {response.json()}")
        data_df['ENTITY_RESOLVER_START_TIME'] = prompt_guard_end_time
        data_df['ENTITY_RESOLVER_END_TIME'] = prompt_guard_end_time
        data_df['SQL_GEN_START_TIME'] = prompt_guard_end_time
        data_df['SQL_GEN_END_TIME'] = prompt_guard_end_time
        data_df['LLM_SQL_START_TIME'] = prompt_guard_end_time
        data_df['LLM_SQL_END_TIME'] = prompt_guard_end_time
    return data_df


# Determine the optimal number of threads based on the system's CPU count
max_workers = min(30, os.cpu_count() * 2)

# Use ThreadPoolExecutor to process prompts in parallel
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    for _, row in input_data.iterrows():
        futures = []
        for i in range(1, num_iterations + 1):  # Use the num_iterations variable
            futures.append(executor.submit(process_prompt, row, i))
        
        results = []
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                results.append(result)
       # Append all results to the DataFrame at once
        if results:
            df = pd.concat(results, ignore_index=True)
            # Sort the DataFrame by PROMPT_TEMPLATE_ID, PROMPT_ID, and ITERATION_NUM
            df = df.sort_values(by=['PROMPT_TEMPLATE_ID', 'PROMPT_ID', 'ITERATION_NUM']).reset_index(drop=True)
            # Write the results DataFrame to the Snowflake table
            df.to_sql('master_user_prompts_iterations', engine, if_exists='append', index=False, schema='dev_testing')
            print(f"Prompt Completed - {row['prompt_id']}")
            # Clear the DataFrame after writing to Snowflake
            df = pd.DataFrame(columns=['PROMPT_TEMPLATE_ID', 'PROMPT_ID', 'PROMPT', 'ITERATION_NUM', 'GUARD_OUTPUT', 'GUARD', 'PROMPT_GUARD_START_TIME', 'PROMPT_GUARD_END_TIME', 'ENTITY_EXTRACTED', 'ENTITY_RESOLVER_API_VALIDATED', 'ENTITY_RESOLVER_START_TIME', 'ENTITY_RESOLVER_END_TIME', 'TABLES_REQUIRED', 'LLM_SQL_QUERY', 'SQL_GEN_API_SUCCESS', 'SQL_GEN_START_TIME', 'SQL_GEN_END_TIME', 'SQL_EXECUTABLE', 'LLM_SQL_START_TIME', 'LLM_SQL_END_TIME'])

# Close the Snowflake connection
engine.dispose()

print("The data has been successfully saved to the Snowflake table 'MASTER_USER_PROMPTS_ITERATIONS'.")
