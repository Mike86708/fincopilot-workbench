import pandas as pd
import hashlib
from datetime import datetime
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import os
import threading

run_date_time = datetime.now().strftime('%Y%m%d%H%M%S')

# Snowflake connection parameters
user = 'hemanth'
password = 'hemanthsnowflake'
account = 'rla01593'
warehouse = 'COMPUTE_WH'
database = 'FINCOPILOT_QA'
schema = 'DEV_TESTING'

# Specify the prompt_template_ids you want to process  2,5,10,11,12
selected_template_ids = '9,10,11,13,16,17,20,21,27,30,31,33,44,45,46,47,50,52,53,56,59,61,64,85,87,89,92,94,103,108,121,156,167,177,183,195,207,214,220,221,235' # Time periods

# Create SQLAlchemy engine with connection pooling
engine = create_engine(f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}', pool_size=10, max_overflow=20)

# Initialize an empty DataFrame to store results
results_df = pd.DataFrame(columns=[
    'prompt_template_id', 'prompt_id', 'iter_num', 'run_date_time', 'golden_sql', 'llm_gen_sql', 'llm_gen_sql_executable',
    'golden_cols', 'llm_gen_cols', 'golden_row_count', 'llm_row_count', 'golden_column_count', 'llm_column_count', 'row_count_matched', 
    'col_count_matched', 'data_matched', 'cols_in_golden_not_in_llm', 'common_columns'
])

# Lock for thread-safe operations on results_df
lock = threading.Lock()

def create_dataframe_from_query(query, prompt_id):
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(query), connection)
            if df.empty or df.isnull().all().all():
                return pd.DataFrame(columns=df.columns)  # Return an empty DataFrame with the correct columns
            if df.shape == (1, 1) and df.iloc[0, 0] == 0:
                return pd.DataFrame(columns=df.columns)  # Return an empty DataFrame with the correct columns
            return df
    except Exception as e:
        print(f"SQL error occurred while processing query for {prompt_id}: {str(e)}. Skipping to next prompt.")
        return None

def round_float_columns(df, decimal_places=2):
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].round(decimal_places)
    return df

def add_hash_key(df, column_names, hash_column_name):
    df[hash_column_name] = df.apply(lambda row: hashlib.md5(str(row[column_names].values).encode()).hexdigest(), axis=1)
    return df

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

def fetch_prompt_ids(template_ids):
    query = """
    SELECT a.PROMPT_ID, a.LLM_SQL_QUERY, b.GOLDEN_SQL_QUERY, a.PROMPT_TEMPLATE_ID, a.ITERATION_NUM, a.SQL_EXECUTABLE
    FROM MASTER_USER_PROMPTS_ITERATIONS a
    JOIN PROMPTS_GOLDEN_SQL b
    ON a.PROMPT_TEMPLATE_ID = b.PROMPT_TEMPLATE_ID AND a.PROMPT_ID = b.PROMPT_ID AND a.PROMPT = b.PROMPT
    WHERE b.GOLDEN_SQL_QUERY IS NOT NULL and a.generated_date = current_date()
    """
    if template_ids:
        query += f" AND a.PROMPT_TEMPLATE_ID IN ({','.join(map(str, template_ids))})"

    with engine.connect() as connection:
        return connection.execute(text(query)).fetchall()

def sort_and_limit_query(query, common_columns, limit=100):
    # Remove existing LIMIT clause
    query = re.sub(r'\s+LIMIT\s+\d+', '', query, flags=re.IGNORECASE)
    
    # Remove all semicolons
    query = re.sub(r';', '', query)
    
    # Sort common columns in descending order
    if common_columns:
        common_columns.sort()
        columns = ", ".join(common_columns)
        query = f"SELECT {columns} FROM ({query}) ORDER BY {columns} LIMIT {limit}"
    
    return query

def process_prompt(row, batch_size=10):
    global results_df
    prompt_id, llm_sql_query, golden_sql_query, prompt_template_id, iter_num, sql_executable = row
    print(f"Prompt Started: {prompt_id}, Iteration Num: {iter_num}")
    try:
        llm_df = create_dataframe_from_query(llm_sql_query,prompt_id)
        golden_df = create_dataframe_from_query(golden_sql_query,prompt_id)

        llm_column_names = llm_df.columns.tolist() if llm_df is not None else []
        golden_column_names = golden_df.columns.tolist() if golden_df is not None else []

        cols_in_golden_not_in_llm = set(golden_column_names) - set(llm_column_names)
        llm_golden_common_cols = list(set(llm_column_names) & set(golden_column_names))

        if llm_df is not None and golden_df is not None:
            # Sort and limit the queries
            llm_sql_query = sort_and_limit_query(llm_sql_query, llm_golden_common_cols)
            golden_sql_query = sort_and_limit_query(golden_sql_query, llm_golden_common_cols)

            llm_df = create_dataframe_from_query(llm_sql_query,prompt_id)
            golden_df = create_dataframe_from_query(golden_sql_query,prompt_id)

            llm_df = round_float_columns(llm_df)
            golden_df = round_float_columns(golden_df)

            llm_df = add_hash_key(llm_df, llm_golden_common_cols, 'LLM_GOLDEN_HASH_VALUE')
            golden_df = add_hash_key(golden_df, llm_golden_common_cols, 'LLM_GOLDEN_HASH_VALUE')
        
        llm_row_count = llm_df.shape[0] if llm_df is not None else 0
        golden_row_count = golden_df.shape[0] if golden_df is not None else 0

        row_count_matched = llm_row_count == golden_row_count
        col_count_matched = len(llm_column_names) == len(golden_column_names)

        data_matched = False
        if row_count_matched and llm_df is not None and golden_df is not None:
            golden_hash_sorted = golden_df['LLM_GOLDEN_HASH_VALUE'].sort_values().reset_index(drop=True)
            llm_hash_sorted = llm_df['LLM_GOLDEN_HASH_VALUE'].sort_values().reset_index(drop=True)
            data_matched = golden_hash_sorted.equals(llm_hash_sorted)
        
        result_row = {
            'prompt_template_id': prompt_template_id,
            'prompt_id': prompt_id,
            'iter_num': iter_num,
            'run_date_time': run_date_time,
            'golden_sql': golden_sql_query,
            'llm_gen_sql': llm_sql_query,
            'llm_gen_sql_executable': sql_executable,
            'golden_cols': ",".join(golden_column_names),
            'llm_gen_cols': ",".join(llm_column_names),
            'golden_row_count': golden_row_count,
            'llm_row_count': llm_row_count,
            'golden_column_count': len(golden_column_names),
            'llm_column_count': len(llm_column_names),
            'row_count_matched': row_count_matched,
            'col_count_matched': col_count_matched,
            'data_matched': data_matched,
            'cols_in_golden_not_in_llm': ",".join(cols_in_golden_not_in_llm) if cols_in_golden_not_in_llm else None,
            'common_columns': ",".join(llm_golden_common_cols)
        }

    except Exception as e:
        print(f"SQL error occurred while processing prompt {prompt_id}, iteration {iter_num}: {str(e)}. Continuing with available data.")
        result_row = {
            'prompt_template_id': prompt_template_id,
            'prompt_id': prompt_id,
            'iter_num': iter_num,
            'run_date_time': run_date_time,
            'golden_sql': golden_sql_query,
            'llm_gen_sql': llm_sql_query,
            'llm_gen_sql_executable': sql_executable,
            'golden_cols': None,
            'llm_gen_cols': None,
            'golden_row_count': None,
            'llm_row_count': None,
            'golden_column_count': None,
            'llm_column_count': None,
            'row_count_matched': False,
            'col_count_matched': False,
            'data_matched': False,
            'cols_in_golden_not_in_llm': None,
            'common_columns': None
        }

    with lock:
        results_df = results_df._append(result_row, ignore_index=True)
        if len(results_df) >= batch_size:
            sorted_results_df = results_df.sort_values(by=['run_date_time', 'prompt_template_id', 'prompt_id', 'iter_num']).reset_index(drop=True)
            sorted_results_df.to_sql('final_output_iterations', engine, index=False, if_exists='append')
            results_df = pd.DataFrame(columns=results_df.columns)
    print(f"Prompt Completed: {prompt_id}, Iteration Num: {iter_num}")

def main():
    global results_df

    template_ids = parse_template_ids(selected_template_ids)
    prompt_rows = fetch_prompt_ids(template_ids)
    
    max_workers = min(30, os.cpu_count() * 2)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_prompt, row) for row in prompt_rows]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error occurred: {str(e)}")
    
    if not results_df.empty:
        sorted_results_df = results_df.sort_values(by=['run_date_time', 'prompt_template_id', 'prompt_id', 'iter_num']).reset_index(drop=True)
        sorted_results_df.to_sql('final_output_iterations', engine, index=False, if_exists='append')
    engine.dispose()

if __name__ == "__main__":
    main()
