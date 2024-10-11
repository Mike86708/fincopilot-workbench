import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from typing import List


# Connect to Snowflake
# Connection for the entire database
testing_conn = None


def connect_to_snowflake(): 
    global testing_conn

    conn = snowflake.connector.connect(
        user=DB_CONNECTION['user'],
        password=DB_CONNECTION['password'],
        account=DB_CONNECTION['account'],
        warehouse=DB_CONNECTION['warehouse'],
        database=DB_CONNECTION['database'],
        role=DB_CONNECTION['role']
    )

    testing_conn = conn


def disconnect_from_snowflake():
    global testing_conn
    
    testing_conn.close()


def write_data_to_database(db_name: str, data: pd.DataFrame):
    '''
    Write the data to the database.

    Parameters:
        db_name (str): The name of the database to write the data to. 
                    Should be in the form 
                    <database>.<schema>.<table>
        data (pd.DataFrame): The data to write to the database.
    '''

    
    db_components = db_name.split('.')

    if len(db_components) != 3:
        
        raise ValueError('Database name should be in the form <database>.<schema>.<table>')
        
    database, schema, table = db_components


    success, nchunks, nrows, _ = write_pandas(testing_conn, data, table_name=table, schema=schema, database=database)

    if success == False:
        print(f'Error writing data to {db_name}')


    print(f'Wrote {nrows} rows to {db_name}')

    return {
        'success': success,
        'nchunks': nchunks,
        'nrows': nrows
    }


def read_from_database(db_name: str, columns: List[str] = None ):
    '''
    Read the data from the database.

    Parameters:
        db_name (str): The name of the database to read the data from. 
                    Should be in the form 
                    <database>.<schema>.<table>
    '''
    
    db_components = db_name.split('.')

    if len(db_components) != 3:
        print(f'Invalid database name {db_name}')
        raise ValueError('Database name should be in the form <database>.<schema>.<table>')
        
    database, schema, table = db_components

    if columns is None:
        sql_statement = f'SELECT * FROM {db_name}'
    elif len(columns) != 0:
        sql_statement = f'SELECT {", ".join(columns)} FROM {db_name}'
    else:
        print(f'Invalid columns when reading from database: {columns}')

    
    with testing_conn.cursor() as cur:
        cur.execute(sql_statement)
        data = cur.fetch_pandas_all()

    return data


def execute_sql(sql_statement: str):
    # logger.debug(f'Executing SQL: {sql_statement}')
    with testing_conn.cursor() as cur:
        cur.execute(sql_statement)
        data = cur.fetch_pandas_all()
    
    return data

def check_executable(sql_statement: str):

    with testing_conn.cursor() as cur:
        try:
            cur.execute('EXPLAIN ' + sql_statement)
        except snowflake.connector.errors.Error as e:
            print(f'Error executing SQL: {sql_statement}')
            return False
    
    return True