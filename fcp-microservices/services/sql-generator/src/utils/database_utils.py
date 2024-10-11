import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from src.utils.main import SETTINGS, logger
from src.utils.test import TEST_SETTINGS
from typing import List

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

CONNECTION_NAME = 'db_connection' 
DB_CONNECTION = TEST_SETTINGS[CONNECTION_NAME]


# Connect to Snowflake
# Connection for the entire database
testing_conn = None


def set_connection_name(connection_name: str):
    global CONNECTION_NAME, DB_CONNECTION
    CONNECTION_NAME = connection_name
    DB_CONNECTION = TEST_SETTINGS[CONNECTION_NAME]

def connect_to_snowflake(): 
    global testing_conn
    if 'password' in DB_CONNECTION:
        conn = snowflake.connector.connect(
            user=DB_CONNECTION['user'],
            password=DB_CONNECTION['password'],
            account=DB_CONNECTION['account'],
            warehouse=DB_CONNECTION['warehouse'],
            database=DB_CONNECTION['database'],
            role=DB_CONNECTION['role']
        )
    elif 'key_path' in DB_CONNECTION:
        with open(DB_CONNECTION['key_path'], "rb") as key_file:

            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        
        conn = snowflake.connector.connect(
            user=DB_CONNECTION['user'],
            private_key=p_key,
            account=DB_CONNECTION['account'],
            warehouse=DB_CONNECTION['warehouse'],
            database=DB_CONNECTION['database'],
            schema=DB_CONNECTION['schema'],
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

    logger.debug(f'Writing data to {db_name}')
    db_components = db_name.split('.')

    if len(db_components) != 3:
        logger.error(f'Invalid database name {db_name}')
        raise ValueError('Database name should be in the form <database>.<schema>.<table>')
        
    database, schema, table = db_components


    success, nchunks, nrows, _ = write_pandas(testing_conn, data, table_name=table, schema=schema, database=database)

    if success == False:
        logger.error(f'Error writing data to {db_name}')


    logger.info(f'Wrote {nrows} rows to {db_name}')

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
    logger.debug(f'Reading from {db_name}')
    db_components = db_name.split('.')

    if len(db_components) != 3:
        logger.error(f'Invalid database name {db_name}')
        raise ValueError('Database name should be in the form <database>.<schema>.<table>')
        
    database, schema, table = db_components

    if columns is None:
        sql_statement = f'SELECT * FROM {db_name}'
    elif len(columns) != 0:
        sql_statement = f'SELECT {", ".join(columns)} FROM {db_name}'
    else:
        logger.exception(f'Invalid columns when reading from database: {columns}')

    
    with testing_conn.cursor() as cur:
        cur.execute(sql_statement)
        data = cur.fetch_pandas_all()

    return data


def execute_sql(sql_statement: str):
    '''
    Execute the SQL statement
    Only for the pre-validated SQL statements

    Parameters:
        sql_statement (str): The SQL statement to execute
    
    '''
    with testing_conn.cursor() as cur:
        cur.execute(sql_statement)
        data = cur.fetch_pandas_all()
    
    return data

def check_executable(sql_statement: str):

    with testing_conn.cursor() as cur:
        try:
            cur.execute('EXPLAIN ' + sql_statement)
        except snowflake.connector.errors.Error as e:
            logger.error(f'Error executing SQL: {sql_statement}')
            return False
    
    return True