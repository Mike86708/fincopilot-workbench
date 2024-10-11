import base64
import yaml, json
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError
import os
import requests


import dotenv



def read_yaml(file_path: str) -> dict:
    
    with open(file_path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            return None
        
    
        
    
    


def get_configuration(config_file: str) -> dict:
    # Read the default online configuration
    remote_config_file = os.environ.get('config_file')
    if 'sqlgen_local' in remote_config_file:
        if os.path.exists(config_file):
            with open(config_file, 'r') as stream:
                try:
                    settings = yaml.safe_load(stream)
                    return settings
                    
                except:
                    pass

    response = requests.get(remote_config_file)
    if response.status_code != 200:
        
        
        if os.path.exists(config_file):
            with open(config_file, 'r') as stream:
                try:
                    settings = yaml.safe_load(stream)
                    return settings
                    
                except:
                    pass

    response = json.loads(json.loads(response.text))

    config = response['message']

    # Do the necessary formatting
    config = config.replace('<cr>', '\n')
    config = config.replace('<CR>', '\n')
    config = yaml.safe_load(config)

    return config



SETTINGS = get_configuration('config.yml')
if os.path.exists('.env') and SETTINGS['app']['environment'] == 'local':
    dotenv.load_dotenv()


def create_new_filename(original_name: str, suffix: str = 'data', replaces: str = 'template') -> str:
    """
    Creates a new filename based on the original filename and current timestamp.

    Args:
        original_name (str): The original filename.
        suffix (str, optional): The suffix to add to the filename. Defaults to 'data'.
        replaces (str, optional): The string to replace in the original filename. Defaults to 'template'.

    Returns:
        str: The new filename in the format "originalname-YYYY-MM-DD-HHMMSS.xlsx".

    Raises:
        ValueError: If the original_name is empty or None.
    """
    try:
        # Check if original_name is empty or None
        if not original_name:
            raise ValueError("Original name cannot be empty or None")

        # Generate timestamp in the format "hh:mm_MM/DD/YYYY" (12h clock)
        timestamp = datetime.now().strftime('%I:%M%p_%m/%d/%Y')

        # Replace 'template' with 'data'
        base_name = original_name.replace(replaces, suffix)

        # Construct new filename
        new_name = f"{base_name}-{timestamp}"
        
        return new_name

    except Exception as e:
        logger.error(f"Error creating new filename: {e}")
        raise e


def create_download_link(filename):
    # Open the file in binary mode
    with open(filename, "rb") as f:
        data = f.read()

    # Encode the file data to base64
    b64 = base64.b64encode(data).decode()

    # Return a downloadable link for the original file
    return f'<a href="data:application/octet-stream;base64,{b64}" download="{filename}">Download file</a>'


def col_num_to_letter(n: int) -> str:
    """
    Convert a column number (1-indexed)
    to a column letter (A, B, C, ..., AA, AB, ...)

    @param n: The column number
    @return: The column letter
    """
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string


def get_secrets() -> dict:
    '''
    Get the secrets from AWS Secrets Manager

    @return: The secrets dictionary
    '''

    secret_name = SETTINGS['aws']['secrets']['name']
    region_name = SETTINGS['aws']['secrets']['region']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    if SETTINGS['app']['environment'] == 'local':
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name,
            aws_access_key_id=os.getenv('aws_access_key_id'),
            aws_secret_access_key=os.getenv('aws_secret_access_key')
        )
    elif SETTINGS['app']['environment'] == 'aws':
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])

        raise KeyError(f"Missing SecretString in AWS secrets: {get_secret_value_response}")
    except ClientError as e:
        logger.error(f"Error getting secrets from aws: {e}")
        raise e 
    


def create_logger(logfile: str):
    '''
    Create a custom logger with the given file and console levels.

    The logger levels are (from lowest to highest): 
    DEBUG, INFO, WARNING, ERROR, CRITICAL

    
    @param logfile (str): The path to the log file.
    @return logging.Logger: The created logger.
    '''

    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Set the logger level to the lowest level

    
    # Console can be set according to the settings
    run_mode = SETTINGS['app']['mode']

    if run_mode == 'dev':
        # Create a file handler
        f_handler = logging.FileHandler(logfile)
        f_handler.setLevel(logging.DEBUG)  # File handler is always the lowest level to catch everything

        # Create formatters and add them to the handlers
        f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')

        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        logger.addHandler(f_handler)


    # Create a console handler
    c_handler = logging.StreamHandler()
    
    
    if run_mode == 'dev':
        console_level = logging.DEBUG
    elif run_mode == 'prod':
        console_level = logging.CRITICAL
    c_handler.setLevel(console_level)

    # Create formatters and add them to the handlers
    c_format = logging.Formatter('%(levelname)s -  %(filename)s - %(message)s')

    c_handler.setFormatter(c_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)

    return logger



# Create the logger
logger = create_logger(SETTINGS['app']['log_file'])

# Get the secrets
SECRETS = get_secrets()
