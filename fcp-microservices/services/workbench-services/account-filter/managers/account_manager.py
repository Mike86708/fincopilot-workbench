from abc import ABC, abstractmethod
import os
import boto3
from botocore.exceptions import ClientError
import snowflake.connector
import json
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from lib.exception.exceptions import FincopilotException
from lib.exception.exception_codes import Reason
from dotenv import load_dotenv

class AccountManager(ABC):
    def __init__(self, exception_class, **kwargs):
        """
        Base constructor for AccountManager.
        
        Args:
            exception_class (Exception): The specific exception class to use for error handling.
            **kwargs: Dynamic parameters required by child classes (e.g., subsidiary_id, period_id).
        """
        self.params = kwargs
        self.conn = None
        self.exception_class = exception_class  # Exception class specific to the child class

    def load_env_variables(self):
        """Load and validate required environment variables."""
        try:
            load_dotenv()
            
            # Access the environment variables
            region_name = os.getenv('REGION_NAME')
            secret_name = os.getenv('SECRET_NAME')
            user_name = os.getenv('USER_NAME')
            snowflake_user = os.getenv('SNOWFLAKE_USER')
            snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
            snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
            snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
            snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
            
            # Check if any required variables are missing
            required_vars = [
                region_name, secret_name, user_name, snowflake_user,
                snowflake_account, snowflake_warehouse, snowflake_database, snowflake_schema
            ]
            if not all(required_vars):
                missing = [var for var, val in zip(
                    ['REGION_NAME', 'SECRET_NAME', 'USER_NAME', 'SNOWFLAKE_USER',
                     'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA'],
                    required_vars
                ) if not val]
                raise self.exception_class(
                    message=f"Missing environment variables: {', '.join(missing)}",
                    reason=Reason.CONFIG_ERROR,
                    subcomponent="load_env_variables"
                )

            # Create a configuration dictionary
            config = {
                'REGION_NAME': region_name,
                'SECRET_NAME': secret_name,
                'USER_NAME': user_name,
                'SNOWFLAKE_USER': snowflake_user,
                'SNOWFLAKE_ACCOUNT': snowflake_account,
                'SNOWFLAKE_WAREHOUSE': snowflake_warehouse,
                'SNOWFLAKE_DATABASE': snowflake_database,
                'SNOWFLAKE_SCHEMA': snowflake_schema
            }
            
            return config
        except self.exception_class:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise self.exception_class(
                message="An unexpected error occurred while loading the environment variables.",
                reason=Reason.CONFIG_ERROR,
                subcomponent="load_env_variables",
                e=e
            )

    def connect_to_snowflake(self):
        """Establish a Snowflake connection using AWS Secrets Manager and environment variables."""
        config = self.load_env_variables()
        region_name = config['REGION_NAME']
        secret_name = config['SECRET_NAME']
        user_name = config['USER_NAME']
        snowflake_user = config['SNOWFLAKE_USER']
        snowflake_account = config['SNOWFLAKE_ACCOUNT']
        snowflake_warehouse = config['SNOWFLAKE_WAREHOUSE']
        snowflake_database = config['SNOWFLAKE_DATABASE']
        snowflake_schema = config['SNOWFLAKE_SCHEMA']

        try:
            # Create a Secrets Manager client
            session = boto3.Session()
            client = session.client(service_name='secretsmanager', region_name=region_name)

            # Retrieve the secrets details from AWS
            response = client.get_secret_value(SecretId=secret_name)

            # Extract the secret key
            if 'SecretString' not in response:
                raise self.exception_class(
                    message="SecretString not found in Secrets Manager response.",
                    reason=Reason.SECRETS_MANAGER_ERROR,
                    subcomponent="connect_to_snowflake"
                )
            key = response['SecretString']
            keys = json.loads(key)

            sf_access_key = keys.get(user_name)
            if sf_access_key is None:
                raise self.exception_class(
                    message=f"Secret key for user '{user_name}' not found in Secrets Manager.",
                    reason=Reason.SECRETS_MANAGER_ERROR,
                    subcomponent="connect_to_snowflake"
                )

        except ClientError as e:
            raise self.exception_class(
                message="Failed to retrieve secrets from AWS Secrets Manager.",
                reason=Reason.SECRETS_MANAGER_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )
        except json.JSONDecodeError:
            raise self.exception_class(
                message="SecretString contains invalid JSON.",
                reason=Reason.SECRETS_MANAGER_ERROR,
                subcomponent="connect_to_snowflake"
            )
        except self.exception_class:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise self.exception_class(
                message="An unexpected error occurred while retrieving secrets.",
                reason=Reason.SECRETS_MANAGER_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )

        try:
            # Generate the private key using the helper method
            private_key_bytes = self._generate_private_key(sf_access_key)

            # Connect to Snowflake
            conn = snowflake.connector.connect(
                user=snowflake_user,
                account=snowflake_account,
                private_key=private_key_bytes,
                warehouse=snowflake_warehouse,
                database=snowflake_database,
                schema=snowflake_schema
            )

            return conn

        except snowflake.connector.errors.DatabaseError as e:
            raise self.exception_class(
                message="Failed to connect to Snowflake.",
                reason=Reason.DATABASE_CONNECTION_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )
        except self.exception_class:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise self.exception_class(
                message="An unexpected error occurred while connecting to Snowflake.",
                reason=Reason.DATABASE_CONNECTION_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )

    def _generate_private_key(self, public_key: str) -> bytes:
        """
        Generate Private Key using the provided public key.

        Args:
            public_key (str): Public Key

        Returns:
            bytes: Private Key in Bytes
        """
        try:
            access_key = (
                "-----BEGIN PRIVATE KEY-----\n" + public_key + "\n-----END PRIVATE KEY-----"
            )

            private_key = serialization.load_pem_private_key(
                access_key.encode("utf-8"), password=None, backend=default_backend()
            )

            private_key_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            return private_key_bytes
        except Exception as e:
            raise self.exception_class(
                "An error occurred while generating the private key",
                reason=Reason.INVALID_INPUT,
                subcomponent="generate_private_key",
            ) from e

    @abstractmethod
    def get_sql_file_name(self) -> str:
        """Provide the SQL file name for the specific manager."""
        pass

    def execute_query(self):
        """
        Execute the SQL query using parameters provided in the child class (e.g., subsidiary_id, period_id).
        """
        try:
            # Get the SQL file name from the child class
            sql_file_name = self.get_sql_file_name()

            # Load the SQL query from the file
            with open(sql_file_name, 'r') as file:
                sql_query = file.read()

            # Ensure a Snowflake connection is established
            if self.conn is None:
                self.conn = self.connect_to_snowflake()

            # Create a cursor and execute the query
            cursor = self.conn.cursor()

            # The parameters for SQL execution are dynamically injected
            cursor.execute(sql_query, self.params)

            # Fetch the result
            return cursor.fetchall()

        except FileNotFoundError as e:
            raise self.exception_class(
                message=f"SQL file '{sql_file_name}' not found",
                reason=Reason.SQL_FILE_NOT_FOUND,
                subcomponent="execute_query"
            ) from e
        except self.exception_class:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise self.exception_class(
                message="An unexpected error occurred while executing the SQL query.",
                reason=Reason.DATABASE_EXECUTION_ERROR,
                subcomponent="execute_query",
                e=e
            )

    @abstractmethod
    def process_data(self, data):
        """
        Abstract method for processing the query result. The child class will define 
        how to transform the data into a useful structure.
        """
        pass

    def get_result(self):
        """Wrapper to fetch the query result and process it using the child class' logic."""
        try:
            # Execute the query and get the raw data
            raw_data = self.execute_query()

            # Process the data using the child class' logic
            return self.process_data(raw_data)

        finally:
            # Close the connection after the task is completed
            if self.conn:
                self.conn.close()
