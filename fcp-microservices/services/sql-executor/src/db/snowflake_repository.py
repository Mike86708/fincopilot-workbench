import base64
import requests
import snowflake.connector
from snowflake.connector import errors
import logging
import time
import boto3
import json
from .database_repository import DatabaseRepository
from util.utils import retrieve_secret

from exceptions.exception import SQLExecutionException
from exceptions.exception_codes import Reason

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class SnowflakeRepository(DatabaseRepository):
    """
    SnowflakeRepository implements the DatabaseRepository interface for Snowflake database operations.
    """

    def __init__(self, config):
        """
        Initialize SnowflakeRepository with configuration data.

        Args:
        - config (dict): Dictionary containing Snowflake configuration data.
        """
        # self.client_id = config["client_id"]
        # self.client_secret = config["client_secret"]
        self.user_name = config["user_name"]
        # self.token_endpoint = config["token_endpoint"]
        # self.scope = config["scope"]
        self.password = None#config["password"]
        self.snowflake_account = config["snowflake_account"]
        self.snowflake_database = config["snowflake_database"]
        self.snowflake_schema = config["snowflake_schema"]
        self.snowflake_warehouse = config["snowflake_warehouse"]
        self.query_tag = config["query_tag"]
        # self.secret_name = config["secret_name"]
        self.service_secret_name = config["service_secret_name"]
       

        self.conn = None

        # AWS Secrets Manager client
        self.session = boto3.session.Session()
        self.secrets_client = self.session.client(service_name='secretsmanager')
        
    
    def establish_connection_service_account(self,query_tag): 
    # WITH SERVICE ACCOUNT PRIVATE KEY
        """
        Establishes connection to Snowflake using stored credentials for service account.

        Raises:
        - RuntimeError: If there is an error establishing the Snowflake connection.
        """
        try:
            
            
            # Retrieve secret
            secret, status_code = retrieve_secret(self.service_secret_name)
            
            if status_code!=200:
                raise SQLExecutionException(message="Error retrieving secret.", reason=Reason.SECRETS_MANAGER_ERROR, field_errors=str(secret))
            
            if secret:
                # Access individual secret values
                # self.user_name = secret.get('user_name')
                # self.password = secret.get('password') 
                sf_access_key = secret.get('fincopilot_user')
                sf_access_key="-----BEGIN PRIVATE KEY-----\n"+sf_access_key+"\n-----END PRIVATE KEY-----"
                private_key= serialization.load_pem_private_key(
                    sf_access_key.encode('utf-8'),
                    password=None,
                    backend=default_backend()
                )

                private_key_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                 
                logger.info("Successfully connected to secrets")
                
            else:
                logger.info("Failed to retrieve the secret.")
            
            # without token
            self.conn = snowflake.connector.connect(
            user=self.user_name,
            account=self.snowflake_account,
            private_key=private_key_bytes,
            # session_parameters={'QUERY_TAG': self.query_tag}
            session_parameters={'QUERY_TAG': query_tag}
            )
            logger.info("Connected to Snowflake.")
        except SQLExecutionException as e : 
            raise SQLExecutionException(message="Error retrieving secret.", reason=Reason.SECRETS_MANAGER_ERROR, field_errors=str(secret))
        except errors.DatabaseError as e:
            logger.error(f"Connection error: {e}")
            # raise RuntimeError("Connection error to Snowflake database")
            raise SQLExecutionException(message="Connection error to Snowflake database.", reason=Reason.DATABASE_CONNECTION_FAILED, field_errors=str(e))
        except errors.Error as e:
            logger.error(f"Error establishing Snowflake connection: {e}")
            # raise RuntimeError("Error establishing Snowflake connection")
            raise SQLExecutionException(message="Error establishing Snowflake connection.", reason=Reason.DATABASE_CONNECTION_FAILED, field_errors=str(e))
        except SQLExecutionException as e:
                logging.error(f"Unexpected error: {e}")
                # raise Exception(f"Unexpected error during query execution {e}")
                raise SQLExecutionException(e.message, e.reason, e=e) 
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            # raise Exception("Unexpected error during connection")    
            raise SQLExecutionException(message="Unexpected error during connection.", reason=Reason.UNKNOWN_ERROR, field_errors=str(e))
 
    
          
    def execute_query_service_account(self, sql_query,query_tag):
        """
        Executes SQL query on Snowflake and retrieves column names and rows.

        Args:
        - sql_query (str): SQL query to execute.

        Returns:
        - tuple: A tuple containing columns and rows.

        Raises:
        - ValueError: If the SQL query is invalid.
        - RuntimeError: If there is an error executing the query.
        """
        try:
            if not self.conn:
                self.establish_connection_service_account(query_tag) 
                # self.establish_connection(oauth_token,role):#with okta api

            cursor = self.conn.cursor()
            cursor.execute(sql_query)
            
            # Retrieve the query ID
            query_id = cursor.sfqid

            # Print or log the query ID
            logging.info(f"Query ID: {query_id}")

            # Fetch column names
            columns = [col[0] for col in cursor.description]

            # Fetch all rows
            rows = cursor.fetchall()
            cursor.close()
            logging.info(f"Executed query: {sql_query}")
            return columns, rows,query_id
        except errors.ProgrammingError as e:
            logging.error(f"Invalid SQL query: {e}")
            # raise ValueError("Invalid SQL query")
            raise SQLExecutionException(message="Invalid SQL query.", reason=Reason.INVALID_SQL_QUERY, field_errors=str(e))
        except errors.Error as e:
            logging.error(f"Error executing query: {e}")
            # raise RuntimeError("Error executing query")
            raise SQLExecutionException(message="Error executing query.", reason=Reason.SQL_EXECUTION_FAILED, field_errors=str(e))
        except SQLExecutionException as e:
                logging.error(f"Unexpected error: {e}")
                # raise Exception(f"Unexpected error during query execution {e}")
                raise SQLExecutionException(e.message, e.reason, e=e) 
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            # raise Exception("Unexpected error during query execution") 
            raise SQLExecutionException(message="Unexpected error during query execution.", reason=Reason.UNKNOWN_ERROR, field_errors=str(e)) from e     
        
    def establish_connection_okta(self,oauth_token,role,user_email,query_tag):#with okta api
            """
            Establishes connection to Snowflake using stored credentials.

            Raises:
            - RuntimeError: If there is an error establishing the Snowflake connection.
            """
            try:
                # print('role->',role)
                # to be used with api (okta) #with okta api
                self.conn = snowflake.connector.connect(
                user=user_email,# user_email is user name
                account=self.snowflake_account,
                authenticator="oauth",
                token=oauth_token,
                warehouse=self.snowflake_warehouse,
                database=self.snowflake_database,
                schema=self.snowflake_schema,
                role=role,
                session_parameters={'QUERY_TAG': query_tag})
                
                
                logger.info("Connected to Snowflake.")
            except errors.DatabaseError as e:
                logger.error(f"Connection error: {e}")
                # raise RuntimeError(f"Connection error to Snowflake database {e}")
                raise SQLExecutionException(message="Connection error to Snowflake database.", reason=Reason.DATABASE_CONNECTION_FAILED, field_errors=str(e))
            except errors.Error as e:
                logger.error(f"Error establishing Snowflake connection: {e}")
                # raise RuntimeError("Error establishing Snowflake connection")
                raise SQLExecutionException(message="Error establishing Snowflake connection.", reason=Reason.DATABASE_CONNECTION_FAILED, field_errors=str(e))
            except SQLExecutionException as e:
                logging.error(f"Unexpected error: {e}")
                # raise Exception(f"Unexpected error during query execution {e}")
                raise SQLExecutionException(e.message, e.reason, e=e) 
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                # raise Exception(f"Unexpected error during connection.")
                raise SQLExecutionException(message="Unexpected error during connection.", reason=Reason.UNKNOWN_ERROR, field_errors=str(e))

    def execute_query_okta(self, sql_query,oauth_token,role,user_email,query_tag):
            """
            Executes SQL query on Snowflake and retrieves column names and rows.

            Args:
            - sql_query (str): SQL query to execute.

            Returns:
            - tuple: A tuple containing columns and rows.

            Raises:
            - ValueError: If the SQL query is invalid.
            - RuntimeError: If there is an error executing the query.
            """
            try:
                if not self.conn:
                    self.establish_connection_okta(oauth_token,role,user_email,query_tag)#with okta api

                cursor = self.conn.cursor()
                cursor.execute(sql_query)
                
                # Retrieve the query ID
                query_id = cursor.sfqid

                # Print or log the query ID
                logging.info(f"Query ID: {query_id}")

                # Fetch column names
                columns = [col[0] for col in cursor.description]

                # Fetch all rows
                rows = cursor.fetchall()

                cursor.close()

                logging.info(f"Executed query: {sql_query}")

                return columns, rows,query_id

            except errors.ProgrammingError as e:
                logging.error(f"Invalid SQL query: {e}")
                # raise ValueError("Invalid SQL query")
                raise SQLExecutionException(message="Invalid SQL query.", reason=Reason.INVALID_SQL_QUERY, field_errors=str(e))
            except errors.Error as e:
                logging.error(f"Error executing query: {e}")
                # raise RuntimeError("Error executing query")
                raise SQLExecutionException(message="Error executing query.", reason=Reason.SQL_EXECUTION_FAILED, field_errors=str(e))
            except SQLExecutionException as e:
                logging.error(f"Unexpected error: {e}")
                # raise Exception(f"Unexpected error during query execution {e}")
                raise SQLExecutionException(e.message, e.reason, e=e) 
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                # raise Exception(f"Unexpected error during query execution {e}")
                raise SQLExecutionException(message="Unexpected error during query execution.", reason=Reason.UNKNOWN_ERROR, field_errors=str(e))

    def close_connection(self):
        """
        Closes the Snowflake connection.

        Raises:
        - RuntimeError: If there is an error closing the Snowflake connection.
        """
        try:
            if self.conn:
                self.conn.close()
                self.conn = None
                logging.info("Snowflake connection closed.")
        except errors.Error as e:
            logging.error(f"Error closing Snowflake connection: {e}")
            # raise RuntimeError("Error closing Snowflake connection")
            raise SQLExecutionException(message="Error closing Snowflake connection.", reason=Reason.DATABASE_CONNECTION_FAILED, field_errors=str(e))
        except SQLExecutionException as e:
                logging.error(f"Unexpected error: {e}")
                # raise Exception(f"Unexpected error during query execution {e}")
                raise SQLExecutionException(e.message, e.reason, e=e) 
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            # raise RuntimeError("Unexpected error during connection closing")
            raise SQLExecutionException(message="Unexpected error during connection closing.", reason=Reason.UNKNOWN_ERROR, field_errors=str(e))

#--------------------------------- 
    def execute_query(self, sql_query):
        """
        Executes SQL query on Snowflake and retrieves column names and rows.

        Args:
        - sql_query (str): SQL query to execute.

        Returns:
        - tuple: A tuple containing columns and rows.

        Raises:
        - ValueError: If the SQL query is invalid.
        - RuntimeError: If there is an error executing the query.
        """
        try:
            if not self.conn:
                self.establish_connection() 
                # self.establish_connection(oauth_token,role):#with okta api

            cursor = self.conn.cursor()
            cursor.execute(sql_query)

            # Fetch column names
            columns = [col[0] for col in cursor.description]

            # Fetch all rows
            rows = cursor.fetchall()

            cursor.close()

            logging.info(f"Executed query: {sql_query}")

            return columns, rows

        except errors.ProgrammingError as e:
            logging.error(f"Invalid SQL query: {e}")
            # raise ValueError("Invalid SQL query")
            raise SQLExecutionException(message="Invalid SQL query.", reason=Reason.INVALID_SQL_QUERY, field_errors=str(e))
        except errors.Error as e:
            logging.error(f"Error executing query: {e}")
            # raise RuntimeError("Error executing query")
            raise SQLExecutionException(message="Error executing query.", reason=Reason.SQL_EXECUTION_FAILED, field_errors=str(e))
        except SQLExecutionException as e:
                logging.error(f"Unexpected error: {e}")
                # raise Exception(f"Unexpected error during query execution {e}")
                raise SQLExecutionException(e.message, e.reason, e=e) 
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            # raise Exception("Unexpected error during query execution")
            raise SQLExecutionException(message="Unexpected error during query execution.", reason=Reason.UNKNOWN_ERROR, field_errors=str(e))
            
        
        
    def establish_connection(self): 
    # def establish_connection(self,oauth_token,role):#with okta api
        """
        Establishes connection to Snowflake using stored credentials.

        Raises:
        - RuntimeError: If there is an error establishing the Snowflake connection.
        """
        try:
            # # to be used with api (okta) #with okta api
            # self.conn = snowflake.connector.connect(
            # user=self.user_name,# user_email is user name
            # account=self.snowflake_account,
            # authenticator="oauth",
            # token=oauth_token,
            # warehouse=self.snowflake_warehouse,
            # database=self.snowflake_database,
            # schema=self.snowflake_schema,
            # role=role)
            
            
            # without token
            self.conn = snowflake.connector.connect(
            user=self.user_name,
            account=self.snowflake_account,
            # authenticator="oauth",
            password=self.password,
            warehouse=self.snowflake_warehouse,
            database=self.snowflake_database,
            schema=self.snowflake_schema,
            session_parameters={'QUERY_TAG': self.query_tag}
            )
            
            ## With token (generated by the code itself using client id and secrete)
            # access_token = self.get_valid_token()

            # self.conn = snowflake.connector.connect(
            #     user=self.user_name,
            #     account=self.snowflake_account,
            #     authenticator="oauth",
            #     token=access_token,
            #     warehouse=self.snowflake_warehouse,
            #     database=self.snowflake_database,
            #     schema=self.snowflake_schema,
            #     session_parameters={'QUERY_TAG': self.query_tag}
            # )
            logger.info("Connected to Snowflake.")
        except errors.DatabaseError as e:
            logger.error(f"Connection error: {e}")
            # raise RuntimeError("Connection error to Snowflake database")
            raise SQLExecutionException(message="Connection error to Snowflake database.", reason=Reason.DATABASE_CONNECTION_FAILED, field_errors=str(e))
        except errors.Error as e:
            logger.error(f"Error establishing Snowflake connection: {e}")
            # raise RuntimeError("Error establishing Snowflake connection")
            raise SQLExecutionException(message="Error establishing Snowflake connection.", reason=Reason.DATABASE_CONNECTION_FAILED, field_errors=str(e))
        except SQLExecutionException as e:
                logging.error(f"Unexpected error: {e}")
                # raise Exception(f"Unexpected error during query execution {e}")
                raise SQLExecutionException(e.message, e.reason, e=e) 
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            # raise RuntimeError("Unexpected error during connection")  
            raise SQLExecutionException(message="Unexpected error during connection.", reason=Reason.UNKNOWN_ERROR, field_errors=str(e))   