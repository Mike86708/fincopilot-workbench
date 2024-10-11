import json
import re
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import boto3
import os
from lib.exception.exceptions import JournalFilterException
from lib.exception.exception_codes import Reason
from botocore.exceptions import ClientError
import snowflake.connector
from datetime import datetime

# For local testing
# from dotenv import load_dotenv

class Utils:

    @staticmethod
    def _generate_private_key(public_key: str) -> bytes:
        """
        Generate Private Key using the provided public key.
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
            raise JournalFilterException(
                "An error occurred while generating the private key",
                reason=Reason.INVALID_INPUT,
                subcomponent="generate_private_key",
            ) from e


    @staticmethod
    def load_env_variables():
        """Load and validate required environment variables."""
        try:
            # load_dotenv()

            region_name = os.getenv('REGION_NAME')
            secret_name = os.getenv('SECRET_NAME')
            user_name = os.getenv('USER_NAME')
            snowflake_user = os.getenv('SNOWFLAKE_USER')
            snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
            snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
            snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
            snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')

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
                raise JournalFilterException(
                    message=f"Missing environment variables: {', '.join(missing)}",
                    reason=Reason.CONFIG_ERROR,
                    subcomponent="load_env_variables"
                )

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

        except JournalFilterException:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise JournalFilterException(
                message="An unexpected error occurred while loading the environment variables.",
                reason=Reason.CONFIG_ERROR,
                subcomponent="load_env_variables",
                e=e
            )

    @staticmethod
    def connect_to_snowflake():
        """Establish a Snowflake connection using AWS Secrets Manager and environment variables."""
        config = Utils.load_env_variables()
        region_name = config['REGION_NAME']
        secret_name = config['SECRET_NAME']
        user_name = config['USER_NAME']
        snowflake_user = config['SNOWFLAKE_USER']
        snowflake_account = config['SNOWFLAKE_ACCOUNT']
        snowflake_warehouse = config['SNOWFLAKE_WAREHOUSE']
        snowflake_database = config['SNOWFLAKE_DATABASE']
        snowflake_schema = config['SNOWFLAKE_SCHEMA']

        try:
            session = boto3.Session()
            client = session.client(service_name='secretsmanager', region_name=region_name)
            response = client.get_secret_value(SecretId=secret_name)

            if 'SecretString' not in response:
                raise JournalFilterException(
                    message="SecretString not found in Secrets Manager response.",
                    reason=Reason.SECRETS_MANAGER_ERROR,
                    subcomponent="connect_to_snowflake"
                )
            key = response['SecretString']
            keys = json.loads(key)

            sf_access_key = keys.get(user_name)
            if sf_access_key is None:
                raise JournalFilterException(
                    message=f"Secret key for user '{user_name}' not found in Secrets Manager.",
                    reason=Reason.SECRETS_MANAGER_ERROR,
                    subcomponent="connect_to_snowflake"
                )

        except ClientError as e:
            raise JournalFilterException(
                message="Failed to retrieve secrets from AWS Secrets Manager.",
                reason=Reason.SECRETS_MANAGER_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )
        except json.JSONDecodeError:
            raise JournalFilterException(
                message="SecretString contains invalid JSON.",
                reason=Reason.SECRETS_MANAGER_ERROR,
                subcomponent="connect_to_snowflake"
            )
        except JournalFilterException:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise JournalFilterException(
                message="An unexpected error occurred while retrieving secrets.",
                reason=Reason.SECRETS_MANAGER_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )

        try:
            private_key_bytes = Utils._generate_private_key(sf_access_key)

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
            raise JournalFilterException(
                message="Failed to connect to Snowflake.",
                reason=Reason.DATABASE_CONNECTION_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )
        except JournalFilterException:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise JournalFilterException(
                message="An unexpected error occurred while connecting to Snowflake.",
                reason=Reason.DATABASE_CONNECTION_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )
    @staticmethod
    def get_filters():
        filters = {
            'subsidiary': [],
            'period': [],
            'journal_source': [],
            'is_reversal': [],
            'currency': [],
            'approval_status': [],
            'next_approver': []
        }
        conn = None
        cursor = None

        try:
            # Establish connection to Snowflake
            conn = Utils.connect_to_snowflake()
            cursor = conn.cursor()

            # Combine queries using UNION ALL and DISTINCT to remove duplicates
            combined_query = """
                SELECT DISTINCT 'subsidiary' AS filter_type, SUBSIDIARY FROM FINCOPILOT_CDM.WORKBENCH.JOURNAL
                UNION ALL
                SELECT DISTINCT 'period' AS filter_type, PERIOD FROM FINCOPILOT_CDM.WORKBENCH.JOURNAL
                UNION ALL
                SELECT DISTINCT 'journal_source' AS filter_type, JOURNAL_SOURCE FROM FINCOPILOT_CDM.WORKBENCH.JOURNAL
                UNION ALL
                SELECT DISTINCT 'is_reversal' AS filter_type, IS_REVERSAL FROM FINCOPILOT_CDM.WORKBENCH.JOURNAL
                UNION ALL
                SELECT DISTINCT 'currency' AS filter_type, CURRENCY FROM FINCOPILOT_CDM.WORKBENCH.JOURNAL
                UNION ALL
                SELECT DISTINCT 'approval_status' AS filter_type, APPROVAL_STATUS FROM FINCOPILOT_CDM.WORKBENCH.JOURNAL
                UNION ALL
                SELECT DISTINCT 'next_approver' AS filter_type, NEXT_APPROVER FROM FINCOPILOT_CDM.WORKBENCH.JOURNAL;
            """

            # Execute the combined query
            cursor.execute(combined_query)
            results = cursor.fetchall()

            # Process the result to populate filters dictionary
            for row in results:
                filter_type, value = row
                filters[filter_type].append(value)

        except JournalFilterException as jfe:
            raise jfe  # Re-raise custom exceptions for handling at a higher level
        except Exception as e:
            raise JournalFilterException("An error occurred while retrieving filters.") from e
        finally:
            # Ensure cursor and connection are always closed
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

        # Return the filters if successful
        return filters
