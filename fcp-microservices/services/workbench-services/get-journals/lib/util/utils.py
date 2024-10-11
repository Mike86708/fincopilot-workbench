import json
import re
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import boto3
import os
from lib.exception.exceptions import GetJournalsException
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
            raise GetJournalsException(
                "An error occurred while generating the private key",
                reason=Reason.INVALID_INPUT,
                subcomponent="generate_private_key",
            ) from e

    @staticmethod
    def format_float(value):
        """Format floating-point values as floats rather than in scientific notation."""
        try:
            if value is None:
                return 0
            return float(f"{value:.2f}")
        except Exception as e:
            raise GetJournalsException(
                "Error formatting float value.",
                reason=Reason.INVALID_INPUT,
                subcomponent="format_float"
            ) from e

    @staticmethod
    def build_query(filters, search_string=None):
        """
        Builds a SQL query based on the provided filters and search string.

        Args:
            filters (dict): The dictionary of filters.
            search_string (str): The search string to match across all columns.

        Returns:
            str: The complete SQL query string.
        """
        # Base query
        query = "SELECT * FROM fincopilot_cdm.workbench.journal WHERE 1=1"
        
        # List to hold filter conditions
        conditions = []

        # Optional filters
        if 'Subsidiary' in filters and filters['Subsidiary']:
            conditions.append(f"Subsidiary = '{filters['Subsidiary']}'")

        if 'Period' in filters and filters['Period']:
            conditions.append(f"Period = '{filters['Period']}'")

        if 'Creation Date' in filters and filters['Creation Date']:
            creation_date = filters['Creation Date']
            # Assuming 'creation_date' is in 'YYYY-MM-DD' format, create start and end range
            start_date_str = f"{creation_date} 00:00:00"
            end_date_str = f"{creation_date} 23:59:59"
            # Add range condition to cover the full day
            conditions.append(f"creation_date BETWEEN '{start_date_str}' AND '{end_date_str}'")

        if 'Journal Source' in filters and filters['Journal Source']:
            conditions.append(f"Journal_Source = '{filters['Journal Source']}'")

        if 'Is Reversal' in filters and filters['Is Reversal']:
            conditions.append(f"Is_Reversal = '{filters['Is Reversal']}'")

        # if 'Created By' in filters and filters['Created By']:
        #     conditions.append(f"Created_By = '{filters['Created By']}'")

        if 'Currency' in filters and filters['Currency']:
            conditions.append(f"Currency = '{filters['Currency']}'")

        if 'Approval Status' in filters and filters['Approval Status']:
            conditions.append(f"Approval_Status = '{filters['Approval Status']}'")

        if 'Next Approver' in filters and filters['Next Approver']:
            conditions.append(f"Next_Approver = '{filters['Next Approver']}'")

        # If a search string is provided, search across all relevant columns
        # if search_string:
        #     search_conditions = []
        #     search_columns = ['Subsidiary', 'Period', 'Document_Number', 'MEMO', 'Journal_Source','Created_By', 'Next_Approver']
        #     for col in search_columns:
        #         search_conditions.append(f"{col} LIKE '%{search_string}%'")
        #     # Combine search conditions with OR (search across all columns)
        #     search_query = " OR ".join(search_conditions)
        #     conditions.append(f"({search_query})")

        # Join the conditions with AND and add to the query
        if conditions:
            query += " AND " + " AND ".join(conditions)

        return query


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
                raise GetJournalsException(
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

        except GetJournalsException:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise GetJournalsException(
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
                raise GetJournalsException(
                    message="SecretString not found in Secrets Manager response.",
                    reason=Reason.SECRETS_MANAGER_ERROR,
                    subcomponent="connect_to_snowflake"
                )
            key = response['SecretString']
            keys = json.loads(key)

            sf_access_key = keys.get(user_name)
            if sf_access_key is None:
                raise GetJournalsException(
                    message=f"Secret key for user '{user_name}' not found in Secrets Manager.",
                    reason=Reason.SECRETS_MANAGER_ERROR,
                    subcomponent="connect_to_snowflake"
                )

        except ClientError as e:
            raise GetJournalsException(
                message="Failed to retrieve secrets from AWS Secrets Manager.",
                reason=Reason.SECRETS_MANAGER_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )
        except json.JSONDecodeError:
            raise GetJournalsException(
                message="SecretString contains invalid JSON.",
                reason=Reason.SECRETS_MANAGER_ERROR,
                subcomponent="connect_to_snowflake"
            )
        except GetJournalsException:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise GetJournalsException(
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
            raise GetJournalsException(
                message="Failed to connect to Snowflake.",
                reason=Reason.DATABASE_CONNECTION_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )
        except GetJournalsException:
            raise  # Re-raise known exceptions
        except Exception as e:
            raise GetJournalsException(
                message="An unexpected error occurred while connecting to Snowflake.",
                reason=Reason.DATABASE_CONNECTION_ERROR,
                subcomponent="connect_to_snowflake",
                e=e
            )

    @staticmethod
    def process_result(data):
        """
        Process the raw data from the accounting activity query.
        """
        try:
            result = []
            for row in data:
                processed_row = {
                    "Period": row[0],
                    "Subsidiary": row[1],
                    "Document_Number": row[2],
                    "MEMO": row[3],
                    "JE_ID": row[4],
                    "Currency": row[5],
                    "Total Debit": row[6],
                    "Total Credit": row[7],
                    "Approval Status": row[8],
                    "Exchange Rate": row[9],
                    "Journal Source": row[10],
                    "Is Reversal": row[11],
                    "Created By": row[12],
                    "Next Approver": row[13],
                    "Creation Date": row[14].strftime('%b %d, %Y') if row[14] else None,
                    "Last Update Date": row[15].strftime('%b %d, %Y') if row[15] else None
                }
                result.append({k: v for k, v in processed_row.items() if v is not None})

            return result

        except Exception as e:
            raise GetJournalsException(
                "Error processing result data.",
                reason=Reason.DATA_PROCESSING_ERROR,
                subcomponent="process_result",
                e=e
            )

    @staticmethod
    def get_filters(event):
        """
        Extract and process filters from the event for SQL query usage.
        """
        try:
            filters = {}
            # Define allowed keys and their processing functions
            allowed_keys = {
                'Subsidiary': lambda v: v,
                'Period': lambda v: v,
                'Creation Date': lambda v: Utils.convert_date_for_sql(v),
                'Journal Source': lambda v: v,
                'Is Reversal': lambda v: v,
                'Currency': lambda v: v,
                'Approval Status': lambda v: v,
                'Next Approver': lambda v: v,
            }

            # Process each key using its corresponding function
            for key, process_fn in allowed_keys.items():
                value = event.get(key)
                if value:
                    processed_value = process_fn(value)
                    if processed_value:
                        filters[key] = processed_value

            return filters

        except GetJournalsException:
            raise  # Re-raise known exceptions

        except Exception as e:
            raise GetJournalsException(
                "Error extracting filters from the event.",
                reason=Reason.INVALID_INPUT,
                subcomponent="get_filters",
                e=e
            )

    @staticmethod
    def convert_date_for_sql(date_str):
        """
        Converts a date string from 'MMM DD, YYYY' format to 'YYYY-MM-DD'.
        """
        try:
            date_obj = datetime.strptime(date_str, '%b %d, %Y')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError as e:
            raise GetJournalsException(
                message=f"Error converting date: {e}",
                reason=Reason.INVALID_INPUT,
                subcomponent="convert_date_for_sql",
                e=e
            )
