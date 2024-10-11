import os
import boto3
import json
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from snowflake.sqlalchemy import URL
from urllib.parse import quote_plus
from workbench_exceptions import FincopilotException
from workbench_exception_codes import Reason
from utils import Utils
from botocore.exceptions import ClientError


# Function to create the Snowflake engine
def get_snowflake_engine() -> Engine:
    try:
        secret = get_secret()
        secret_dict = json.loads(secret)

        sf_user_name = Utils.GetEnvironmentVariableValue("SNOW_FLAKE_USER_KEY")
        private_key = Utils.generate_private_key(secret_dict.get(sf_user_name))
        
        user = Utils.GetEnvironmentVariableValue("SNOW_FLAKE_USER")
        account = Utils.GetEnvironmentVariableValue("SNOW_FLAKE_ACCOUNT")
        database = Utils.GetEnvironmentVariableValue("SNOW_FLAKE_DATABASE")
        schema = Utils.GetEnvironmentVariableValue("SNOW_FLAKE_SCHEMA")
        warehouse = Utils.GetEnvironmentVariableValue("SNOW_FLAKE_WAREHOUSE")
        role = Utils.GetEnvironmentVariableValue("SNOW_FLAKE_ROLE")

        if not account or not database or not schema or not warehouse or not role:
            raise FincopilotException(
                "Required environment variables are not set; SNOW_FLAKE_ACCOUNT or SNOW_FLAKE_DATABASE or SNOW_FLAKE_SCHEMA or SNOW_FLAKE_WAREHOUSE or SNOW_FLAKE_ROLE",
                reason=Reason.ENV_VARIABLE_MISSING,
                subcomponent="get_snowflake_engine",
            )

        engine = create_engine(
            URL(
                user=user,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role=role,
            ),
            connect_args={
                "private_key": private_key,
            },
        )

        return engine
    except FincopilotException as e:
        print(e)
        raise  # Re-raise already defined TrialBalanceSummaryExceptions
    except Exception as e:
        raise FincopilotException(
            f"Error creating Snowflake engine {e}",
            reason=Reason.DATABASE_CONNECTION_ERROR,
            subcomponent="get_snowflake_engine",
        ) from e
    
def get_secret():
    secret_name = Utils.GetEnvironmentVariableValue("SECRET_NAME")
    region_name = Utils.GetEnvironmentVariableValue("REGION_NAME")
    secrets_manager = Utils.GetEnvironmentVariableValue("SECRETS_MANAGER")

    if not secret_name or not region_name:
        raise FincopilotException(
            "Required environment variables are not set; SECRET_NAME or REGION_NAME",
            reason=Reason.ENV_VARIABLE_MISSING,
            subcomponent="get_secret",
        )
    
    # Create a Secrets Manager client
    try:
        session = boto3.session.Session()
        client = session.client(
            service_name=secrets_manager,
            region_name=region_name,
        )

        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise FincopilotException(
            "Failed to retrieve secret from AWS Secrets Manager",
            reason=Reason.SECRETS_MANAGER_ERROR,
            subcomponent="get_secret",
        ) from e
    except Exception as e:
        raise FincopilotException(
            e,
            reason=Reason.SECRETS_MANAGER_ERROR,
            subcomponent="get_secret",
        ) from e

    secret = get_secret_value_response["SecretString"]

    return secret

