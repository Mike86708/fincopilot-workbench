import os
import json
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from workbench_exception_codes import Reason
from trial_balance_summary_exceptions import TrialBalanceSummaryException
from utils import Utils
from snowflake_common import *


# Function to get trial balance summary
def get_trial_balance_summary(subsidiary_id: int, period_id: str):
    try:
        engine = get_snowflake_engine()

        if engine is None:
            raise TrialBalanceSummaryException(
                "Snowflake engine is not available",
                reason=Reason.DATABASE_CONNECTION_ERROR,
                subcomponent="get_trial_balance_summary",
            )

        query = None

        try:
            with open("trial_balance_summary.sql", "r") as file:
                query = file.read()
        except FileNotFoundError as e:
            raise TrialBalanceSummaryException(
                "SQL query file not found",
                reason=Reason.SQL_FILE_NOT_FOUND,
                subcomponent="get_trial_balance_summary",
            ) from e
        except Exception as e:
            raise TrialBalanceSummaryException(
                "Error reading SQL query file",
                reason=Reason.FILE_READ_ERROR,
                subcomponent="get_trial_balance_summary",
            ) from e

        if query is None or not query.strip():
            raise TrialBalanceSummaryException(
                "SQL query is empty",
                reason=Reason.INVALID_SQL_QUERY,
                subcomponent="get_trial_balance_summary",
            )
        
        result = []

        with engine.connect() as connection:
            trial_balance_summary_information = connection.execute(
                text(query),
                {
                    "period_id": period_id,
                    "subsidiary_id": subsidiary_id,
                },
            ).fetchall()

            for row in trial_balance_summary_information:
                org_hierarchy = Utils.build_org_hierarchy(row)
                totaL_amount = Utils.format_float(row[5])
                account_id = row[7]
                if (totaL_amount >= 0 ):
                    debit = abs(totaL_amount)
                    credit = None
                else:
                    credit = abs(totaL_amount)
                    debit = None
                result.append({
                    "orgHierarchy": org_hierarchy,
                    "account_id": account_id,
                    "credit": credit,  # total_credit
                    "debit": debit,   # total_debit
                    "total_amount": totaL_amount  # total_amount
                })
        
        return result
    except SQLAlchemyError as e:
        raise TrialBalanceSummaryException(
            "Database error occurred",
            reason=Reason.DATABASE_EXECUTION_ERROR,
            subcomponent="get_trial_balance_summary",
        ) from e
    except TrialBalanceSummaryException:
        raise  # Re-raise already defined TrialBalanceSummaryExceptions
    except Exception as e:
        raise TrialBalanceSummaryException(
            "An unexpected error occurred while getting balance summary",
            reason=Reason.UNEXPECTED_ERROR,
            subcomponent="get_trial_balance_summary",
        ) from e


# Lambda handler function
def lambda_handler(event, context):
    try:
        subsidiary_id = event.get("subsidiary_id")
        period_id = event.get("period_id")

        # Validate that subsidiary_id is an integer
        if subsidiary_id is None or not isinstance(subsidiary_id, int):
            raise TrialBalanceSummaryException(
                "Invalid or missing 'subsidiary_id'. It must be an integer.",
                reason=Reason.INVALID_INPUT,
                subcomponent="lambda_handler",
            )

        # Validate that period_id is a string and matches the format 'MMM YYYY'
        if not isinstance(period_id, str) or not Utils.validate_period_id(period_id):
            raise TrialBalanceSummaryException(
                "Invalid or missing 'period_id'. It must be a string in the format 'MMM YYYY' (e.g., 'Jan 2023').",
                reason=Reason.INVALID_INPUT,
                subcomponent="lambda_handler",
            )

        trial_balance_summary = get_trial_balance_summary(subsidiary_id, period_id)

        response = {
            "statusCode": 200,
            "body": trial_balance_summary,
        }
    except TrialBalanceSummaryException as e:
        print(f"Error in lambda_handler: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps(e.get_response_data(), indent=2),
        }
    except Exception as e:
        # Catch any other unforeseen exceptions
        print(f"Unexpected error in lambda_handler: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "An unexpected error occurred.",
                    "details": str(e),
                },
                indent=2,
            ),
        }

    return response


if __name__ == "__main__":
    try:
        event = {"subsidiary_id": 1, "period_id": "Jun 2022"}
        context = {}
        response = lambda_handler(event, context)
        print("Lambda response:", response)
    except Exception as e:
        print(f"Error running the script: {e}")
