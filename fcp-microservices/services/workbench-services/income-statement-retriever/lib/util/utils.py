from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from lib.exception.exceptions import IncomeStatementException
from lib.exception.exception_codes import Reason
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class Utils:

    @staticmethod
    def build_org_hierarchy(row):
        """
        Build organization hierarchy from a row.

        Args:
            row: List containing organization levels and account hierarchy

        Returns:
            List of organization hierarchy
        """
        try:
            logger.info(f"Building organization hierarchy for row: {row}")
            org_hierarchy = []
            if row[0]:  # level_3_account
                org_hierarchy.append(row[0])
            if row[1] and row[1] != row[0]:  # level_2_account
                org_hierarchy.append(row[1])
            if row[2]:  # level_1_account
                org_hierarchy.append(row[2])

            # Add the account hierarchy
            if row[4]:  # account_hierarchy
                org_hierarchy.extend(row[4].split(' -> '))

            return org_hierarchy
        except KeyError as e:
            raise IncomeStatementException(
                "A key error occurred while building the organization hierarchy",
                reason=Reason.MISSING_KEY,
                subcomponent="build_org_hierarchy"
            ) from e
        except Exception as e:
            logger.error(f"Error in build_org_hierarchy: {e}")
            
            raise IncomeStatementException(
                "An unexpected error occurred while building organization hierarchy",
                reason=Reason.UNEXPECTED_ERROR,
                subcomponent="build_org_hierarchy"
            ) from e

    @staticmethod
    def format_float(value):
        """Format floating-point values as floats rather than in scientific notation."""
        try:
            logger.info(f"Formatting float value: {value}")
            if value is None:
                return 0.0
            return float(f"{value:.2f}")
        except (ValueError, TypeError) as e:
            logger.error(f"Error formatting float value: {e}")
            raise IncomeStatementException(
                "An error occurred while formatting the float value",
                reason=Reason.INVALID_PARAMETER,
                subcomponent="format_float",
            ) from e

    @staticmethod
    def validate_period_ids(from_period_id: int, to_period_id: int):
        """
        Validates the from_period_id and to_period_id

        Args:
            from_period_id (int): The starting period ID.
            to_period_id (int): The ending period ID.

        Raises:
            IncomeStatementException: If validation fails.
        """
        if not isinstance(from_period_id, int) or not isinstance(to_period_id, int):
            raise IncomeStatementException(
                "Period IDs must be integers",
                reason=Reason.INVALID_PARAMETER,
                subcomponent="validate_period_ids",
            )

        if from_period_id > to_period_id:
            raise IncomeStatementException(
                "The 'from_period_id' should not be bigger than the 'to_period_id'",
                reason=Reason.INVALID_INPUT,
                subcomponent="validate_period_ids",
            )

    @staticmethod
    def validate_subsidiary_id(subsidiary_id: int):
        """
        Validate the subsidiary_id

        Args:
            subsidiary_id (int): Subsidiary ID to validate.

        Raises:
            IncomeStatementException: If validation fails.
        """
        if not isinstance(subsidiary_id, int):
            raise IncomeStatementException(
                "Subsidiary ID must be an integer",
                reason=Reason.INVALID_PARAMETER,
                subcomponent="validate_subsidiary_id",
            )

    @staticmethod
    def validate_event_params(event_params: dict):
        """
        Validate event parameters

        Args:
            event_params (dict): Event parameters dictionary

        Raises:
            IncomeStatementException: If validation fails.
        """
        required_keys = ['from_period_id', 'to_period_id', 'subsidiary_id']

        for key in required_keys:
            if key not in event_params:
                raise IncomeStatementException(
                    f"Missing required key: {key}",
                    reason=Reason.MISSING_KEY,
                    subcomponent="validate_event_params",
                )
            
            if event_params[key] is None:
                raise IncomeStatementException(
                    f"Invalid input for key: {key}",
                    reason=Reason.INVALID_INPUT,
                    subcomponent="validate_event_params",
                )

        # Additional type and range validations
        Utils.validate_period_ids(event_params['from_period_id'], event_params['to_period_id'])
        Utils.validate_subsidiary_id(event_params['subsidiary_id'])