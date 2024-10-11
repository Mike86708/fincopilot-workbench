from lib.exception.exceptions import AccountingActivityException
from lib.exception.exception_codes import Reason

class Utils:
    @staticmethod
    def format_float(value):
        """Format floating-point values as floats rather than in scientific notation."""
        try:
            if value is None:
                return 0.0
            return float(f"{value:.2f}")
        except (ValueError, TypeError) as e:
            raise AccountingActivityException(
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
            AccountingActivityException: If validation fails.
        """

        if from_period_id > to_period_id:
            raise AccountingActivityException(
                "The 'from_period_id' cannot be greater than the 'to_period_id'.",
                reason=Reason.INVALID_PARAMETER,
                subcomponent="validate_period_ids",
            )

    @staticmethod
    def validate_type(type_value: str):
        """
        Validates the 'type' parameter.

        Args:
            type_value (str): The type value to validate.

        Raises:
            AccountingActivityException: If validation fails.
        """
        valid_types = ["BALANCE_SUMMARY", "TRIAL_BALANCE"]
        if type_value not in valid_types:
            raise AccountingActivityException(
                message=f"Invalid 'type' parameter: {type_value}. Must be one of {valid_types}.",
                reason=Reason.INVALID_PARAMETER,
                subcomponent="validate_type",
            )

    @staticmethod
    def validate_event_params(event_params):
        required_params = {
            "type": str,
            "from_period_id": int,
            "to_period_id": int,
            "subsidiary_id": int,
            "account_id": int
        }

        for param, expected_type in required_params.items():
            if param not in event_params:
                raise AccountingActivityException(
                    message=f"Missing required parameter: {param}",
                    reason=Reason.MISSING_KEY,
                    subcomponent="validate_event_params"
                )
            if not isinstance(event_params[param], expected_type):
                raise AccountingActivityException(
                    message=f"Invalid parameter: {param}. Expected {expected_type.__name__}, got {type(event_params[param]).__name__}.",
                    reason=Reason.INVALID_PARAMETER,
                    subcomponent="validate_event_params"
                )

        # Validate 'type' parameter separately
        Utils.validate_type(event_params['type'])

        # Additional validation rules
        Utils.validate_period_ids(event_params['from_period_id'], event_params['to_period_id'])
