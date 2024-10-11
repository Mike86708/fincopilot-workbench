from lib.exception.exceptions import AccountFilterException
from lib.exception.exception_codes import Reason
from collections import defaultdict

class Utils:
    @staticmethod
    def build_hierarchy(df):
        """Build the hierarchy from the cleaned DataFrame."""
        # Create a dictionary to store account relationships by parent ID
        hierarchy = defaultdict(list)

        # Populate dictionary with account information
        for _, row in df.iterrows():
            account_id = int(row['ACCOUNT_ID'])
            parent_id = int(row['PARENT_ID']) if row['PARENT_ID'] != -1 else None
            hierarchy[parent_id].append({
                'label': row['ACCOUNT_NAME'],
                'value': account_id,
                'children': []
            })

        # Link children to their respective parents
        for parent_id, children in hierarchy.items():
            for child in children:
                child['children'] = hierarchy.get(child['value'], [])

        # Return root-level accounts (those without a parent)
        return hierarchy[None]
    
    @staticmethod
    def validate_type(type_value: str):
        """
        Validates the 'type' parameter.

        Args:
            type_value (str): The type value to validate.

        Raises:
            AccountFilterException: If validation fails.
        """
        valid_types = ["BALANCE_SUMMARY", "INCOME_STATEMENT", "TRIAL_BALANCE"]
        if type_value not in valid_types:
            raise AccountFilterException(
                message=f"Invalid 'type' parameter: {type_value}. Must be one of {valid_types}.",
                reason=Reason.INVALID_PARAMETER,
                subcomponent="validate_type",
            )

    @staticmethod
    def validate_event_params(event_params):
        required_params = {
            "data_type": str,
            "subsidiary_id": int,
        }

        for param, expected_type in required_params.items():
            if param not in event_params:
                raise AccountFilterException(
                    message=f"Missing required parameter: {param}",
                    reason=Reason.MISSING_KEY,
                    subcomponent="validate_event_params"
                )
            if not isinstance(event_params[param], expected_type):
                raise AccountFilterException(
                    message=f"Invalid parameter: {param}. Expected {expected_type.__name__}, got {type(event_params[param]).__name__}.",
                    reason=Reason.INVALID_PARAMETER,
                    subcomponent="validate_event_params"
                )

        # # Validate 'data_type' parameter separately
        Utils.validate_type(event_params['data_type'])

