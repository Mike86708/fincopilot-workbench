import re
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class Utils:

    @staticmethod
    def validate_period_id(period_id):
        """
        Validates that the accounting period name is in the format 'MMM YYYY'.

        Args:
            period_id (str): The accounting period name to validate.

        Returns:
            bool: True if the format is correct, otherwise False.
        """
        pattern = r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}$"
        return re.match(pattern, period_id) is not None

    
    @staticmethod
    def build_org_hierarchy(row):
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

    @staticmethod
    def format_float(value):
        """Format floating-point values as floats rather than in scientific notation."""
        if value is None:
            return 0
        return float(f"{value:.2f}")