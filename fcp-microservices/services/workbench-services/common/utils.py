import re
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from workbench_exceptions import FincopilotException
from workbench_exception_codes import Reason
import os

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
    def generate_private_key(public_key: str) -> bytes:
        """
        Generate Private Key using public Key

        Args:
            public_key (str): Public Key

        Returns:
            bytes: Private Key in Bytes
        """

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

    """
    Method returns the value of the passed environment variable.
    It throws an exception if key is missing
    """
    @staticmethod
    def GetEnvironmentVariableValue(required_env_key):
            env_key_val = os.getenv(required_env_key)
            if env_key_val is None:
                raise FincopilotException(
                    f"Missing '{required_env_key}' in environment variables",
                    reason=Reason.MISSING_KEY,
                    subcomponent="getCredentials"
                )
            return env_key_val

    """
    Method build the org_hierarchy from the database rows
    """
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

    """
    Method to Format floating-point values as floats rather than in scientific notation.
    """
    @staticmethod
    def format_float(value):
        if value is None:
            return 0
        return float(f"{value:.2f}")