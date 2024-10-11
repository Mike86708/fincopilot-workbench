from lib.util.utils import Utils
from lib.exception.exception_codes import Reason
from lib.exception.exceptions import AccountFilterException
from managers.account_manager import AccountManager
import pandas as pd

class AccountFilterManager(AccountManager):
    def __init__(self, **kwargs):
        super().__init__(exception_class=AccountFilterException, **kwargs)
    
    def get_sql_file_name(self):
        """
        Determine the SQL file name based on the 'type' parameter.

        Returns:
            str: SQL file name.
        """
        request_type = self.params.get('data_type')
        if request_type == "TRIAL_BALANCE":
            return 'sql/trial_balance.sql'
        elif request_type == "BALANCE_SUMMARY":
            return 'sql/balance_summary.sql'
        elif request_type == "INCOME_STATEMENT":
            return 'sql/income_statement.sql'
        else:
            raise AccountFilterException(
                message=f"Invalid request type: {request_type}",
                reason=Reason.INVALID_PARAMETER,
                subcomponent="get_sql_file_name"
            )
        

    def process_data(self, data):
        """Process the raw data from the accounting activity query."""
        # Convert the SQL result into a pandas DataFrame
        df = pd.DataFrame(data, columns=['ACCOUNT_ID', 'ACCOUNT_NAME', 'PARENT_ID'])

        # Treat NaN as None for root-level nodes
        df['PARENT_ID'] = df['PARENT_ID'].fillna(-1)  # Treat NaN as -1 for root-level nodes

        # Remove duplicate entries based on ACCOUNT_ID and PARENT_ID to clean the data
        cleaned_data = df.drop_duplicates(subset=['ACCOUNT_ID', 'PARENT_ID'])

        # Build the hierarchy
        return Utils.build_hierarchy(cleaned_data)