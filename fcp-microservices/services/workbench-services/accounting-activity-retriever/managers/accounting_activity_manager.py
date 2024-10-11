from lib.util.utils import Utils
from lib.exception.exception_codes import Reason
from lib.exception.exceptions import AccountingActivityException
from managers.account_manager import AccountManager

class AccountingActivityManager(AccountManager):
    def __init__(self, **kwargs):
        super().__init__(exception_class=AccountingActivityException, **kwargs)
    
    def get_sql_file_name(self):
        """
        Determine the SQL file name based on the 'type' parameter.

        Returns:
            str: SQL file name.
        """
        request_type = self.params.get('type')
        if request_type == "TRIAL_BALANCE":
            return 'sql/trial_balance.sql'
        elif request_type == "BALANCE_SUMMARY":
            return 'sql/balance_summary.sql'
        else:
            raise AccountingActivityException(
                message=f"Invalid request type: {request_type}",
                reason=Reason.INVALID_PARAMETER,
                subcomponent="get_sql_file_name"
            )

    def process_data(self, data):
        """Process the raw data from the accounting activity query."""
        result = []
        
        for row in data:
            transaction_data = {
                "Account_Number": row[0],
                "Transaction_Type": row[1],
                "Transaction_Date": row[2],
                "Document_Number": row[3],
                "Link_Url": "temp",
                "Name": row[4],
                "Amount": Utils.format_float(row[5]),
                "Memo": "temp"
            }
            result.append(transaction_data)

        return result
