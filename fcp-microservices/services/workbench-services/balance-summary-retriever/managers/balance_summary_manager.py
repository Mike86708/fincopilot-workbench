from managers.account_manager import AccountManager
from lib.exception.exceptions import BalanceSummaryException
from lib.util.utils import Utils

class BalanceSummaryManager(AccountManager):
    def __init__(self, **kwargs):
        """Initialize BalanceSummaryManager with specific exception class."""
        super().__init__(exception_class=BalanceSummaryException, **kwargs)

    def get_sql_file_name(self) -> str:
        """
        Provide the SQL file name specific to the balance summary.
        """
        return "sql/balance_summary_query.sql"

    def process_data(self, data):
        """
        Process the raw data from the balance summary query.
        """
        result = []
        for row in data:
            org_hierarchy = Utils.build_org_hierarchy(row)
            result.append({
                "orgHierarchy": org_hierarchy,
                "account_id": row[9],
                "credit": Utils.format_float(row[5]),  # Assuming total_credit is in the 6th column
                "debit": Utils.format_float(row[6]),   # Assuming total_debit is in the 7th column
                "total_amount": Utils.format_float(row[7]),  # Assuming total_amount is in the 8th column
            })
        return result
