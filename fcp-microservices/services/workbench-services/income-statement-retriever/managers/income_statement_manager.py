import logging
from lib.util.utils import Utils
from lib.exception.exceptions import IncomeStatementException
from managers.account_manager import AccountManager

logger = logging.getLogger(__name__)

class IncomeStatementManager(AccountManager):
    def __init__(self, **kwargs):
        """Initialize IncomeStatementManager with specific exception class."""
        super().__init__(exception_class=IncomeStatementException, **kwargs)

    def get_sql_file_name(self) -> str:
        """Provide the SQL file name specific to income statements."""
        return "sql/income_statement_query.sql"

    def process_data(self, data):
        """Process the raw data from the income statement query."""
        logger.info("Processing income statement data.")
        result = []
        for row in data:
            org_hierarchy = Utils.build_org_hierarchy(row)
            result.append({
                "orgHierarchy": org_hierarchy,
                "account_id": row[7],
                "total_amount": Utils.format_float(row[5]),  # Adjust the index based on your SQL query
            })
        logger.info(f"Processed {len(result)} rows of income statement data.")
        return result
