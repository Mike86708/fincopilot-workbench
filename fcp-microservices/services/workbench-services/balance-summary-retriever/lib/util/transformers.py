"""
This library file is not currently used in our project, but it is kept for future reference.
Please do not remove it, as it may be useful for upcoming features or enhancements.
"""

from abc import ABC, abstractmethod
import pandas as pd
from lib.exception.exception_codes import Reason
from lib.exception.exceptions import BalanceSummaryException
import json
import math

class DataTransformer(ABC):

    @abstractmethod
    def transform(self, data):
        pass


class HierarchyTransformer(DataTransformer):

    columns = []
    combine_columns = []
    hierarchy_column = ""
    id_column = ""
    parent_column = ""
    main_column = ""

    def convert_row(self, row):
        return row

    def compare_columns(self, columnA, columnB):
        return columnA == columnB

    def get_hierarchy(self, row, id_map, tree):
        """
        Constructs a hierarchy for a given row and updates the ID map and tree structure.

        Args:
            row (pd.Series): The current row of the DataFrame being processed.
            id_map (dict): A dictionary mapping account IDs to DataFrame indices.
            tree (dict): A tree structure representing parent-child relationships.

        Returns:
            list: A list representing the hierarchy path.
        """
        try:
            hierarchy = []
            last_value = None

            id = row.get(self.id_column)
            parent_id = row.get("PARENT_ID", "root")

            # Validate that account_id is present
            if id is None:
                raise ValueError(f"Missing '{self.id_column}' in the row")

            # Set ID MAP
            id_map[id] = row.name

            # Set Tree
            if parent_id not in tree:
                tree[parent_id] = []

            tree[parent_id].append(id)

            # Iterate through LEVEL columns and construct hierarchy
            for combine_column in self.combine_columns:
                current_value = row.get(combine_column)
                if not self.compare_columns(current_value, last_value):
                    hierarchy.append(current_value)
                    last_value = current_value

            # Add the account name to the hierarchy
            hierarchy.append(row.get(self.main_column))

            return hierarchy
        except ValueError as e:
            raise BalanceSummaryException(
                e, message="Value error in get_hierarchy function"
            )
        except Exception as e:
            raise BalanceSummaryException(
                e, message="Unexpected error in get_hierarchy function"
            )

    def set_relation(self, df, id_map, tree):
        """
        Updates the DataFrame with hierarchical paths using the tree structure.

        Args:
            df (pd.DataFrame): The DataFrame containing the balance summary.
            id_map (dict): A dictionary mapping account IDs to DataFrame indices.
            tree (dict): A tree structure representing parent-child relationships.
        """
        try:
            stack = ["root"]

            while stack:
                top = stack.pop()
                if top not in tree:
                    continue

                for child in tree[top]:
                    stack.append(child)

                    if top == "root" or top not in id_map:
                        continue

                    hierarchy = df.iloc[id_map[top]][self.hierarchy_column].copy()
                    hierarchy.append(df.iloc[id_map[child]][self.main_column])
                    df.at[id_map[child], self.hierarchy_column] = hierarchy
        except KeyError as e:
            raise BalanceSummaryException(
                reason=Reason.MISSING_OBJECT_KEY,
                subcomponent="set_relation",
            )
        except Exception as e:
            raise BalanceSummaryException(
                e,
                message="Unexpected error in set_relation function",
            )

    def transform(self, data):
        """
        Transforms the balance summary into a hierarchical structure.

        Args:
            balancesummary (list): A list of tuples representing the balance summary.

        Returns:
            list: A list of dictionaries containing hierarchical paths and financial data.
        """
        if not data:
            return []

        try:
            # Create a DataFrame from the balance summary
            df = pd.DataFrame(data, columns=self.columns)
        except ValueError as e:
            raise BalanceSummaryException(
                e, message="Error creating DataFrame in transform function"
            )
        except Exception as e:
            raise BalanceSummaryException(
                e, message="Unexpected error creating DataFrame in transform function"
            )

        id_map = {}
        tree = {}

        try:
            # Apply the get_hierarchy function to create the 'PATH' column
            df[self.hierarchy_column] = df.apply(
                lambda row: self.get_hierarchy(row, id_map, tree), axis=1
            )
        except Exception as e:
            raise BalanceSummaryException(
                e, message="Error during hierarchy generation in transform function"
            )

        try:
            # Add relationships using the tree structure
            self.set_relation(df, id_map, tree)
        except Exception as e:
            raise BalanceSummaryException(
                e, message="Error during relationship setting in transform function"
            )

        converted_data = []
        for index, row in df.iterrows():
            try:
                converted_data.append(self.convert_row(row))
            except Exception as e:
                raise BalanceSummaryException(
                    e, message=f"Error processing row {index} in transform function"
                )
        # return converted_data
        return json.dumps(converted_data)


class BalanceSummaryTransformer(HierarchyTransformer):
    columns = [
        "LEVEL_3_ACCOUNT",
        "LEVEL_2_ACCOUNT",
        "LEVEL_1_ACCOUNT",
        "LEVEL_0_ROLLUP",
        "ACCOUNT_NAME",
        "CREDIT",
        "DEBIT",
        "TOTAL_AMOUNT",
        "PARENT_ID",
        "ACCOUNT_ID",
    ]

    combine_columns = [
        "LEVEL_3_ACCOUNT",
        "LEVEL_2_ACCOUNT",
        "LEVEL_1_ACCOUNT",
    ]

    hierarchy_column = "PATH"
    id_column = "ACCOUNT_ID"
    parent_column = "PARENT_ID"
    main_column = "ACCOUNT_NAME"

    def compare_columns(self, columnA, columnB):
        """
        Compares two levels to determine if they are equivalent, taking into account possible pluralization.

        Args:
            levelA (str): The first level to compare.
            levelB (str): The second level to compare.

        Returns:
            bool: True if the levels are considered equivalent, otherwise False.
        """
        try:
            if columnA == columnB:
                return True

            if columnA is None or columnB is None:
                return False

            # Check for pluralization (simple rule: adding 's' or 'es')
            if columnA.endswith("s") and columnA[:-1] == columnB:
                return True  # levelA is plural, check if levelB is singular
            elif columnA.endswith("es") and columnA[:-2] == columnB:
                return True  # levelA ends with 'es', check if levelB is singular

            if columnB.endswith("s") and columnB[:-1] == columnA:
                return True  # levelB is plural, check if levelA is singular
            elif columnB.endswith("es") and columnB[:-2] == columnA:
                return True  # levelB ends with 'es', check if levelA is singular

            return False
        except Exception as e:
            raise BalanceSummaryException(e, message="Error in check_level function")
        
    def check_number(self, num):
        if num is None:
            return 0
        f_num = float(num)
        if math.isnan(f_num):
            return 0
        return f_num

    def convert_row(self, row):
        return {
            "orgHierarchy": row["PATH"],
            "credit": self.check_number(row["CREDIT"]),
            "debit": self.check_number(row["DEBIT"]),
            "total_amount": self.check_number(row["TOTAL_AMOUNT"]),
        }
