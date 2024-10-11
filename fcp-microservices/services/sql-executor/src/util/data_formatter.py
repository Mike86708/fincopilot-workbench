import json
import pandas as pd
import fnmatch
import logging

class DataFormatter:
    """
    A class to handle data formatting operations based on a JSON configuration file.

    Attributes:
        config_file (str): Path to the JSON configuration file.
        config (dict): Loaded configuration from config_file.
    """

    def __init__(self, config_file):
        """
        Initialize the DataFormatter instance with the path to the configuration file.

        Parameters:
            config_file (str): Path to the JSON configuration file.
        """
        self.config_file = config_file
        self.config = self.load_config()

    def load_config(self):
        """
        Load the JSON configuration file.

        Returns:
            dict: The loaded configuration as a dictionary.

        Raises:
            FileNotFoundError: If the configuration file is not found.
            json.JSONDecodeError: If there is an error decoding the JSON.
        """
        try:
            with open(self.config_file, 'r') as file:
                return json.load(file)
        except FileNotFoundError as e:
            logging.error(f"Configuration file not found: {e}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
            raise

    def get_formatting_group(self, group_name):
        """
        Retrieve a formatting group by its name from the configuration.

        Parameters:
            group_name (str): The name of the formatting group.

        Returns:
            dict or None: The formatting group if found, otherwise None.
        """
        for group in self.config.get('formatting_groups', []):
            if group['name'] == group_name:
                return group
        logging.warning(f"Formatting group '{group_name}' not found.")
        return None

    def convert_and_format_date(self, x, date_format):
        """
        Convert and format a date value based on the specified format.

        Parameters:
            x (any): The value to convert and format.
            date_format (str): The desired date format.

        Returns:
            str or None: The formatted date string or None if input is None or cannot be parsed.
        """
        if pd.isnull(x) or x == '':
            return x
        try:
            # Attempt to parse date in YYYY-MM-DD format
            dt = pd.to_datetime(x, format='%Y-%m-%d', errors='raise')
        except ValueError:
            try:
                # Attempt to parse date in YYYY-DD-MM format
                dt = pd.to_datetime(x, format='%Y-%d-%m', errors='raise')
            except ValueError:
                # If parsing fails, return the original value
                return x
        # Return formatted date string
        return dt.strftime(date_format)

    def format_date(self, df, column_patterns, date_format):
        """
        Format date columns in the DataFrame according to the specified patterns and date format.

        Parameters:
            df (pd.DataFrame): The DataFrame to format.
            column_patterns (list): List of patterns to match column names.
            date_format (str): The desired date format.

        Returns:
            pd.DataFrame: The DataFrame with formatted date columns.

        Raises:
            Exception: If there is an error during date formatting.
        """
        try:
            for pattern in column_patterns:
                matching_columns = fnmatch.filter(df.columns, pattern)
                for col in matching_columns:
                    # Apply date formatting, preserving None values
                    df[col] = df[col].apply(lambda x: self.convert_and_format_date(x, date_format) if x is not None else None)
            logging.info("Date formatting applied successfully.")
            return df
        except Exception as e:
            logging.error(f"Error formatting date columns: {e}")
            raise

    def add_thousand_separator(self, df, column_patterns, decimal_places=2):
        """
        Add thousand separators to numeric columns in the DataFrame based on the specified patterns.

        Parameters:
            df (pd.DataFrame): The DataFrame to format.
            column_patterns (list): List of patterns to match column names.
            decimal_places (int): Number of decimal places to round to (default is 2).

        Returns:
            pd.DataFrame: The DataFrame with thousand separators added to numeric columns.

        Raises:
            Exception: If there is an error during formatting.
        """
        formatted_columns = set()
        try:
            for pattern in column_patterns:
                matching_columns = fnmatch.filter(df.columns, pattern)
                logging.info(f"Pattern '{pattern}' matched columns: {matching_columns}")
                for col in matching_columns:
                    if col not in formatted_columns:
                        try:
                            # Apply thousand separators, preserving None values
                            df[col] = df[col].apply(
                                lambda x: f"{round(x, decimal_places):,}" if pd.notnull(x) and x is not None else None
                            )
                            formatted_columns.add(col)
                            logging.info(f"Formatted column '{col}' with thousand separators.")
                        except Exception as e:
                            logging.error(f"Error formatting column '{col}': {e}")
            return df
        except Exception as e:
            logging.error(f"Error adding thousand separators: {e}")
            raise

    def remove_columns(self, df, column_patterns):
        """
        Remove columns from the DataFrame based on the specified patterns.

        Parameters:
            df (pd.DataFrame): The DataFrame to modify.
            column_patterns (list): List of patterns to match column names.

        Returns:
            pd.DataFrame: The DataFrame with specified columns removed.

        Raises:
            Exception: If there is an error during column removal.
        """
        try:
            for col_pattern in column_patterns:
                matching_cols = [col for col in df.columns if fnmatch.fnmatch(col, col_pattern)]
                df.drop(columns=matching_cols, inplace=True)
                logging.info(f"Removed columns: {matching_cols}")
            return df
        except Exception as e:
            logging.error(f"Error removing columns: {e}")
            raise

    def apply_formatting(self, data, group_name):
        """
        Apply the specified formatting group to the provided data.

        Parameters:
            data (dict): The input data dictionary with 'columns' and 'rows'.
            group_name (str): The name of the formatting group to apply.

        Returns:
            dict: The formatted data dictionary with applied formatting.

        Raises:
            ValueError: If the formatting group is not found in the configuration.
            Exception: If there is an error applying the formatting.
        """
        try:
            # Convert data to DataFrame
            df = pd.DataFrame(data["rows"], columns=data["columns"])
            formatting_group = self.get_formatting_group(group_name)

            if not formatting_group:
                raise ValueError(f"Formatting group '{group_name}' not found in configuration.")

            # Apply each instruction in the formatting group
            for instruction in formatting_group['instructions']:
                if instruction['type'] == 'format_date':
                    df = self.format_date(df, instruction['column_patterns'], instruction['format'])
                elif instruction['type'] == 'add_thousand_separator':
                    df = self.add_thousand_separator(df, instruction['column_patterns'])
                elif instruction['type'] == 'remove_columns':
                    df = self.remove_columns(df, instruction['column_patterns'])
                # Add more conditions for other types of formatting instructions as needed

            # Convert DataFrame back to dictionary format
            formatted_data = {
                "columns": df.columns.tolist(),
                "rows": df.values.tolist()
            }

            logging.info("Formatting applied successfully.")
            return formatted_data
        except Exception as e:
            logging.error(f"Error applying formatting: {e}")
            raise

# Example usage
if __name__ == "__main__":
    # Sample CSV loading for testing
    df = pd.read_csv('2024-07-26 3_39pm.csv')
    
    data = {
        "columns": df.columns.tolist(),
        "rows": df.where(pd.notnull(df), None).values.tolist()
    }
    
    # Initialize the DataFormatter with the path to the configuration file
    data_formatter = DataFormatter('data_formatting_config.json')
    group_name = "Standard Formatting"

    try:
        # Apply formatting and print the result
        formatted_data = data_formatter.apply_formatting(data, group_name)
        # print(formatted_data)
    except Exception as e:
        logging.error(f"Failed to apply formatting: {e}")
