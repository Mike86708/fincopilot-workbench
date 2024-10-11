from abc import ABC, abstractmethod

class DatabaseRepository(ABC):
    """
    Abstract base class defining the interface for database repository implementations.
    """

    @abstractmethod
    def establish_connection(self):
        """
        Establishes a connection to the database.
        """
        pass

    @abstractmethod
    def execute_query(self, sql_query):
        """
        Executes a SQL query and returns the results.
        
        Args:
        - sql_query (str): The SQL query to execute.

        Returns:
        - tuple: A tuple containing columns and rows.
        """
        pass

    @abstractmethod
    def close_connection(self):
        """
        Closes the database connection.
        """
        pass
