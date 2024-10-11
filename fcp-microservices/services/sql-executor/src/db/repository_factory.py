import logging
from .snowflake_repository import SnowflakeRepository
from .database_repository import DatabaseRepository

class UnsupportedDatabaseError(Exception):
    """Exception raised for unsupported database types."""
    pass

class RepositoryFactory:
    """
    Factory class to create instances of database repositories.
    """

    @staticmethod
    def create_repository(db_type, config):
        """
        Creates a database repository instance based on the specified type.
        
        Args:
        - db_type (str): The type of the database (e.g., 'snowflake').
        - config (dict): Configuration data required to initialize the repository.

        Returns:
        - DatabaseRepository: An instance of a class implementing the DatabaseRepository interface.

        Raises:
        - UnsupportedDatabaseError: If the specified database type is not supported.
        - RuntimeError: If an unexpected error occurs during repository creation.
        """
        try:
            if db_type == 'snowflake':
                return SnowflakeRepository(config)
            else:
                raise UnsupportedDatabaseError(f"Unsupported database type: {db_type}")
        except UnsupportedDatabaseError as e:
            logging.error(f"Unsupported database type error: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during repository creation: {e}")
            raise RuntimeError("Unexpected error during repository creation") from e

