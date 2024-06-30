from sqlalchemy import MetaData, inspect
import database_utils as db_utils


class DataExtractor:
    """Class to extract data from the database."""

    def __init__(self):
        db_connector = db_utils.DatabaseConnector.read_db_creds()
        if db_connector:
            self.db_engine = db_connector.init_db_engine()
        else:
            self.db_engine = None

    # Function to list all the table names in the RDS
    def list_db_tables(self):
        if self.db_engine is None:
            print("Database engine is not initialized.")
            return None

        try:
            # Returns the list of table names in the RDS
            inspector = inspect(self.db_engine)
            tables = inspector.get_table_names()
            print("Successfully extracted list of table names.")
            return tables
        except Exception as e:
            print(f"Error listing tables: {e}")
            return None

    def read_rds_table(self):
        pass
