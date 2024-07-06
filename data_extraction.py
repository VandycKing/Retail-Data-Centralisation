import pandas as pd
import tabula
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
        """This function lists all the table names in the 
        RDS database.

        Returns:
            list: list of table names in a string format
        """
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

    # Function to read tables from the rds database
    def read_rds_table(self, table_name):
        """This function reads tables from the rds database into 
        a pandas dataframe.

        Args:
            table_name (str): Name of table in the rds database

        Returns:
            pd.DataFrame: Returns a pandas dataframe 
        """
        if self.db_engine is None:
            print("Database engine is not initialized.")
            return None

        try:
            # Use pandas to read the table into a DataFrame
            df = pd.read_sql_table(table_name, self.db_engine)
            print(f"Successfully read table {table_name}.")
            return df
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return None

    def retrieve_pdf_data(self, link):
        """This method reads tabular data from a PDF link.

        Args:
            link (str): This is a link to a PDF file.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the concatenated data.
        """
        # Read the tables from the PDF with stream mode
        dfs = tabula.read_pdf(link, pages='all', stream=True)

        # Print the number of DataFrames extracted
        print(f"The number of DataFrames in the link is: {len(dfs)}")

        # Check if dfs is a list of DataFrames and concatenate them
        if isinstance(dfs, list):
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = dfs

        # Print the shape of the concatenated DataFrame
        print(
            f"The DataFrame has {df.shape[0]} rows and {df.shape[1]} columns")

        return df
