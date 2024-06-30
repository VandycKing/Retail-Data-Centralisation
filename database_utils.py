import yaml
import sqlalchemy


class DatabaseConnector:

    def __init__(self, db_config):
        self.db_config = db_config
        self.db_engine = None

    def init_db_engine(self):
        """This method initialises the database engine

        Returns:
            object: Returns the database engine object
        """
        if self.db_config:
            try:
                # Constructs database URL
                db_url = (
                    f"postgresql://{self.db_config['RDS_USER']}:"
                    f"{self.db_config['RDS_PASSWORD']}@"
                    f"{self.db_config['RDS_HOST']}:"
                    f"{self.db_config['RDS_PORT']}/"
                    f"{self.db_config['RDS_DATABASE']}"
                )

                # Creates SQLAlchemy engine
                self.db_engine = sqlalchemy.create_engine(db_url)
                print("Database engine initialized successfully.")
                return self.db_engine
            except Exception as e:
                print(f"Error initializing database engine: {e}")

    @classmethod
    def read_db_creds(cls):
        """This class method reads the RDS database credentials
        from a yaml file and returns the class object

        Returns:
            object: Returns the class object of the Database Connector
            class
        """
        try:
            with open('db_creds.yaml', 'r') as db_config_file:
                db_config = yaml.safe_load(db_config_file)
                return cls(db_config)
        except FileNotFoundError:
            print("Error: The file 'db_creds.yaml' does not exist.")
        except yaml.YAMLError as e:
            print(f"Error reading the YAML file: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        return None  # Returns None if there is an error


# Initialise database
if __name__ == "__main__":
    db_connector = DatabaseConnector.read_db_creds()
    if db_connector is not None:
        db_connector.init_db_engine()
