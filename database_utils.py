import yaml
import sqlalchemy


class DatabaseConnector:

    def __init__(self):
        self.db_config = None
        self.engine = None

    def init_db_engine(self):
        if self.db_config:
            try:
                # Constructs database url
                db_url = db_url = (
                    f"postgresql://{self.db_config['RDS_USER']}:"
                    f"{self.db_config['RDS_PASSWORD']}@"
                    f"{self.db_config['RDS_HOST']}:"
                    f"{self.db_config['RDS_PORT']}/"
                    f"{self.db_config['RDS_DATABASE']}"
                )

                # Creates SQLAlchemy engine
                self.engine = sqlalchemy.create_engine(db_url)
                print("Database engine initialized successfully.")
            except Exception as e:
                print(f"Error initializing database engine: {e}")

    def read_db_creds(self):
        try:
            with open('db_config.yaml', 'r') as db_config_file:
                self.db_config = yaml.safe_load(db_config_file)
                return self.db_config
        except FileNotFoundError:
            print("Error: The file 'db_creds.yaml' does not exist.")
        except yaml.YAMLError as e:
            print(f"Error reading the YAML file: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
