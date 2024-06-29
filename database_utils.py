import yaml


class DatabaseConnector:

    def __init__(self):
        self.db_config = None

    def init_db_engine(self):
        pass

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
