import pandas as pd


class DataCleaning:
    """Class for cleaning user data."""

    @staticmethod
    def clean_user_data(df_dict):
        """Clean the user data based on the table name.

        Args:
            df_dict (dict): Dictionary containing table names as keys and DataFrames as values.

        Returns:
            dict: Dictionary containing cleaned DataFrames.
        """
        cleaned_data = {}
        for table_name, df in df_dict.items():
            if table_name == 'legacy_store_details':
                cleaned_data[table_name] = DataCleaning.clean_legacy_store_details(
                    df)
            elif table_name == 'dim_card_details':
                cleaned_data[table_name] = DataCleaning.clean_dim_card_details(
                    df)
            elif table_name == 'legacy_users':
                cleaned_data[table_name] = DataCleaning.clean_legacy_users(df)
            elif table_name == 'orders_table':
                cleaned_data[table_name] = DataCleaning.clean_orders_table(df)
            else:
                raise ValueError(f"Unsupported table_name: {table_name}")

        return cleaned_data

    @staticmethod
    def clean_legacy_store_details(df):
        """Clean the user data for legacy_store_details table.

        Args:
            df (pd.DataFrame): DataFrame containing user data for legacy_store_details.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        # Ensure the index column exists and is set properly
        if 'index' in df.columns:
            df.set_index('index', inplace=True)

        # Correct the column name if needed
        if 'longitude' in df.columns:
            df.rename(columns={"longitude": "longitude"}, inplace=True)

        # Convert all column names to lowercase
        df.columns = [col.lower() for col in df.columns]

        # Process each column based on its expected type
        for column in df.columns:
            if column == "opening_date":
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif column in ("longitude", "latitude", "staff_numbers", "lat"):
                df[column] = pd.to_numeric(df[column], errors='coerce')
            elif column in ("address", "locality", "store_code", "store_type", "country_code", "continent"):
                df[column] = df[column].astype('string')

        # Replace 'NULL' with None in string columns
        for col in df.select_dtypes(include=['string']).columns:
            df[col] = df[col].apply(lambda x: None if x == 'NULL' else x)

        # Drop columns where all values are NaN
        df.dropna(axis=1, how='all', inplace=True)

        # Drop rows that do not have at least 70% non-NaN values
        df.dropna(axis=0, thresh=round(df.shape[1] * 0.7), inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

    @staticmethod
    def clean_dim_card_details(df):
        """Clean the user data for dim_card_details table.

        Args:
            df (pd.DataFrame): DataFrame containing user data for dim_card_details.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        df.columns = [col.lower() for col in df.columns]

        for column in df.columns:
            if column in ("date_payment_confirmed", "expiry_date"):
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif column == "card_provider":
                df[column] = df[column].astype('string')

        df.dropna(axis=0, thresh=round(df.shape[1] * 0.7), inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df

    @staticmethod
    def clean_legacy_users(df):
        """Clean the user data for legacy_users table.

        Args:
            df (pd.DataFrame): DataFrame containing user data for legacy_users.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        if 'index' in df.columns:
            df.set_index('index', inplace=True)

        df['date_of_birth'] = pd.to_datetime(
            df['date_of_birth'], errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype('string')

        for col in df.select_dtypes(include=['string']).columns:
            df[col] = df[col].replace('NULL', None)

        df.dropna(axis=0, thresh=round(df.shape[1] * 0.7), inplace=True)
        mask = df['user_uuid'].str.contains(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', na=False)
        df = df[mask]
        df.reset_index(drop=True, inplace=True)

        return df

    @staticmethod
    def clean_orders_table(df):
        """Clean the user data for orders_table.

        Args:
            df (pd.DataFrame): DataFrame containing user data for orders_table.

        Returns:
            pd.DataFrame: Cleaned DataFrame.
        """
        df_index = df['index'].sort_values().reset_index(drop=True)
        df_level_0 = df['level_0'].sort_values().reset_index(drop=True)
        compare = df_index == df_level_0
        if compare.all():
            df.set_index('index', inplace=True)
            df.drop(columns=['level_0'], inplace=True)
        elif 'index' in df.columns:
            df.set_index('index', inplace=True)

        df.dropna(axis=1, thresh=round(df.shape[0] * .95), inplace=True)

        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype('string')

        df.reset_index(drop=True, inplace=True)

        return df
