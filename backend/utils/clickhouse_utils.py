from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
import pandas as pd


class ClickHouseConnector:
    def __init__(self, host, port, user=None, password=None, jwt_token=None, database=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.jwt_token = jwt_token
        self.database = database
        self.client = None

    def connect(self):
        try:
            settings = {}
            if self.jwt_token:
                settings['jwt'] = self.jwt_token

            self.client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                settings=settings
            )
            return True
        except ClickHouseError as e:
            raise Exception(f"ClickHouse connection error: {str(e)}")

    def get_tables(self):
        if not self.client:
            self.connect()

        try:
            query = "SHOW TABLES"
            if self.database:
                query = f"SHOW TABLES FROM {self.database}"

            result = self.client.execute(query)
            return [table[0] for table in result]
        except ClickHouseError as e:
            raise Exception(f"Error fetching tables: {str(e)}")

    def get_table_columns(self, table_name):
        if not self.client:
            self.connect()

        try:
            query = f"DESCRIBE TABLE {table_name}"
            result = self.client.execute(query)
            columns = []
            for row in result:
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'default': row[2],
                    'comment': row[3]
                })
            return columns
        except ClickHouseError as e:
            raise Exception(f"Error fetching columns: {str(e)}")

    def execute_query(self, query):
        if not self.client:
            self.connect()

        try:
            result, columns = self.client.execute(query, with_column_types=True)
            column_names = [col[0] for col in columns]
            return pd.DataFrame(result, columns=column_names)
        except ClickHouseError as e:
            raise Exception(f"Query execution error: {str(e)}")

    def create_table_from_df(self, df, table_name, primary_key=None):
        if not self.client:
            self.connect()

        try:
            # Generate CREATE TABLE statement
            columns = []
            for col, dtype in df.dtypes.items():
                ch_type = self._map_pandas_to_clickhouse(dtype)
                columns.append(f"{col} {ch_type}")

            create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            create_stmt += ", ".join(columns)

            if primary_key:
                create_stmt += f", PRIMARY KEY ({primary_key})"

            create_stmt += ") ENGINE = MergeTree()"
            if primary_key:
                create_stmt += f" ORDER BY {primary_key}"

            self.client.execute(create_stmt)
            return True
        except ClickHouseError as e:
            raise Exception(f"Table creation error: {str(e)}")

    def insert_dataframe(self, df, table_name):
        if not self.client:
            self.connect()

        try:
            data = df.to_dict('records')
            columns = df.columns.tolist()

            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"
            self.client.execute(query, data)
            return len(data)
        except ClickHouseError as e:
            raise Exception(f"Data insertion error: {str(e)}")

    def _map_pandas_to_clickhouse(self, dtype):
        if dtype == 'int64':
            return 'Int64'
        elif dtype == 'float64':
            return 'Float64'
        elif dtype == 'bool':
            return 'UInt8'
        elif 'datetime' in str(dtype):
            return 'DateTime'
        else:
            return 'String'

    def get_join_preview(self, main_table, joins, columns, where_clause=None):
        if not self.client:
            self.connect()

        try:
            join_clause = f" FROM {main_table}"
            for join in joins:
                join_clause += f" {join['type']} JOIN {join['table']} ON {join['condition']}"

            query = f"SELECT {', '.join(columns)} {join_clause}"
            if where_clause:
                query += f" WHERE {where_clause}"

            query += " LIMIT 100"
            return self.execute_query(query)
        except ClickHouseError as e:
            raise Exception(f"Join query error: {str(e)}")
