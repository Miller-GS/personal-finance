import dotenv
import sqlalchemy
import pandas

class PostgresConnector:
    CONN_STRING_TEMPLATE = "postgresql://{user}:{password}@{hostname}:{port}/{database}" 

    def __init__(self, host: str, database: str, user: str, password: str, port: str) -> None:
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection_string = PostgresConnector.CONN_STRING_TEMPLATE.format(
            user=self.user,
            password=self.password,
            hostname=self.host,
            port=self.port,
            database=self.database
        )
        self.engine = sqlalchemy.create_engine(self.connection_string)
        self.is_connected = False

    def open(self) -> None:
        self.connection = self.engine.connect()
        self.is_connected = True

    def close(self) -> None:
        self.connection.close()
        self.is_connected = False

    def __enter__(self) -> 'PostgresConnector':
        self.open()
        return self
    
    def __exit__(self, *_) -> None:
        self.close()

    def assert_connected(self) -> None:
        if not self.is_connected:
            raise ConnectionError('Not connected to database')

    def execute(self, query: str) -> sqlalchemy.Sequence:
        self.assert_connected()

        return self.connection.execute(sqlalchemy.text(query)).fetchall()
    
    def save_dataframe(self, dataframe: pandas.DataFrame, table_name: str, schema: str = 'public', if_exists: str = 'fail') -> None:
        self.assert_connected()

        self.connection.execute(sqlalchemy.schema.CreateSchema("raw", if_not_exists=True))
        dataframe.to_sql(
            table_name,
            self.connection,
            schema=schema,
            if_exists=if_exists,
            index=False
        )
        self.connection.commit()
    
    @staticmethod
    def create_from_dot_env_file(dotenv_file: str):
        config = dotenv.dotenv_values(dotenv_file)
        return PostgresConnector(
            host=config['POSTGRES_HOSTNAME'],
            database=config['POSTGRES_DB'],
            user=config['POSTGRES_USER'],
            password=config['POSTGRES_PASSWORD'],
            port=config['POSTGRES_PORT']
        )
