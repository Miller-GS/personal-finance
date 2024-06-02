import psycopg2
import dotenv

class PostgresConnector:
    def __init__(self, host, database, user, password, port):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None
        self.is_connected = False

    def open(self):
        self.connection = psycopg2.connect(host=self.host, database=self.database, user=self.user, password=self.password, port=self.port)
        self.cursor = self.connection.cursor()
        self.is_connected = True

    def close(self):
        self.cursor.close()
        self.connection.close()
        self.is_connected = False

    def __enter__(self):
        self.open()
        return self
    
    def __exit__(self, *_):
        self.close()

    def execute(self, query: str) -> list:
        if not self.is_connected:
            raise Exception('Not connected to database')

        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    @staticmethod
    def create_from_dot_env_file(dotenv_file: str):
        config = dotenv.dotenv_values(dotenv_file)
        return PostgresConnector(
            host=config['POSTGRES_HOST'],
            database=config['POSTGRES_DATABASE'],
            user=config['POSTGRES_USER'],
            password=config['POSTGRES_PASSWORD'],
            port=config['POSTGRES_PORT']
        )
