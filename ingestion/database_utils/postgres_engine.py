import dotenv
from sqlalchemy import create_engine

class PostgresEngineCreator:
    CONN_STRING_TEMPLATE = "postgres://{user}:{password}@{hostname}:{port}/{database}" 

    @staticmethod
    def create_from_dot_env_file(dotenv_file: str):
        config = dotenv.dotenv_values(dotenv_file)
        conn_string = PostgresEngineCreator.CONN_STRING_TEMPLATE.format(
            user=config['POSTGRES_USER'],
            password=config['POSTGRES_PASSWORD'],
            hostname=config['POSTGRES_HOSTNAME'],
            port=config['POSTGRES_PORT'],
            database=config['POSTGRES_DB']
        )
        return create_engine(conn_string)