from connectors.postgres_connector import PostgresConnector
from readers.excel_dataframe_reader import ExcelDataFrameReader
from . import POSTGRES_ENV_FILE 

FINANCIAL_TRANSACTIONS_FILE_PATTERN = "../source_data/b3_financial_transactions/movimentacao-*.xlsx"

reader = ExcelDataFrameReader()
df_financial_transactions = reader.read_with_glob_pattern(FINANCIAL_TRANSACTIONS_FILE_PATTERN)

postgres_connector = PostgresConnector.create_from_dot_env_file(POSTGRES_ENV_FILE)
with postgres_connector:
    postgres_connector.save_dataframe(df_financial_transactions, 'b3_financial_transactions', 'raw', 'replace')