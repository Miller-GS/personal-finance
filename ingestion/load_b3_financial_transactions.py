import pandas as pd
from connectors.postgres_connector import PostgresConnector

POSTGRES_ENV_FILE = '../local_env/postgres/.env'
FINANCIAL_TRANSACTIONS_FILE = "../source_data/b3_financial_transactions/movimentacao-2024-06-01-21-31-58.xlsx"

df_financial_transactions = pd.read_excel(FINANCIAL_TRANSACTIONS_FILE)
postgres_connector = PostgresConnector.create_from_dot_env_file(POSTGRES_ENV_FILE)
with postgres_connector:
    postgres_connector.save_dataframe(df_financial_transactions, 'financial_transactions', 'raw', 'replace')