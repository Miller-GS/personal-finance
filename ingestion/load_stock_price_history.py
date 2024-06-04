import re
import sys
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from connectors.postgres_connector import PostgresConnector
from readers.excel_dataframe_reader import ExcelDataFrameReader

FINANCIAL_TRANSACTIONS_FILE_PATTERN = "../source_data/b3_financial_transactions/movimentacao-*.xlsx"
DEFAULT_START_DATE = datetime(2018, 1, 1)
DEFAULT_END_DATE = datetime.now()

def main():
    start_date, end_date = parse_args()
    tickers = get_tickers_of_interest()
    df_stock_price_history = get_stock_price_history(tickers, start_date, end_date)
    connector = PostgresConnector.create_from_dot_env_file()
    with connector:
        connector.save_dataframe(df_stock_price_history, 'stock_price_history', 'raw', 'replace')

def parse_args() -> tuple[datetime, datetime]:
    if len(sys.argv) < 3:
        end_date = DEFAULT_END_DATE
    else:
        end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d')
    if len(sys.argv) == 1:
        start_date = DEFAULT_START_DATE
    else:
        start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')

    return start_date, end_date

def get_tickers_of_interest() -> set[str]:
    reader = ExcelDataFrameReader()
    df_financial_transactions = reader.read_with_glob_pattern(FINANCIAL_TRANSACTIONS_FILE_PATTERN)
    products = df_financial_transactions["Produto"].unique()
    # Extracting tickers from products
    # e.g., TAEE11 - TRANSMISSORA ALIANCA DE ENERGIA ELETRICA S/A -> TAEE11
    tickers = set()
    for product in products:
        match = re.match(r"^([A-Z]{4}\d{1,2}) -", product)
        if match:
            tickers.add(match.group(1))

    print(f"Found {len(tickers)} tickers of interest: {tickers}")

    return tickers

def get_stock_price_history(tickers: set[str], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    dfs = []
    for ticker in tickers:
        print(f"Fetching stock price history for {ticker}")
        df = get_stock_price_history_from_ticker(ticker, start_date, end_date)
        if df is not None:
            dfs.append(df)
    return pd.concat(dfs)

def get_stock_price_history_from_ticker(ticker_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    ticker_obj = yf.Ticker(f"{ticker_name}.SA")
    history = ticker_obj.history(start=start_date, end=end_date, auto_adjust=False)
    if history.empty:
        return None
    history.sort_index(inplace=True, ascending=False)
    history["split_factor_acc"] = history["Stock Splits"].replace(0, 1).cumprod().fillna(1)
    history.reset_index(inplace=True)
    history["ticker"] = ticker_name
    return history

if __name__ == '__main__':
    main()