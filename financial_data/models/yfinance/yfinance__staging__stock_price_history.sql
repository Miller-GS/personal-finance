{{ config(materialized='table') }}

SELECT
    ticker,
    DATE("Date") AS date,
    ROUND("Open"::NUMERIC, 2) AS open_value_brl,
    ROUND("High"::NUMERIC, 2) AS high_value_brl,
    ROUND("Low"::NUMERIC, 2) AS low_value_brl,
    ROUND("Close"::NUMERIC, 2) * split_factor_acc AS close_value_raw_brl,
    ROUND("Close"::NUMERIC, 2) AS close_value_split_adj_brl,
    ROUND("Adj Close"::NUMERIC, 2) AS close_value_split_div_adj_brl,
    "Volume" AS volume,
    "Dividends" AS dividends,
    "Stock Splits" AS stock_splits,
    split_factor_acc,
    ROUND("Close"::NUMERIC, 2)/ROUND("Adj Close"::NUMERIC, 2) AS div_factor_acc,
    split_factor_acc * ROUND("Close"::NUMERIC, 2)/ROUND("Adj Close"::NUMERIC, 2) AS total_adj_factor_acc
FROM
     {{ source('yfinance', 'stock_price_history') }}
