{{ config(materialized='table') }}

WITH stock_price_history AS (
    SELECT
        ticker,
        date,
        close_value_raw_brl,
        close_value_split_adj_brl,
        close_value_split_div_adj_brl,
        LEAD(date) OVER(PARTITION BY ticker ORDER BY DATE) AS next_market_open_date
    FROM
        {{ ref('yfinance__staging__stock_price_history') }} AS sph
)
SELECT
    sph.ticker,
    dd.date,
    sph.date AS last_market_close_date,
    sph.close_value_raw_brl,
    close_value_split_adj_brl,
    close_value_split_div_adj_brl
FROM
    stock_price_history AS sph
JOIN
    {{ source('dw', 'dim_date') }} AS dd
        ON dd.date >= sph.date
        AND dd.date < COALESCE(sph.next_market_open_date, NOW())
