{{ config(materialized='table') }}

WITH all_transactions_daily AS (
    SELECT
        t.sk_product,
        p.product_code AS current_product_code,
        dd.sk_date,
        t.acc_units,
        t.acc_total_cost_brl,
        t.acc_avg_cost_brl,
        ROW_NUMBER() OVER(PARTITION BY t.sk_product, dd.sk_date ORDER BY t.transaction_nr_for_product DESC) = 1 AS is_last_transaction_of_day,
        dd.date
    FROM
        {{ ref('b3__staging__stock_transactions_with_avg_cost') }} AS t
    JOIN
        {{ ref('b3__staging__products') }} AS p
            ON t.product = p.product
    JOIN
        {{ source('dw', 'dim_date') }} AS dd
            ON dd.date BETWEEN t.transaction_date AND COALESCE(t.next_transaction_date_for_same_product, CURRENT_DATE)
)
SELECT
    sk_product,
    sk_date,
    acc_units,
    acc_total_cost_brl,
    acc_avg_cost_brl,
    close_value_raw_brl * acc_units AS total_market_value_brl,
    close_value_raw_brl AS market_value_per_unit_brl
FROM
    all_transactions_daily AS atd
LEFT JOIN
    {{ ref('yfinance__staging__daily_stock_close_price') }} AS sp
        ON atd.current_product_code = sp.ticker
        AND atd.date = sp.date
WHERE
    is_last_transaction_of_day
    AND acc_units != 0
