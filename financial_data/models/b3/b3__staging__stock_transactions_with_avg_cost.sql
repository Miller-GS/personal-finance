{{ config(materialized='table') }}

WITH sorted_transactions AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY product ORDER BY transaction_date) AS rw
    FROM
        {{ ref('b3__staging__financial_transactions') }}
    WHERE
        transaction_type = 'Transferência - Liquidação'
),
transactions_with_avg_cost AS (
    WITH RECURSIVE prev_row AS (
        SELECT
            product,
            debit_or_credit,
            number_of_units,
            price_per_unit_brl,
            total_price_brl,
            (CASE WHEN debit_or_credit = 'Credit' THEN 1 ELSE -1 END) * number_of_units AS acc_units,
            (CASE WHEN debit_or_credit = 'Credit' THEN 1 END) * total_price_brl AS acc_total_cost_brl,
            (CASE WHEN debit_or_credit = 'Credit' THEN 1 END) * total_price_brl / number_of_units AS acc_avg_cost_brl,
            transaction_date,
            rw
        FROM
            sorted_transactions
        WHERE
            rw = 1
        UNION ALL
        SELECT
            st.product,
            st.debit_or_credit,
            st.number_of_units,
            st.price_per_unit_brl,
            st.total_price_brl,
            p.acc_units + (CASE WHEN st.debit_or_credit = 'Credit' THEN 1 ELSE -1 END) * st.number_of_units AS acc_units,
            (p.acc_total_cost_brl + (CASE WHEN st.debit_or_credit = 'Credit' THEN st.total_price_brl ELSE - st.number_of_units * p.acc_avg_cost_brl END))::NUMERIC AS acc_total_cost_brl,
            (
                (p.acc_total_cost_brl + (CASE WHEN st.debit_or_credit = 'Credit' THEN st.total_price_brl ELSE - st.number_of_units * p.acc_avg_cost_brl END))
                /(p.acc_units + (CASE WHEN st.debit_or_credit = 'Credit' THEN 1 ELSE -1 END) * st.number_of_units)
            ) AS acc_avg_cost_brl,
            st.transaction_date,
            st.rw
        FROM
            prev_row AS p
        JOIN
            sorted_transactions AS st
                ON p.product = st.product
                AND p.rw + 1 = st.rw
    )
    SELECT
        product,
        debit_or_credit,
        number_of_units,
        price_per_unit_brl,
        total_price_brl,
        CASE WHEN debit_or_credit = 'Debit' THEN total_price_brl - acc_avg_cost_brl * number_of_units END AS net_profit,
        acc_units,
        acc_total_cost_brl,
        acc_avg_cost_brl,
        transaction_date
    FROM
        prev_row
)
SELECT *
FROM
    transactions_with_avg_cost