{{ config(materialized='table') }}

WITH sorted_transactions AS (
    SELECT
        p.sk_product,
        ft.product,
        CASE
            -- We have to invert here because when we are credited what we were owned from the auction,
            -- we have the debit of that fraction of stock. The last part is what we're interested here.
            -- We do have it from the B3 data, however since the debit happens before the auction, we "lose" some data.
            WHEN ft.transaction_type = 'Leilão de Fração' THEN 'Debit'
            -- Similarly, when stocks are grouped, instead of treating it as a debit, we're going to treat it as a credit.
            -- This is because we're going to consider the average cost of the stock, and the groupment is a way to "sell" the stock.
            WHEN ft.transaction_type = 'Grupamento' THEN 'Debit'
            ELSE ft.debit_or_credit 
        END debit_or_credit,
        ft.number_of_units,
        ft.price_per_unit_brl,
        ft.total_price_brl,
        ft.transaction_type,
        ft.transaction_date,
        ROW_NUMBER() OVER(PARTITION BY p.sk_product ORDER BY ft.transaction_date) AS transaction_nr_for_product
    FROM
        {{ ref('b3__staging__financial_transactions') }} AS ft
    JOIN
        {{ ref('b3__staging__products') }} AS p
            ON ft.product = p.product
    WHERE
        transaction_type IN ('Transferência - Liquidação', 'Bonificação em Ativos', 'Leilão de Fração', 'Grupamento')
),
transactions_with_avg_cost AS (
    WITH RECURSIVE prev_row AS (
        SELECT
            sk_product,
            product,
            debit_or_credit,
            transaction_type,
            number_of_units AS delta_units,
            price_per_unit_brl,
            total_price_brl,
            (CASE WHEN debit_or_credit = 'Credit' THEN 1 ELSE -1 END) * number_of_units AS acc_units,
            (CASE WHEN debit_or_credit = 'Credit' THEN 1 END) * total_price_brl AS acc_total_cost_brl,
            NULL::NUMERIC AS previous_acc_avg_cost_brl,
            transaction_date,
            transaction_nr_for_product
        FROM
            sorted_transactions
        WHERE
            transaction_nr_for_product = 1
        UNION ALL
        SELECT
            st.sk_product,
            st.product,
            st.debit_or_credit,
            st.transaction_type,
            CASE
                -- When it's a grouping, B3 gives us the final number of units instead of how many were "reduced"
                -- Therefore, to get the actual reduction, we have to subtract the previous number of units from this
                WHEN st.transaction_type = 'Grupamento' THEN st.number_of_units - p.acc_units
                WHEN st.debit_or_credit = 'Credit' THEN st.number_of_units
                ELSE - st.number_of_units
            END AS delta_units,
            st.price_per_unit_brl,
            st.total_price_brl,
            CASE
                -- When it's a grouping, B3 gives us the final number of units instead of how many were "reduced"
                WHEN st.transaction_type = 'Grupamento' THEN st.number_of_units
                -- Otherwise, it gives us the "delta", so we need to add or subtract accordingly
                WHEN st.debit_or_credit = 'Credit' THEN p.acc_units + st.number_of_units
                ELSE p.acc_units - st.number_of_units
            END AS acc_units,
            (
                p.acc_total_cost_brl + (
                    CASE
                        WHEN st.transaction_type = 'Grupamento' OR st.debit_or_credit = 'Credit' THEN COALESCE(st.total_price_brl, 0)
                        ELSE COALESCE(-st.number_of_units * p.acc_total_cost_brl / NULLIF(p.acc_units, 0), 0)
                    END
                )
            )::NUMERIC AS acc_total_cost_brl,
            p.acc_total_cost_brl / p.acc_units AS previous_acc_avg_cost_brl,
            st.transaction_date,
            st.transaction_nr_for_product
        FROM
            prev_row AS p
        JOIN
            sorted_transactions AS st
                ON p.sk_product = st.sk_product
                AND p.transaction_nr_for_product + 1 = st.transaction_nr_for_product
    )
    SELECT
        sk_product,
        product,
        debit_or_credit,
        transaction_type,
        delta_units,
        price_per_unit_brl,
        total_price_brl,
        ROUND(
            CASE
                WHEN debit_or_credit = 'Debit'
                    THEN total_price_brl + previous_acc_avg_cost_brl * delta_units
            END,
            3
        ) AS net_profit,
        ROUND(acc_units, 3) AS acc_units,
        ROUND(acc_total_cost_brl, 3) AS acc_total_cost_brl,
        ROUND(COALESCE(acc_total_cost_brl / NULLIF(acc_units, 0), previous_acc_avg_cost_brl), 3) AS acc_avg_cost_brl,
        transaction_nr_for_product,
        transaction_date,
        LEAD(transaction_date) OVER(PARTITION BY sk_product ORDER BY transaction_date) AS next_transaction_date_for_same_product
    FROM
        prev_row
)
SELECT *
FROM
    transactions_with_avg_cost