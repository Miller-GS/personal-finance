{{ config(materialized='table') }}

SELECT
    CASE "Entrada/Saída"
        WHEN 'Credito' THEN 'Credit'
        WHEN 'Debito' THEN 'Debit'
        ELSE "Entrada/Saída"
    END AS debit_or_credit,
    "Movimentação" AS transaction_type,
    "Produto" AS product,
    "Instituição" AS financial_institution,
    CASE
        -- Apparently B3 is sometimes rounding the number of units in fractional auctions. We need to fix it here.
        WHEN "Movimentação" = 'Leilão de Fração' THEN ROUND("Valor da Operação"::NUMERIC/"Preço unitário"::NUMERIC, 2)
        ELSE "Quantidade"::NUMERIC
    END AS number_of_units,
    NULLIF("Preço unitário", '-')::NUMERIC AS price_per_unit_brl,
    NULLIF("Valor da Operação", '-')::NUMERIC AS total_price_brl,
    TO_DATE("Data", 'DD/MM/YYYY') AS transaction_date
FROM
    {{ source('b3', 'b3_financial_transactions') }}
