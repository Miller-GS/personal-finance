{{ config(materialized='table') }}

WITH all_products AS (
    SELECT
        CASE
            WHEN product SIMILAR TO '[A-Z]{4}[0-9]{1,2} - %' THEN SUBSTRING(product, '([A-Z]{4}[0-9]{1,2}) - ')
            WHEN product LIKE 'CDB - %' THEN SUBSTRING(product, 'CDB - (CDB\w+) ')
            ELSE product
        END AS product_code,
        product,
        CASE
            WHEN product SIMILAR TO '[A-Z]{4}[0-9]{1,2} - %' THEN 'Stock'
            WHEN product LIKE 'CDB - %' THEN 'CDB'
            WHEN product LIKE 'Tesouro %' THEN 'Bond'
            ELSE 'Unknown'
        END AS product_type,
        MIN(transaction_date) AS first_appearance_date
    FROM
        {{ ref('b3__staging__financial_transactions') }}
    GROUP BY 1,2,3
),
synonyms AS (
    SELECT
        '(CARD)(\d+)' AS old,
        'CSUD\2' AS new
),
products_with_synonyms AS (
    SELECT
        COALESCE(REGEXP_REPLACE(ap.product_code, s.old, s.new), ap.product_code) AS product_code,
        ap.product,
        ap.product_type,
        ap.first_appearance_date
    FROM
        all_products AS ap
    LEFT JOIN
        synonyms AS s
            ON ap.product_code SIMILAR TO s.old
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['product_code', 'product_type']) }} AS sk_product,
    product_code,
    product,
    FIRST_VALUE(product) OVER(PARTITION BY product_code, product_type ORDER BY first_appearance_date DESC) AS latest_product_name,
    product_type,
    first_appearance_date
FROM
    products_with_synonyms