{{ config(materialized='table') }}

SELECT
    sk_product,
    ANY_VALUE(latest_product_name) AS latest_product_name,
    ARRAY_AGG(product) AS all_product_names,
    ANY_VALUE(product_type) AS product_type,
    MIN(first_appearance_date) AS first_appearance_date
FROM
    {{ ref('b3__staging__products') }}
GROUP BY
    sk_product