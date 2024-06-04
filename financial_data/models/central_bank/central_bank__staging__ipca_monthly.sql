{{ config(materialized='table') }}

SELECT
    TO_DATE(data, 'DD/MM/YYYY') AS date,
    COALESCE(ipca.valor::NUMERIC/100,0) AS ipca_monthly,
    ROUND(public.MUL(COALESCE(ipca.valor::NUMERIC/100, 0) + 1) OVER (
        PARTITION BY 1
        ORDER BY TO_DATE(data, 'DD/MM/YYYY')
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ), 6) AS correction_factor
FROM
    {{ source('central_bank', 'central_bank_ipca_monthly') }} AS ipca
