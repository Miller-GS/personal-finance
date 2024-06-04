{{ config(materialized='table') }}

SELECT
    COALESCE(TO_DATE(data, 'DD/MM/YYYY'), dd.date) AS date,
    COALESCE(cdi.valor::NUMERIC/100,0) AS cdi_overnight,
    ROUND(public.MUL(COALESCE(cdi.valor::NUMERIC/100, 0) + 1) OVER (
        PARTITION BY 1
        ORDER BY COALESCE(TO_DATE(data, 'DD/MM/YYYY'), dd.date)
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ), 6) AS correction_factor
FROM
    {{ source('dw', 'dim_date') }} AS dd
LEFT JOIN
    {{ source('central_bank', 'central_bank_cdi_daily') }} AS cdi
        ON dd.date = TO_DATE(data, 'DD/MM/YYYY')
WHERE
    dd.date <= CURRENT_DATE