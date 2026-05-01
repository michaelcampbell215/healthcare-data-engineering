SELECT DISTINCT
    payment_nature,
    payment_form
FROM {{ ref('stg_general_payments') }}
WHERE payment_nature IS NOT NULL
    AND payment_nature != ''
