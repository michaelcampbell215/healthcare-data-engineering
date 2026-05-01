WITH source AS (
    SELECT * FROM {{ ref('stg_general_payments') }}
),
unpivoted_products AS(
    SELECT
        recipient_profile_id, 
        teaching_hospital_ccn,
        amount_usd, 
        number_of_payments,
        record_id,
        subsidiary_id,
        payment_nature,
        payment_form,
        payment_date,
        program_year,
        recipient_type,
        dispute_status,
        physician_ownership_indicator,
        charity_indicator,
        related_product_indicator,
        product_name_1              AS product_name, 
        product_category_1          AS product_category, 
        product_type_1              AS product_type, 
        product_ndc_1               AS product_ndc,
        1                           AS product_slot
    FROM source
    WHERE product_name_1 IS NOT NULL
        AND amount_usd IS NOT NULL

    UNION ALL

    SELECT
        recipient_profile_id, 
        teaching_hospital_ccn,
        amount_usd, 
        number_of_payments,
        record_id,
        subsidiary_id,
        payment_nature,
        payment_form,
        payment_date,
        program_year,
        recipient_type,
        dispute_status,
        physician_ownership_indicator,
        charity_indicator,
        related_product_indicator,
        product_name_2              AS product_name, 
        product_category_2          AS product_category, 
        product_type_2              AS product_type, 
        product_ndc_2               AS product_ndc,
        2                           AS product_slot
    FROM source
    WHERE product_name_2 IS NOT NULL
        AND amount_usd IS NOT NULL
    UNION ALL

    SELECT
        recipient_profile_id, 
        teaching_hospital_ccn,
        amount_usd, 
        number_of_payments,
        record_id,
        subsidiary_id,
        payment_nature,
        payment_form,
        payment_date,
        program_year,
        recipient_type,
        dispute_status,
        physician_ownership_indicator,
        charity_indicator,
        related_product_indicator,
        product_name_3              AS product_name, 
        product_category_3          AS product_category, 
        product_type_3              AS product_type, 
        product_ndc_3               AS product_ndc,
        3                           AS product_slot
    FROM source
    WHERE product_name_3 IS NOT NULL
        AND amount_usd IS NOT NULL

    UNION ALL

    SELECT        
        recipient_profile_id, 
        teaching_hospital_ccn,
        amount_usd, 
        number_of_payments,
        record_id,
        subsidiary_id,
        payment_nature,
        payment_form,
        payment_date,
        program_year,
        recipient_type,
        dispute_status,
        physician_ownership_indicator,
        charity_indicator,
        related_product_indicator,
        product_name_4              AS product_name, 
        product_category_4          AS product_category, 
        product_type_4              AS product_type, 
        product_ndc_4               AS product_ndc,
        4                           AS product_slot
    FROM source
    WHERE product_name_4 IS NOT NULL
        AND amount_usd IS NOT NULL

    UNION ALL

    SELECT
        recipient_profile_id, 
        teaching_hospital_ccn,
        amount_usd, 
        number_of_payments,
        record_id,
        subsidiary_id,
        payment_nature,
        payment_form,
        payment_date,
        program_year,
        recipient_type,
        dispute_status,
        physician_ownership_indicator,
        charity_indicator,
        related_product_indicator,
        product_name_5              AS product_name, 
        product_category_5          AS product_category, 
        product_type_5              AS product_type, 
        product_ndc_5               AS product_ndc,
        5                           AS product_slot
    FROM source
    WHERE product_name_5 IS NOT NULL
        AND amount_usd IS NOT NULL
    
),
fct_payments AS(
    SELECT
        {{ dbt_utils.generate_surrogate_key(['record_id', 'product_slot']) }} AS payment_id,
        *
    FROM unpivoted_products
)

SELECT * FROM fct_payments

