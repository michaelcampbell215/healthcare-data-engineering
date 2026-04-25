{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('cms_open_payments', 'raw_general_payments') }}
), staged AS (
    SELECT 
        -- Core Record Info
        record_id,
        change_type,
        CAST(NULLIF(program_year, '') AS int64) AS program_year,
        
        -- Date Parsing 
        parse_date('%m/%d/%Y', NULLIF(date_of_payment, '')) AS payment_date,
        parse_date('%m/%d/%Y', NULLIF(payment_publication_date, '')) AS publication_date,
        
        -- Financials
        CAST(NULLIF(total_amount_of_payment_usdollars, '') AS NUMERIC) AS amount_usd,
        CAST(NULLIF(number_of_payments_included_in_total_amount, '') AS int64) AS number_of_payments,
        
        -- Payment Categorization
        TRIM(nature_of_payment_or_transfer_of_value) AS payment_nature,
        TRIM(form_of_payment_or_transfer_of_value) AS payment_form,
        
        -- Recipient (Physician) Details
        covered_recipient_profile_id AS recipient_profile_id,
        covered_recipient_npi AS recipient_npi,
        covered_recipient_type AS recipient_type,
        TRIM(covered_recipient_first_name) AS recipient_first_name,
        TRIM(covered_recipient_middle_name) AS recipient_middle_name,
        TRIM(covered_recipient_lASt_name) AS recipient_lASt_name,
        TRIM(covered_recipient_name_suffix) AS recipient_name_suffix,
        
        -- Specialty Array (BigQuery syntax for CONCAT_WS)
        ARRAY_TO_STRING(
            [NULLIF(covered_recipient_specialty_1, ''),
             NULLIF(covered_recipient_specialty_2, ''),
             NULLIF(covered_recipient_specialty_3, ''),
             NULLIF(covered_recipient_specialty_4, ''),
             NULLIF(covered_recipient_specialty_5, ''),
             NULLIF(covered_recipient_specialty_6, '')],
            '|'
        ) AS recipient_specialty,
        
        COALESCE(NULLIF(covered_recipient_license_state_code1, ''), NULLIF(covered_recipient_license_state_code2, '')) AS recipient_license_state,
        
        -- Recipient (Hospital) Details
        teaching_hospital_ccn,
        TRIM(teaching_hospital_name) AS teaching_hospital_name,
        
        -- Geography
        TRIM(recipient_primary_business_street_address_line1) AS recipient_address_line1,
        TRIM(recipient_primary_business_street_address_line2) AS recipient_address_line2,
        TRIM(recipient_city) AS recipient_city,
        TRIM(recipient_state) AS recipient_state,
        recipient_zip_code AS recipient_zip, 
        recipient_country,
        recipient_province,
        
        -- Manufacturer/Payer
        submitting_applicable_mfr_or_gpo_name AS payer_name,
        applicable_mfr_or_gpo_making_payment_id AS payer_id,
        applicable_mfr_or_gpo_making_payment_name AS subsidiary_name,
        applicable_mfr_or_gpo_making_payment_id AS subsidiary_id,
        applicable_mfr_or_gpo_making_payment_state AS subsidiary_state,
        applicable_mfr_or_gpo_making_payment_country AS subsidiary_country,
        
        -- Context & Risk Indicators
        name_of_third_party_entity_receiving_payment AS name_of_third_party_entity,
        physician_ownership_indicator,
        third_party_payment_recipient_indicator,
        charity_indicator,
        related_product_indicator,
        dispute_status_for_publication AS dispute_status,
        contextual_information,
        
        -- Travel
        city_of_travel AS travel_city,
        state_of_travel AS travel_state,
        country_of_travel AS travel_country,
        
        -- Products
        indicate_drug_or_biological_or_device_or_supply_1 AS product_type_1,
        TRIM(name_of_drug_or_biological_or_device_or_supply_1) AS product_name_1,
        TRIM(product_category_or_therapeutic_area_1) AS product_category_1,
        ASsociated_drug_or_biological_ndc_1 AS product_ndc_1,
        
        indicate_drug_or_biological_or_device_or_supply_2 AS product_type_2,
        TRIM(name_of_drug_or_biological_or_device_or_supply_2) AS product_name_2,
        TRIM(product_category_or_therapeutic_area_2) AS product_category_2,
        ASsociated_drug_or_biological_ndc_2 AS product_ndc_2,
        
        indicate_drug_or_biological_or_device_or_supply_3 AS product_type_3,
        TRIM(name_of_drug_or_biological_or_device_or_supply_3) AS product_name_3,
        TRIM(product_category_or_therapeutic_area_3) AS product_category_3,
        ASsociated_drug_or_biological_ndc_3 AS product_ndc_3,
        
        indicate_drug_or_biological_or_device_or_supply_4 AS product_type_4,
        TRIM(name_of_drug_or_biological_or_device_or_supply_4) AS product_name_4,
        TRIM(product_category_or_therapeutic_area_4) AS product_category_4,
        ASsociated_drug_or_biological_ndc_4 AS product_ndc_4,
        
        indicate_drug_or_biological_or_device_or_supply_5 AS product_type_5,
        TRIM(name_of_drug_or_biological_or_device_or_supply_5) AS product_name_5,
        TRIM(product_category_or_therapeutic_area_5) AS product_category_5,
        ASsociated_drug_or_biological_ndc_5 AS product_ndc_5

    FROM source
)

SELECT * FROM staged
