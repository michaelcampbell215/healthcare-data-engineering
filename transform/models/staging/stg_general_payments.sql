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
        NULLIF(INITCAP(TRIM(covered_recipient_first_name)), '') AS recipient_first_name,
        NULLIF(INITCAP(TRIM(covered_recipient_middle_name)), '') AS recipient_middle_name,
        NULLIF(INITCAP(TRIM(covered_recipient_last_name)), '') AS recipient_last_name,
        NULLIF(INITCAP(TRIM(covered_recipient_name_suffix)), '') AS recipient_name_suffix,
        
        -- Primary specialty: last pipe segment from specialty_1 (most specific designation)
        NULLIF(TRIM(REGEXP_EXTRACT(NULLIF(TRIM(covered_recipient_specialty_1), ''), r'[^|]+$')), '') AS recipient_primary_specialty,
        -- All specialties: extract last pipe segment from each column, then join with |
        ARRAY_TO_STRING(
            ARRAY(
                SELECT cleaned
                FROM UNNEST([
                    NULLIF(TRIM(REGEXP_EXTRACT(NULLIF(TRIM(covered_recipient_specialty_1), ''), r'[^|]+$')), ''),
                    NULLIF(TRIM(REGEXP_EXTRACT(NULLIF(TRIM(covered_recipient_specialty_2), ''), r'[^|]+$')), ''),
                    NULLIF(TRIM(REGEXP_EXTRACT(NULLIF(TRIM(covered_recipient_specialty_3), ''), r'[^|]+$')), ''),
                    NULLIF(TRIM(REGEXP_EXTRACT(NULLIF(TRIM(covered_recipient_specialty_4), ''), r'[^|]+$')), ''),
                    NULLIF(TRIM(REGEXP_EXTRACT(NULLIF(TRIM(covered_recipient_specialty_5), ''), r'[^|]+$')), ''),
                    NULLIF(TRIM(REGEXP_EXTRACT(NULLIF(TRIM(covered_recipient_specialty_6), ''), r'[^|]+$')), '')
                ]) AS cleaned
                WHERE cleaned IS NOT NULL
            ),
            '|'
        ) AS recipient_specialty,
        
        COALESCE(NULLIF(covered_recipient_license_state_code1, ''), NULLIF(covered_recipient_license_state_code2, '')) AS recipient_license_state,
        
        -- Recipient (Hospital) Details
        teaching_hospital_ccn,
        TRIM(teaching_hospital_name) AS teaching_hospital_name,
        
        -- Geography
        TRIM(recipient_primary_business_street_address_line1) AS recipient_address_line1,
        TRIM(recipient_primary_business_street_address_line2) AS recipient_address_line2,
        NULLIF(INITCAP(TRIM(recipient_city)), '') AS recipient_city,
        -- CMS data quality: state codes and zip codes are sometimes routed to province/postal_code.
        -- Logic mirrors Phase 1 BatchCleanProvince — only triggers when province is non-empty on US records.
        CASE
            WHEN TRIM(recipient_country) IN ('United States', 'United States Minor Outlying Islands')
                AND NULLIF(TRIM(recipient_province), '') IS NOT NULL
            THEN CASE
                WHEN UPPER(TRIM(recipient_city)) = 'SAN JUAN'                                                THEN 'PR'
                WHEN UPPER(TRIM(recipient_city)) = 'CHICAGO'                                                 THEN 'IL'
                WHEN UPPER(TRIM(recipient_city)) = 'BLACKFOOT'                                               THEN 'ID'
                WHEN UPPER(TRIM(recipient_city)) = 'MEMPHIS'                                                 THEN 'TN'
                WHEN UPPER(TRIM(recipient_city)) = 'DEDEDO'                                                  THEN 'GU'
                WHEN UPPER(TRIM(recipient_city)) = 'BROOKLYN'                                                THEN 'NY'
                WHEN UPPER(TRIM(recipient_city)) IN ('ENCINITAS', 'SAN MATEO')                               THEN 'CA'
                WHEN UPPER(TRIM(recipient_city)) IN ('WEST PALM BEACH', 'JACKSONVILLE', 'FORT LAUDERDALE')   THEN 'FL'
                WHEN UPPER(TRIM(recipient_city)) IN ('SUGARLAND', 'SPRING', 'TE', 'FORT WORTH', 'MCALLEN')   THEN 'TX'
                WHEN UPPER(TRIM(recipient_province)) IN ('CA','DE','FL','GA','KY','LA','MA','MD','NY','OK','PA','PR','TN','TX','WA')
                    THEN UPPER(TRIM(recipient_province))
                ELSE NULLIF(TRIM(recipient_state), '')
            END
            ELSE NULLIF(TRIM(recipient_state), '')
        END AS recipient_state,
        -- Use recipient_postal_code as zip only for the specific province values Phase 1 identified as misrouted.
        CASE
            WHEN TRIM(recipient_country) IN ('United States', 'United States Minor Outlying Islands')
                AND NULLIF(TRIM(recipient_province), '') IS NOT NULL
                AND UPPER(TRIM(recipient_province)) IN (
                    'CA','DE','FL','GA','KY','LA','MA','MD','NY','OK','PA','PR','TE','TN','TX','WA',
                    'GUAM','PUERTO RICO','FORT BEND','TEXAS','FLORIDA','TENNESSEE','ILLINOIS',
                    'SAN MATEO','USA','FLORIDA FL','SAN DIEGO','HIDALGO COUNTY','IDAHO'
                )
            THEN NULLIF(REGEXP_EXTRACT(TRIM(recipient_postal_code), r'^\d{5}'), '')
            ELSE NULLIF(REGEXP_EXTRACT(TRIM(recipient_zip_code),    r'^\d{5}'), '')
        END AS recipient_zip,
        recipient_country,
        -- Province is meaningful only for non-US records; null it for US records where it held misrouted state/zip data.
        CASE
            WHEN TRIM(recipient_country) IN ('United States', 'United States Minor Outlying Islands')
                AND NULLIF(TRIM(recipient_province), '') IS NOT NULL
            THEN NULL
            ELSE NULLIF(TRIM(recipient_province), '')
        END AS recipient_province,
        
        -- Manufacturer/Payer
        submitting_applicable_mfr_or_gpo_name AS payer_name,
        applicable_mfr_or_gpo_making_payment_id AS subsidiary_id,
        applicable_mfr_or_gpo_making_payment_name AS subsidiary_name,
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
        NULLIF(INITCAP(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(name_of_drug_or_biological_or_device_or_supply_1, r'\(R\)|\(TM\)', ''), r'\s+', ' '))), '') AS product_name_1,
        NULLIF(INITCAP(TRIM(product_category_or_therapeutic_area_1)), '') AS product_category_1,
        associated_drug_or_biological_ndc_1 AS product_ndc_1,

        indicate_drug_or_biological_or_device_or_supply_2 AS product_type_2,
        NULLIF(INITCAP(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(name_of_drug_or_biological_or_device_or_supply_2, r'\(R\)|\(TM\)', ''), r'\s+', ' '))), '') AS product_name_2,
        NULLIF(INITCAP(TRIM(product_category_or_therapeutic_area_2)), '') AS product_category_2,
        associated_drug_or_biological_ndc_2 AS product_ndc_2,

        indicate_drug_or_biological_or_device_or_supply_3 AS product_type_3,
        NULLIF(INITCAP(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(name_of_drug_or_biological_or_device_or_supply_3, r'\(R\)|\(TM\)', ''), r'\s+', ' '))), '') AS product_name_3,
        NULLIF(INITCAP(TRIM(product_category_or_therapeutic_area_3)), '') AS product_category_3,
        associated_drug_or_biological_ndc_3 AS product_ndc_3,

        indicate_drug_or_biological_or_device_or_supply_4 AS product_type_4,
        NULLIF(INITCAP(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(name_of_drug_or_biological_or_device_or_supply_4, r'\(R\)|\(TM\)', ''), r'\s+', ' '))), '') AS product_name_4,
        NULLIF(INITCAP(TRIM(product_category_or_therapeutic_area_4)), '') AS product_category_4,
        associated_drug_or_biological_ndc_4 AS product_ndc_4,

        indicate_drug_or_biological_or_device_or_supply_5 AS product_type_5,
        NULLIF(INITCAP(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(name_of_drug_or_biological_or_device_or_supply_5, r'\(R\)|\(TM\)', ''), r'\s+', ' '))), '') AS product_name_5,
        NULLIF(INITCAP(TRIM(product_category_or_therapeutic_area_5)), '') AS product_category_5,
        associated_drug_or_biological_ndc_5 AS product_ndc_5

    FROM source
),

cleaned AS (
    SELECT * REPLACE (
        CASE
            WHEN payment_date < '2013-01-01'
            THEN DATE(program_year, EXTRACT(MONTH FROM payment_date), EXTRACT(DAY FROM payment_date))
            ELSE payment_date
        END AS payment_date
    )
    FROM staged
)

SELECT * FROM cleaned
WHERE record_id != 'No'
    AND (program_year IS NULL OR program_year >= 2000)
    AND (payer_name IS NULL OR NOT REGEXP_CONTAINS(payer_name, r'^\d+'))
