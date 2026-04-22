-- =========================================================================
-- Create Staging Table with 1:1 Column Mapping to CSV
-- =========================================================================

DROP TABLE IF EXISTS stg_general_payments;
CREATE TABLE stg_general_payments (
    change_type                                              TEXT,
    covered_recipient_type                                   TEXT,
    teaching_hospital_ccn                                    TEXT,
    teaching_hospital_id                                     TEXT,
    teaching_hospital_name                                   TEXT,
    covered_recipient_profile_id                             TEXT,
    covered_recipient_npi                                    TEXT,
    covered_recipient_first_name                             TEXT,
    covered_recipient_middle_name                            TEXT,
    covered_recipient_last_name                              TEXT,
    covered_recipient_name_suffix                            TEXT,
    recipient_primary_business_street_address_line1          TEXT,
    recipient_primary_business_street_address_line2          TEXT,
    recipient_city                                           TEXT,
    recipient_state                                          TEXT,
    recipient_zip_code                                       TEXT,
    recipient_country                                        TEXT,
    recipient_province                                       TEXT,
    recipient_postal_code                                    TEXT,
    covered_recipient_primary_type_1                         TEXT,
    covered_recipient_primary_type_2                         TEXT,
    covered_recipient_primary_type_3                         TEXT,
    covered_recipient_primary_type_4                         TEXT,
    covered_recipient_primary_type_5                         TEXT,
    covered_recipient_primary_type_6                         TEXT,
    covered_recipient_specialty_1                            TEXT,
    covered_recipient_specialty_2                            TEXT,
    covered_recipient_specialty_3                            TEXT,
    covered_recipient_specialty_4                            TEXT,
    covered_recipient_specialty_5                            TEXT,
    covered_recipient_specialty_6                            TEXT,
    covered_recipient_license_state_code1                    TEXT,
    covered_recipient_license_state_code2                    TEXT,
    covered_recipient_license_state_code3                    TEXT,
    covered_recipient_license_state_code4                    TEXT,
    covered_recipient_license_state_code5                    TEXT,
    submitting_applicable_mfr_or_gpo_name                    TEXT,
    applicable_mfr_or_gpo_making_payment_id                  TEXT,
    applicable_mfr_or_gpo_making_payment_name                TEXT,
    applicable_mfr_or_gpo_making_payment_state               TEXT,
    applicable_mfr_or_gpo_making_payment_country             TEXT,
    total_amount_of_payment_usdollars                        TEXT,
    date_of_payment                                          TEXT,
    number_of_payments_included_in_total_amount              TEXT,
    form_of_payment_or_transfer_of_value                     TEXT,
    nature_of_payment_or_transfer_of_value                   TEXT,
    city_of_travel                                           TEXT,
    state_of_travel                                          TEXT,
    country_of_travel                                        TEXT,
    physician_ownership_indicator                            TEXT,
    third_party_payment_recipient_indicator                  TEXT,
    name_of_third_party_entity_receiving_payment             TEXT,
    charity_indicator                                        TEXT,
    third_party_equals_covered_recipient_indicator           TEXT,
    contextual_information                                   TEXT,
    delay_in_publication_indicator                           TEXT,
    record_id                                                TEXT,
    dispute_status_for_publication                           TEXT,
    related_product_indicator                                TEXT,
    covered_or_noncovered_indicator_1                        TEXT,
    indicate_drug_or_biological_or_device_or_supply_1        TEXT,
    product_category_or_therapeutic_area_1                   TEXT,
    name_of_drug_or_biological_or_device_or_supply_1         TEXT,
    associated_drug_or_biological_ndc_1                      TEXT,
    associated_device_or_medical_supply_pdi_1                TEXT,
    covered_or_noncovered_indicator_2                        TEXT,
    indicate_drug_or_biological_or_device_or_supply_2        TEXT,
    product_category_or_therapeutic_area_2                   TEXT,
    name_of_drug_or_biological_or_device_or_supply_2         TEXT,
    associated_drug_or_biological_ndc_2                      TEXT,
    associated_device_or_medical_supply_pdi_2                TEXT,
    covered_or_noncovered_indicator_3                        TEXT,
    indicate_drug_or_biological_or_device_or_supply_3        TEXT,
    product_category_or_therapeutic_area_3                   TEXT,
    name_of_drug_or_biological_or_device_or_supply_3         TEXT,
    associated_drug_or_biological_ndc_3                      TEXT,
    associated_device_or_medical_supply_pdi_3                TEXT,
    covered_or_noncovered_indicator_4                        TEXT,
    indicate_drug_or_biological_or_device_or_supply_4        TEXT,
    product_category_or_therapeutic_area_4                   TEXT,
    name_of_drug_or_biological_or_device_or_supply_4         TEXT,
    associated_drug_or_biological_ndc_4                      TEXT,
    associated_device_or_medical_supply_pdi_4                TEXT,
    covered_or_noncovered_indicator_5                        TEXT,
    indicate_drug_or_biological_or_device_or_supply_5        TEXT,
    product_category_or_therapeutic_area_5                   TEXT,
    name_of_drug_or_biological_or_device_or_supply_5         TEXT,
    associated_drug_or_biological_ndc_5                      TEXT,
    associated_device_or_medical_supply_pdi_5                TEXT,
    program_year                                             TEXT,
    payment_publication_date                                 TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================================
-- Load Data using LOCAL INFILE
-- =========================================================================
-- SET GLOBAL local_infile = 1;

LOAD DATA LOCAL INFILE 'C:/Users/Mike/Documents/Analysis Projects/Compliance & spend dashboards/OP_DTL_GNRL_PGYR2024_P06302025_06162025.csv'
INTO TABLE stg_general_payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- =========================================================================
-- Basic Integrity Check
-- =========================================================================
SELECT COUNT(*) as total_rows_loaded FROM stg_general_payments;
SELECT record_id, total_amount_of_payment_usdollars, date_of_payment 
FROM stg_general_payments 
LIMIT 5;
