-- =========================================================================
-- Create Destination Table
-- =========================================================================

DROP TABLE IF EXISTS general_payments;
CREATE TABLE general_payments (
    payment_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    record_id VARCHAR(50),
    change_type VARCHAR(20),
    program_year INT,
    payment_date DATE,
    publication_date DATE,
    amount_usd DECIMAL(15, 2),
    number_of_payments INT,
    payment_nature VARCHAR(200),
    payment_form VARCHAR(100),
    recipient_profile_id VARCHAR(20),
    recipient_npi VARCHAR(15),
    recipient_type VARCHAR(50),
    recipient_first_name VARCHAR(100),
    recipient_middle_name VARCHAR(50),
    recipient_last_name VARCHAR(100),
    recipient_name_suffix VARCHAR(20),
    recipient_full_name VARCHAR(255), 
    teaching_hospital_name VARCHAR(255), 
    recipient_specialty VARCHAR(500), 
    recipient_license_state VARCHAR(50),
    teaching_hospital_ccn VARCHAR(20), 
    recipient_address_line1 VARCHAR(255),
    recipient_address_line2 VARCHAR(255),
    recipient_city VARCHAR(100),
    recipient_state VARCHAR(10),
    recipient_zip VARCHAR(20),
    recipient_zip_population INT,
    recipient_country VARCHAR(100),
    recipient_province VARCHAR(100),
    payer_name VARCHAR(150),
    payer_id VARCHAR(20),
    subsidiary_name VARCHAR(150),
    subsidiary_id VARCHAR(20),
    subsidiary_state VARCHAR(10),
    subsidiary_country VARCHAR(100),
    
    -- Third Party
    name_of_third_party_entity VARCHAR(255),
    
    -- Risk Context Indicators
    physician_ownership_indicator VARCHAR(10),
    third_party_payment_recipient_indicator VARCHAR(50),
    charity_indicator VARCHAR(10),
    related_product_indicator VARCHAR(10),
    dispute_status VARCHAR(20),
    contextual_information TEXT,
    travel_city VARCHAR(100),
    travel_state VARCHAR(10),
    travel_country VARCHAR(100),
    product_type_1 VARCHAR(100), product_name_1 VARCHAR(255), product_category_1 VARCHAR(255), product_ndc_1 VARCHAR(20),
    product_type_2 VARCHAR(100), product_name_2 VARCHAR(255), product_category_2 VARCHAR(255), product_ndc_2 VARCHAR(20),
    product_type_3 VARCHAR(100), product_name_3 VARCHAR(255), product_category_3 VARCHAR(255), product_ndc_3 VARCHAR(20),
    product_type_4 VARCHAR(100), product_name_4 VARCHAR(255), product_category_4 VARCHAR(255), product_ndc_4 VARCHAR(20),
    product_type_5 VARCHAR(100), product_name_5 VARCHAR(255), product_category_5 VARCHAR(255), product_ndc_5 VARCHAR(20),
    etl_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_record_id (record_id)
);


-- Create Audit Log
DROP TABLE IF EXISTS migration_log;
CREATE TABLE IF NOT EXISTS migration_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    log_type ENUM('BATCH', 'SUMMARY') DEFAULT 'BATCH',
    batch_end_id BIGINT,
    rows_processed INT,
    notes TEXT, -- Added for extra context or error messages
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);