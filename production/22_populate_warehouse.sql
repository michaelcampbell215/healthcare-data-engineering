-- =========================================================================
-- POPULATE WAREHOUSE (ETL -> STAR SCHEMA)
-- Purpose: Load clean data into Dimensions and Fact Table with Progress Tracking
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS PopulateDimensions $$
CREATE PROCEDURE PopulateDimensions()
BEGIN
    DECLARE min_id BIGINT;
    DECLARE max_id BIGINT;
    DECLARE batch_size INT DEFAULT 50000; 
    DECLARE current_start BIGINT;
    DECLARE current_end BIGINT;
    DECLARE total_rows_inserted INT DEFAULT 0;
    DECLARE rows_in_this_batch INT DEFAULT 0;
    DECLARE last_processed_id BIGINT;
    DECLARE unknown_product_id INT;

    -- =========================================================================
    -- 1. POPULATE DIMENSIONS 
    -- =========================================================================
    
    -- Date Dimension
    SELECT 'Populating DimDate...' AS Status;
    INSERT IGNORE INTO dim_date (date_key, full_date, year, quarter, month, month_name, day_of_week, is_weekend)
    SELECT 
        CAST(DATE_FORMAT(date_val, '%Y%m%d') AS UNSIGNED),
        date_val,
        YEAR(date_val),
        QUARTER(date_val),
        MONTH(date_val),
        MONTHNAME(date_val),
        DAYNAME(date_val),
        CASE WHEN DAYOFWEEK(date_val) IN (1, 7) THEN 1 ELSE 0 END
    FROM (
        SELECT DATE_ADD('2020-01-01', INTERVAL t.seq DAY) AS date_val
        FROM (
            SELECT (t1.i*1000 + t2.i*100 + t3.i*10 + t4.i) AS seq
            FROM (SELECT 0 AS i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t1,
                 (SELECT 0 AS i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t2,
                 (SELECT 0 AS i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t3,
                 (SELECT 0 AS i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t4
        ) t
        WHERE t.seq <= 4000
    ) dates;

    -- Payer Dimension
    SELECT 'Populating DimPayer...' AS Status;
    INSERT IGNORE INTO dim_payer (payer_id, payer_name)
    SELECT DISTINCT payer_id, payer_name FROM general_payments WHERE payer_id IS NOT NULL AND payer_id != '';

    -- Product Dimension (Ensuring "Unknown" exists for FK constraints)
    SELECT 'Populating DimProduct...' AS Status;
    
    -- Insert Real Products
    INSERT IGNORE INTO dim_product (product_name, product_category, product_type, ndc)
    SELECT DISTINCT product_name_1, product_category_1, product_type_1, product_ndc_1
    FROM general_payments WHERE product_name_1 IS NOT NULL AND product_name_1 != '';

    -- Insert "Unknown" Placeholder (if not exists)
    INSERT IGNORE INTO dim_product (product_key, product_name, product_category, product_type, ndc)
    VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 'Unknown');
    
    -- Recipient Dimension
    SELECT 'Populating DimRecipient...' AS Status;
    INSERT IGNORE INTO dim_recipient (
        recipient_id, recipient_name, recipient_type, specialty, npi, ccn, city, state, zip, population, lat, lng
    )
    SELECT 
        COALESCE(NULLIF(recipient_profile_id, ''), teaching_hospital_ccn),
        MAX(CASE 
            WHEN recipient_type LIKE '%Hospital%' THEN teaching_hospital_name 
            ELSE CONCAT_WS(' ', recipient_first_name, recipient_last_name) 
        END),
        MAX(recipient_type),
        MAX(recipient_specialty),
        MAX(recipient_npi),
        MAX(teaching_hospital_ccn),
        MAX(recipient_city), -
        COALESCE(MAX(z.state_id), MAX(recipient_state)), -
        COALESCE(MAX(z.zip_code), LEFT(MAX(recipient_zip), 5)), 
        MAX(recipient_zip_population),
        MAX(z.lat),
        MAX(z.lng)
    FROM general_payments g
    LEFT JOIN ref_zip_city z ON LEFT(g.recipient_zip, 5) = z.zip_code
    WHERE (recipient_profile_id IS NOT NULL AND recipient_profile_id != '') 
       OR (teaching_hospital_ccn IS NOT NULL AND teaching_hospital_ccn != '')
    GROUP BY COALESCE(NULLIF(recipient_profile_id, ''), teaching_hospital_ccn);

    -- Payment Nature Dimension
    SELECT 'Populating DimNature...' AS Status;
    INSERT IGNORE INTO dim_nature (payment_nature)
    SELECT DISTINCT payment_nature FROM general_payments WHERE payment_nature IS NOT NULL AND payment_nature != '';

    -- Insert "Unknown" Placeholder
    INSERT IGNORE INTO dim_nature (nature_key, payment_nature) VALUES (-1, 'Unknown');

    -- =========================================================================
    -- 2. COMPLETION
    -- =========================================================================

    SELECT 'Dimensions Populated Successfully.' AS Status;

END $$

DELIMITER ;

CALL PopulateDimensions();
DROP PROCEDURE IF EXISTS PopulateDimensions;
