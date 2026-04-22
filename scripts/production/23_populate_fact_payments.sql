-- =========================================================================
-- POPULATE FACT TABLE (ETL -> STAR SCHEMA) - BATCHED EDITION
-- Purpose: Load clean data into Fact Table with Progress Tracking
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS PopulateFactPayments $$
CREATE PROCEDURE PopulateFactPayments()
BEGIN
    DECLARE min_id BIGINT;
    DECLARE max_id BIGINT;
    DECLARE batch_size INT DEFAULT 50000; 
    DECLARE current_start BIGINT;
    DECLARE current_end BIGINT;
    DECLARE total_rows_inserted INT DEFAULT 0;
    DECLARE rows_in_this_batch INT DEFAULT 0;
    DECLARE last_processed_id BIGINT;

    -- =========================================================================
    -- POPULATE FACT TABLE (BATCH LOOP)
    -- =========================================================================
    
    SELECT MIN(payment_id), MAX(payment_id) INTO min_id, max_id FROM general_payments;
    
    -- Check Resume Point
    SELECT MAX(batch_end_id) INTO last_processed_id 
    FROM migration_log 
    WHERE process_name = 'PopulateFactPayments' AND log_type = 'BATCH';

    IF last_processed_id IS NULL THEN
        SET current_start = min_id;
    ELSE
        SET current_start = last_processed_id + 1;
    END IF;

    SELECT CONCAT('Starting Fact Load from ID: ', current_start) as Status;

    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;        
        START TRANSACTION;

        INSERT INTO fact_payments (
            date_key, recipient_key, payer_key, product_key, nature_key, amount_usd, number_of_payments, record_id
        )
        SELECT 
            d.date_key,
            r.recipient_key,
            p.payer_key,
            COALESCE(prod.product_key, -1), -- Points to the 'Unknown' record we created
            COALESCE(nat.nature_key, -1),   -- Points to Clean Dimension
            g.amount_usd,
            g.number_of_payments,
            g.record_id
        FROM general_payments g
        INNER JOIN dim_date d ON g.payment_date = d.full_date
        INNER JOIN dim_recipient r ON COALESCE(NULLIF(g.recipient_profile_id, ''), g.teaching_hospital_ccn) = r.recipient_id
        INNER JOIN dim_payer p ON g.payer_id = p.payer_id 
        LEFT JOIN dim_product prod ON 
            (g.product_name_1 = prod.product_name OR (g.product_name_1 IS NULL AND prod.product_name IS NULL)) AND
            (g.product_category_1 = prod.product_category OR (g.product_category_1 IS NULL AND prod.product_category IS NULL)) AND 
            (g.product_type_1 = prod.product_type OR (g.product_type_1 IS NULL AND prod.product_type IS NULL)) AND
            (g.product_ndc_1 = prod.ndc OR (g.product_ndc_1 IS NULL AND prod.ndc IS NULL))
        LEFT JOIN dim_nature nat ON g.payment_nature = nat.payment_nature
        WHERE g.payment_id >= current_start 
          AND g.payment_id < current_end
          AND g.amount_usd IS NOT NULL;

        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_inserted = total_rows_inserted + rows_in_this_batch;
        
        -- Log Progress
        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('PopulateFactPayments', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;        
        SET current_start = current_end;
    END WHILE;

    -- Final Summary
    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('PopulateFactPayments', 'SUMMARY', max_id, total_rows_inserted, 'Fact Table Load Complete');

    SELECT CONCAT('Load Complete. Total Facts Inserted: ', total_rows_inserted) AS FinalStatus;

END $$

DELIMITER ;

CALL PopulateFactPayments();
DROP PROCEDURE IF EXISTS PopulateFactPayments;
