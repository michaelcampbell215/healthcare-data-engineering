-- =========================================================================
-- Batch Clean Products (Blanks to NULL + TitleCase Names)
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS BatchCleanProducts $$
CREATE PROCEDURE BatchCleanProducts()
BEGIN
    DECLARE min_id BIGINT;
    DECLARE max_id BIGINT;
    DECLARE batch_size INT DEFAULT 50000; 
    DECLARE current_start BIGINT;
    DECLARE current_end BIGINT;
    DECLARE total_rows_updated INT DEFAULT 0;
    DECLARE rows_in_this_batch INT DEFAULT 0;
    DECLARE last_processed_id BIGINT;
    
    SELECT MIN(payment_id), MAX(payment_id) INTO min_id, max_id FROM general_payments;
    
    -- Resume Logic
    SELECT MAX(batch_end_id) INTO last_processed_id 
    FROM migration_log 
    WHERE process_name = 'BatchCleanProducts' AND log_type = 'BATCH';

    IF last_processed_id IS NULL THEN
        SET current_start = min_id;
    ELSE
        SET current_start = last_processed_id + 1;
    END IF;

    SELECT CONCAT('Starting Product Cleanup & Standardization from ID: ', current_start) as Status;

    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;        
        START TRANSACTION;

        UPDATE general_payments
        SET 
            -- Slot 1
            product_type_1 = NULLIF(TRIM(product_type_1), ''),
            product_name_1 = TitleCase(NULLIF(TRIM(product_name_1), '')), -- Cleans blank AND fixes case
            product_category_1 = TitleCase(NULLIF(TRIM(product_category_1), '')),
            product_ndc_1 = NULLIF(TRIM(product_ndc_1), ''),
            
            -- Slot 2
            product_type_2 = NULLIF(TRIM(product_type_2), ''),
            product_name_2 = TitleCase(NULLIF(TRIM(product_name_2), '')),
            product_category_2 = TitleCase(NULLIF(TRIM(product_category_2), '')),
            product_ndc_2 = NULLIF(TRIM(product_ndc_2), ''),

            -- Slot 3
            product_type_3 = NULLIF(TRIM(product_type_3), ''),
            product_name_3 = TitleCase(NULLIF(TRIM(product_name_3), '')),
            product_category_3 = TitleCase(NULLIF(TRIM(product_category_3), '')),
            product_ndc_3 = NULLIF(TRIM(product_ndc_3), ''),

            -- Slot 4
            product_type_4 = NULLIF(TRIM(product_type_4), ''),
            product_name_4 = TitleCase(NULLIF(TRIM(product_name_4), '')),
            product_category_4 = TitleCase(NULLIF(TRIM(product_category_4), '')),
            product_ndc_4 = NULLIF(TRIM(product_ndc_4), ''),

            -- Slot 5
            product_type_5 = NULLIF(TRIM(product_type_5), ''),
            product_name_5 = TitleCase(NULLIF(TRIM(product_name_5), '')),
            product_category_5 = TitleCase(NULLIF(TRIM(product_category_5), '')),
            product_ndc_5 = NULLIF(TRIM(product_ndc_5), '')

        WHERE payment_id >= current_start AND payment_id < current_end;
          

        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;
        
        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanProducts', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;        
        SET current_start = current_end;
    END WHILE;

    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanProducts', 'SUMMARY', max_id, total_rows_updated, 'Converted blanks to NULL and TitleCased names');

    SELECT CONCAT('Cleanup Complete. Total Rows Affected: ', total_rows_updated) as Status;
END $$

DELIMITER ;

CALL BatchCleanProducts();
DROP PROCEDURE IF EXISTS BatchCleanProducts;