DELIMITER $$


-- =========================================================================
-- Standardize Address Columns
-- =========================================================================
DROP PROCEDURE IF EXISTS BatchCleanAddresses $$
CREATE PROCEDURE BatchCleanAddresses()
BEGIN
    DECLARE min_id INT;
    DECLARE max_id INT;
    DECLARE batch_size INT DEFAULT 10000; 
    DECLARE current_start INT;
    DECLARE current_end INT;
    DECLARE total_rows_updated INT DEFAULT 0; 
    DECLARE rows_in_this_batch INT DEFAULT 0;
    DECLARE last_processed_id INT;
    
    SELECT MIN(payment_id), MAX(payment_id) INTO min_id, max_id FROM general_payments;
    SELECT MAX(batch_end_id) INTO last_processed_id 
    FROM migration_log 
    WHERE process_name = 'BatchCleanAddresses' AND log_type = 'BATCH';

    IF last_processed_id IS NULL THEN
        SET current_start = min_id;
    ELSE
        SET current_start = last_processed_id + 1;
    END IF;

    SELECT CONCAT('Starting Cleanup from ID: ', current_start) AS Status;

    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;        
        START TRANSACTION;

        UPDATE general_payments
        SET 
            recipient_address_line1 = CleanAddress(recipient_address_line1),
            recipient_address_line2 = CleanAddress(recipient_address_line2)
        WHERE payment_id >= current_start AND payment_id < current_end;

        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;   

        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanAddresses', 'BATCH', current_end - 1, rows_in_this_batch);
          
        COMMIT;         
 
        SET current_start = current_end;
    END WHILE;

    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanAddresses', 'SUMMARY', max_id, total_rows_updated, 'Finished entire range successfully');

    SELECT CONCAT('Cleanup Complete. Total Rows Affected: ', total_rows_updated) AS FinalStatus;
END $$

DELIMITER ;


CALL BatchCleanAddresses();

DROP PROCEDURE IF EXISTS BatchCleanAddresses();