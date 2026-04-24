DELIMITER $$

-- =========================================================================
-- Standardize Location Columns
-- =========================================================================
DROP PROCEDURE IF EXISTS BatchCleanLocation $$
CREATE PROCEDURE BatchCleanLocation()
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
    WHERE process_name = 'BatchCleanLocation' AND log_type = 'BATCH';

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
            recipient_city = CleanCity(TitleCase(recipient_city)),
            recipient_state = UPPER(recipient_state),
            recipient_country = TitleCase(recipient_country),
            travel_city = CleanCity(TitleCase(travel_city)),
            travel_state = UPPER(travel_state),
            travel_country = TitleCase(travel_country),
            subsidiary_state = UPPER(subsidiary_state),
            subsidiary_country = TitleCase(subsidiary_country)
        WHERE payment_id >= current_start 
            AND payment_id < current_end;
    
    
        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanLocation', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;  
        
 
        SET current_start = current_end;
    END WHILE;

    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanLocation', 'SUMMARY', max_id, total_rows_updated, 'Finished entire range successfully');

    SELECT CONCAT('Cleanup Complete. Total Rows Affected: ', total_rows_updated) AS FinalStatus;
END $$

DELIMITER ;

CALL BatchCleanLocation();

DROP PROCEDURE BatchCleanLocation();