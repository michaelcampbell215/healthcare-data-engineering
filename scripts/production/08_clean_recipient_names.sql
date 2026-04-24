-- =========================================================================
-- Batch Clean Recipient Names & Details (Standardized Logging)
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS BatchCleanRecipientNames $$
CREATE PROCEDURE BatchCleanRecipientNames()
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
    WHERE process_name = 'BatchCleanRecipientNames' AND log_type = 'BATCH';

    IF last_processed_id IS NULL THEN
        SET current_start = min_id;
    ELSE
        SET current_start = last_processed_id + 1;
    END IF;
    
    SELECT CONCAT('Starting Batch: Recipient Names from ID: ', current_start) as Status;
    
    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;        
        START TRANSACTION;

        UPDATE general_payments
        SET 
            recipient_first_name = CleanName(recipient_first_name),
            recipient_middle_name = CleanName(recipient_middle_name),
            recipient_last_name = CleanName(recipient_last_name),
            recipient_name_suffix = TitleCase(CleanName(recipient_name_suffix)),
            recipient_license_state = UPPER(TRIM(recipient_license_state)),
            recipient_specialty = TitleCase(recipient_specialty),
            -- Standardize Hospital Affiliation names for all records
            teaching_hospital_name = TitleCase(CleanHospitalName(teaching_hospital_name))
        WHERE payment_id >= current_start 
          AND payment_id < current_end;

        -- Reconstruct Full Name after cleaning components
        UPDATE general_payments
        SET recipient_full_name = TRIM(CONCAT_WS(' ', 
            recipient_first_name, 
            recipient_middle_name, 
            recipient_last_name, 
            recipient_name_suffix
        ))
        WHERE payment_id >= current_start 
          AND payment_id < current_end;

        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;
        
        -- Batch Logging
        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanRecipientNames', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;        
        
        SET current_start = current_end;
    END WHILE;

    -- Summary Logging
    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanRecipientNames', 'SUMMARY', max_id, total_rows_updated, 'Finished entire range successfully');

    SELECT CONCAT('Batch Complete: Recipient Names. Rows Processed: ', total_rows_updated) as Status;
END $$

DELIMITER ;

CALL BatchCleanRecipientNames();
DROP PROCEDURE IF EXISTS BatchCleanRecipientNames;