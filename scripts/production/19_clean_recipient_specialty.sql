-- =========================================================================
-- Batch Clean Recipient Specialty (Extract Primary Specialty)
-- Purpose: Convert "Type|Broad|Specific" into "Specific"
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS BatchCleanSpecialty $$
CREATE PROCEDURE BatchCleanSpecialty()
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
    WHERE process_name = 'BatchCleanSpecialty' AND log_type = 'BATCH';

    IF last_processed_id IS NULL THEN
        SET current_start = min_id;
    ELSE
        SET current_start = last_processed_id + 1;
    END IF;

    SELECT CONCAT('Starting Specialty Cleanup from ID: ', current_start) as Status;

    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;        
        START TRANSACTION;

        UPDATE general_payments
        SET recipient_specialty = TRIM(
            CASE
                -- If it contains a pipe, grab the last segment
                WHEN recipient_specialty LIKE '%|%' THEN 
                    SUBSTRING_INDEX(recipient_specialty, '|', -1)
                
                -- If it starts with "Allopathic...", strip that out
                WHEN recipient_specialty LIKE 'Allopathic & Osteopathic Physicians%' THEN 
                    TRIM(REPLACE(recipient_specialty, 'Allopathic & Osteopathic Physicians', ''))
                
                ELSE recipient_specialty
            END
        )
        WHERE payment_id >= current_start 
          AND payment_id < current_end
          AND recipient_specialty IS NOT NULL
          AND (recipient_specialty LIKE '%|%' OR recipient_specialty LIKE 'Allopathic%');

        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;
        
        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanSpecialty', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;        
        SET current_start = current_end;
    END WHILE;

    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanSpecialty', 'SUMMARY', max_id, total_rows_updated, 'Extracted last segment of pipe-delimited specialty');

    SELECT CONCAT('Specialty Cleanup Complete. Total Rows Affected: ', total_rows_updated) as Status;
END $$

DELIMITER ;

CALL BatchCleanSpecialty();
DROP PROCEDURE IF EXISTS BatchCleanSpecialty;
