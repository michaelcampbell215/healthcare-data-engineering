DELIMITER $$

-- =========================================================================
-- Create BatchCleanCity
-- =========================================================================
DROP PROCEDURE IF EXISTS BatchCleanCity $$
CREATE PROCEDURE BatchCleanCity()
BEGIN
    DECLARE min_id INT;
    DECLARE max_id INT;
    DECLARE batch_size INT DEFAULT 10000; 
    DECLARE current_start INT;
    DECLARE current_end INT;
    DECLARE total_rows_updated INT DEFAULT 0; 
    DECLARE rows_in_this_batch INT DEFAULT 0;
    DECLARE last_processed_id INT;

    -- 1. Range Setup
    SELECT MIN(payment_id), MAX(payment_id) INTO min_id, max_id FROM general_payments;

    -- 2. Resume Logic (Check migration_log)
    SELECT MAX(batch_end_id) INTO last_processed_id 
    FROM migration_log 
    WHERE process_name = 'BatchCleanCity' AND log_type = 'BATCH';

    IF last_processed_id IS NULL THEN
        SET current_start = min_id;
    ELSE
        SET current_start = last_processed_id + 1;
    END IF;

    SELECT CONCAT('Starting Reference-Based Cleanup from ID: ', current_start) AS Status;

    -- 3. Batch Processing Loop
    -- WARNING: This procedure is single-threaded. Running multiple instances concurrently 
    -- will cause race conditions and deadlocks because they will compete for the same ID ranges.
    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;
        
        START TRANSACTION;

        -- Reference-Based Standardization
        -- Optimized join using the PRIMARY KEY on ref_zip_city.zip_code
        UPDATE general_payments g
        INNER JOIN ref_zip_city r ON LEFT(g.recipient_zip, 5) = r.zip_code
        SET 
            g.recipient_city = r.city,
            g.recipient_state = r.state_id,
            g.recipient_zip = LEFT(g.recipient_zip, 5),
            g.recipient_zip_population = CAST(r.population AS UNSIGNED)
        WHERE g.payment_id >= current_start 
          AND g.payment_id < current_end
          AND g.recipient_country = 'United States';
          -- Removed the (city != city) check to ensure we populate population data
          -- even if the city/state were already correct.	
	
          
        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;

        -- Progress Tracking
        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanCity', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;
        
        SET current_start = current_end;
    END WHILE;
    
    -- 4. Final Summary
    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanCity', 'SUMMARY', max_id, total_rows_updated, 'Completed Ref-based location standardization');

    SELECT CONCAT('Ref-based Cleanup Complete. Total Rows Affected: ', total_rows_updated) AS FinalStatus;
END $$

DELIMITER ;

CALL BatchCleanCity();

DROP PROCEDURE IF EXISTS BatchCleanCity; 