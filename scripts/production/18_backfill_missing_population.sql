-- =========================================================================
-- Backfill Missing Population Data (City/State Fallback)
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS BackfillPopulation $$
CREATE PROCEDURE BackfillPopulation()
BEGIN
    DECLARE min_id BIGINT;
    DECLARE max_id BIGINT;
    DECLARE batch_size INT DEFAULT 50000; 
    DECLARE current_start BIGINT;
    DECLARE current_end BIGINT;
    DECLARE total_rows_updated INT DEFAULT 0;
    DECLARE rows_in_this_batch INT DEFAULT 0;
    
    SELECT MIN(payment_id), MAX(payment_id) INTO min_id, max_id FROM general_payments;
    SET current_start = min_id;
    
    SELECT CONCAT('Starting Population Backfill from ID: ', current_start) as Status;
    
    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;        
        START TRANSACTION;

        -- Update based on City/State match where Zip match failed
        UPDATE general_payments g
        JOIN (
            -- Get one population value per City/State (using MAX to handle duplicates)
            SELECT city, state_id, MAX(CAST(population AS UNSIGNED)) as fallback_pop
            FROM ref_zip_city
            GROUP BY city, state_id
        ) r ON g.recipient_city = r.city AND g.recipient_state = r.state_id
        SET g.recipient_zip_population = r.fallback_pop
        WHERE g.payment_id >= current_start 
          AND g.payment_id < current_end
          AND g.recipient_zip_population IS NULL
          AND g.recipient_country = 'United States';

        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;
        
        COMMIT;        
        
        SET current_start = current_end;
    END WHILE;

    -- Log this special maintenance task
    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BackfillPopulation', 'SUMMARY', max_id, total_rows_updated, 'Filled NULL populations using City/State match');

    SELECT CONCAT('Backfill Complete. Total Rows Updated: ', total_rows_updated) as Status;
END $$

DELIMITER ;

CALL BackfillPopulation();
DROP PROCEDURE IF EXISTS BackfillPopulation;