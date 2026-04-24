DELIMITER $$
-- =========================================================================
-- Create BatchCleanProvince
-- =========================================================================
DROP PROCEDURE IF EXISTS BatchCleanProvince $$
CREATE PROCEDURE BatchCleanProvince()
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
    WHERE process_name = 'BatchCleanProvince' AND log_type = 'BATCH';

    IF last_processed_id IS NULL THEN
        SET current_start = min_id;
    ELSE
        SET current_start = last_processed_id + 1;
    END IF;

    SELECT CONCAT('Starting Cleanup from ID: ', current_start) AS Status;
    
    WHILE current_start <= max_id DO
        SET current_end = current_start + batch_size;    
        
        START TRANSACTION;        
        UPDATE general_payments g
        INNER JOIN stg_general_payments s ON g.payment_id = s.staging_id
        SET 
            g.recipient_state = CASE
            WHEN s.recipient_city = 'San Juan' THEN 'PR'  
                WHEN s.recipient_city = 'CHICAGO' THEN 'IL'
                WHEN s.recipient_city = 'BLACKFOOT' THEN 'ID'
                WHEN s.recipient_city = 'MEMPHIS' THEN 'TN'
                WHEN s.recipient_city = 'DEDEDO' THEN 'GU'
                WHEN s.recipient_city = 'BROOKLYN' THEN 'NY'
                WHEN s.recipient_city IN ('ENCINITAS', 'SAN MATEO') THEN 'CA'
                WHEN s.recipient_city IN ('WEST PALM BEACH', 'JACKSONVILLE', 'FORT LAUDERDALE') THEN 'FL'
                WHEN s.recipient_city IN ('SUGARLAND', 'SPRING', 'TE', 'FORT WORTH', 'MCALLEN') THEN 'TX'
                WHEN s.recipient_province IN ('CA','DE','FL','GA','KY','LA','MA','MD','NY','OK','PA','PR','TN','TX','WA') THEN s.recipient_province
                ELSE g.recipient_state
            END,
            g.recipient_zip = CASE
                WHEN s.recipient_province IN ('CA','DE','FL','GA','KY','LA','MA','MD','NY','OK','PA','PR', 'TE', 'TN','TX','WA') THEN s.recipient_postal_code
                WHEN s.recipient_province IN ('GUAM', 'PUERTO RICO', 'FORT BEND', 'TEXAS', 'FLORIDA', 'TENNESSEE', 'ILLINOIS', 'SAN MATEO', 'USA', 'FLORIDA FL', 'SAN DIEGO', 'HIDALGO COUNTY', 'IDAHO') THEN s.recipient_postal_code
                ELSE g.recipient_zip
            END
            WHERE payment_id >= current_start 
                AND payment_id < current_end 
                AND s.recipient_province != ''
                AND s.recipient_country IN ('United States', 'United States Minor Outlying Islands');
                
        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;   

        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanProvince', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;        
 
        SET current_start = current_end;
    END WHILE;

    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanProvince', 'SUMMARY', max_id, total_rows_updated, 'Finished entire range successfully');

    SELECT CONCAT('Cleanup Complete. Total Rows Affected: ', total_rows_updated) AS FinalStatus;
END $$

DELIMITER ;


CALL BatchCleanProvince();

DROP PROCEDURE IF EXISTS BatchCleanProvince();