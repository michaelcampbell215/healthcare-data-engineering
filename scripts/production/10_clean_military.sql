DELIMITER $$


-- =========================================================================
-- Create BatchCleanMilitaryAddress
-- =========================================================================
DROP PROCEDURE IF EXISTS BatchCleanMilitaryAddress $$
CREATE PROCEDURE BatchCleanMilitaryAddress()
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
    WHERE process_name = 'BatchCleanMilitaryAddress' AND log_type = 'BATCH';

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
            g.recipient_city = CASE 	
                WHEN s.recipient_city = 'Camp Foster' AND s.recipient_country = 'Japan' AND s.recipient_postal_code = '96362' THEN 'Fpo'
                WHEN s.recipient_city = 'Yokota Air Force Base' AND s.recipient_country = 'Japan' AND s.recipient_postal_code = '96326' THEN 'Apo'
                WHEN s.recipient_city = 'Kadena Ab' AND s.recipient_country = 'Japan' AND s.recipient_postal_code = '96367' THEN 'Apo'
                WHEN s.recipient_city = 'Seoul' AND s.recipient_country = 'Korea (republic Of)' AND s.recipient_postal_code = '962055652' THEN 'Apo'
                ELSE g.recipient_city
            END,	

            g.recipient_state = CASE
                WHEN s.recipient_city = 'APO' AND s.recipient_country = 'United Arab Emirates' AND s.recipient_postal_code = '09603' THEN 'AP'  
                WHEN s.recipient_city = 'APO' AND s.recipient_country = 'Germany' AND s.recipient_postal_code IN ('09094','09165','09096','09180','09244','09126') THEN 'AE'        
                WHEN s.recipient_province = 'AE' AND s.recipient_country = 'Italy' AND s.recipient_postal_code = '09636' THEN 'AE'   
                WHEN s.recipient_city in ('FPO', 'Apo') AND s.recipient_country = 'Japan' AND s.recipient_postal_code IN ('96328','96362','96306') THEN 'AP'    
                WHEN s.recipient_city = 'APO' AND s.recipient_country = 'Korea (democratic People\'s Republic Of)' AND s.recipient_postal_code = '962782060' THEN 'AP' 
                WHEN s.recipient_province = 'RHODE ISLAND' and s.recipient_country = 'Afghanistan' then 'RI'
                WHEN s.recipient_province = 'Washington' and s.recipient_country = 'Aland Islands' then 'WA'
                WHEN s.recipient_province = 'Washington' and s.recipient_country = 'Austria' then 'WA'
                WHEN s.recipient_province = 'TEXAS' and s.recipient_country = 'Cook Islands' then 'TX'
                WHEN s.recipient_city = 'Camp Foster' AND s.recipient_country = 'Japan' AND s.recipient_postal_code = '96362' THEN 'AP'
                WHEN s.recipient_city = 'Yokota Air Force Base' AND s.recipient_country = 'Japan' AND s.recipient_postal_code = '96326' THEN 'AP'
                WHEN s.recipient_city = 'Kadena Ab' AND s.recipient_country = 'Japan' AND s.recipient_postal_code = '96367' THEN 'AP'
                WHEN s.recipient_city = 'Seoul' AND s.recipient_country = 'Korea (republic Of)' AND s.recipient_postal_code = '962055652' THEN 'AP'
                ELSE g.recipient_state
            END,

            g.recipient_zip = CASE
                WHEN s.recipient_city IN ('APO', 'YOKOTA AIR FORCE BASE', 'FPO') THEN s.recipient_postal_code
                WHEN s.recipient_city = 'SEOUL' AND s.recipient_province = 'APO AP' THEN s.recipient_postal_code
                WHEN s.recipient_city = 'CAMP FOSTER' AND s.recipient_province = 'OK' THEN s.recipient_postal_code
                WHEN s.recipient_city = 'KADENA AB' AND s.recipient_province = 'OKINAWA' THEN s.recipient_postal_code
                WHEN s.recipient_city = 'PROVIDENCE' and s.recipient_province = 'RHODE ISLAND' THEN s.recipient_postal_code
                WHEN s.recipient_city = 'CONROE' and s.recipient_province = 'TEXAS' THEN s.recipient_postal_code
                WHEN s.recipient_city = 'PA' AND s.recipient_state = '15905-4305' AND s.recipient_zip_code = 'United States' THEN '15905'                
                WHEN s.recipient_city in ('VANCOUVER', 'Seattle') and s.recipient_province = 'Washington' THEN s.recipient_postal_code
                ELSE g.recipient_zip
            END,

            g.recipient_country = CASE 
                WHEN s.recipient_province = 'RHODE ISLAND' AND s.recipient_country = 'Afghanistan' THEN 'United States'
                WHEN s.recipient_province = 'Washington' AND s.recipient_country = 'Aland Islands' THEN 'United States'
                WHEN s.recipient_province = 'Washington' AND s.recipient_country = 'Austria' THEN 'United States'
                WHEN s.recipient_province = 'TEXAS' AND s.recipient_country = 'Cook Islands' THEN 'United States'
                WHEN s.recipient_city IN ('FPO', 'APO', 'KADENA AB', 'YOKOTA AIR FORCE BASE', 'CAMP FOSTER', 'KADENA AB', 'SEOUL') 
                    AND s.recipient_country IN ('Germany', 'Korea (republic Of)','Korea (democratic People\'s Republic Of)', 'Italy', 'Japan','United Arab Emirates') 
                    THEN 'United States'
                ELSE g.recipient_country
            END
  
        WHERE payment_id >= current_start 
          AND payment_id < current_end
          AND s.recipient_country NOT IN ('United States', 'United States Minor Outlying Islands');
          
        SET rows_in_this_batch = ROW_COUNT();
        SET total_rows_updated = total_rows_updated + rows_in_this_batch;

        INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed) 
        VALUES ('BatchCleanMilitaryAddress', 'BATCH', current_end - 1, rows_in_this_batch);

        COMMIT;        

        SET current_start = current_end;
    END WHILE;

    INSERT INTO migration_log (process_name, log_type, batch_end_id, rows_processed, notes) 
    VALUES ('BatchCleanMilitaryAddress', 'SUMMARY', max_id, total_rows_updated, 'Finished entire range successfully');

    SELECT CONCAT('Cleanup Complete. Total Rows Affected: ', total_rows_updated) AS FinalStatus;
END $$


DELIMITER ;


CALL BatchCleanMilitaryAddress();

DROP PROCEDURE IF EXISTS BatchCleanMilitaryAddress();