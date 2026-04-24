-- =========================================================================
-- DATA QUALITY PURGE
-- Purpose: Remove corrupted rows with shifted columns or invalid IDs
-- =========================================================================

DELETE FROM general_payments 
WHERE record_id = 'No' 
   OR program_year < 2000 
   OR payer_name REGEXP '^[0-9]+$'; -- Removes rows where the name is just a number

-- Log the purge
INSERT INTO migration_log (process_name, log_type, rows_processed, notes) 
VALUES ('DataQualityPurge', 'SUMMARY', ROW_COUNT(), 'Removed corrupted/shifted records from raw migration');

SELECT CONCAT('Data Purge Complete. Rows Removed: ', ROW_COUNT()) as Status;
