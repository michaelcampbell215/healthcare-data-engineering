-- =========================================================================
-- OPTIMIZE PERFORMANCE (INDEXING)
-- Purpose: Add critical indexes to Fact Table to prevent timeouts.
-- =========================================================================

SELECT 'Adding Index on fact_payments(record_id)... This may take 5-10 minutes.' as Status;

-- Create Index if not exists (MySQL syntax doesn't support IF NOT EXISTS for ADD INDEX easily, duplicate error is harmless)
-- Use a stored procedure to handle duplicate index error gracefully
DELIMITER $$
DROP PROCEDURE IF EXISTS AddIndexRecordID $$
CREATE PROCEDURE AddIndexRecordID()
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'fact_payments' 
        AND INDEX_NAME = 'idx_fact_record_id'
    ) THEN
        ALTER TABLE fact_payments ADD INDEX idx_fact_record_id (record_id);
    END IF;
END $$
DELIMITER ;

CALL AddIndexRecordID();
DROP PROCEDURE IF EXISTS AddIndexRecordID;

SELECT 'Optimization Complete.' as Status;
