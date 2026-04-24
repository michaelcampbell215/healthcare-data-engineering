-- =========================================================================
-- Apply Specialty Standardizations
-- Purpose: Update general_payments using the mappings in ref_specialties
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS ApplySpecialtyMappings $$
CREATE PROCEDURE ApplySpecialtyMappings()
BEGIN
    DECLARE rows_updated INT DEFAULT 0;

    SELECT 'Starting Specialty Standardization...' AS Status;

    UPDATE general_payments g
    JOIN ref_specialties r ON g.recipient_specialty = r.original_specialty
    SET g.recipient_specialty = r.standardized_specialty
    WHERE g.recipient_specialty != r.standardized_specialty;
    
    SET rows_updated = ROW_COUNT();

    -- Log Completion
    INSERT INTO migration_log (process_name, log_type, rows_processed, notes) 
    VALUES ('ApplySpecialtyMappings', 'SUMMARY', rows_updated, 'Applied standardized specialties from reference table');

    SELECT CONCAT('Specialty Standardization Complete. Rows Updated: ', rows_updated) AS FinalStatus;
END $$

DELIMITER ;

CALL ApplySpecialtyMappings();
DROP PROCEDURE IF EXISTS ApplySpecialtyMappings;
