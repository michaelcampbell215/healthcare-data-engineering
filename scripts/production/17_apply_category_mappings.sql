-- =========================================================================
-- Apply Product Category Standardizations
-- Purpose: Update general_payments using the mappings in ref_product_categories
-- =========================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS ApplyCategoryMappings $$
CREATE PROCEDURE ApplyCategoryMappings()
BEGIN
    DECLARE rows_updated INT DEFAULT 0;

    SELECT 'Starting Category Standardization...' AS Status;

    -- Update Slot 1
    UPDATE general_payments g
    JOIN ref_product_categories r ON g.product_category_1 = r.original_category
    SET g.product_category_1 = r.standardized_category
    WHERE g.product_category_1 != r.standardized_category;
    
    SELECT CONCAT('Slot 1 Updated: ', ROW_COUNT()) AS Status;

    -- Update Slot 2
    UPDATE general_payments g
    JOIN ref_product_categories r ON g.product_category_2 = r.original_category
    SET g.product_category_2 = r.standardized_category
    WHERE g.product_category_2 != r.standardized_category;

    SELECT CONCAT('Slot 2 Updated: ', ROW_COUNT()) AS Status;

    -- Update Slot 3
    UPDATE general_payments g
    JOIN ref_product_categories r ON g.product_category_3 = r.original_category
    SET g.product_category_3 = r.standardized_category
    WHERE g.product_category_3 != r.standardized_category;
    
    SELECT CONCAT('Slot 3 Updated: ', ROW_COUNT()) AS Status;

    -- Update Slot 4
    UPDATE general_payments g
    JOIN ref_product_categories r ON g.product_category_4 = r.original_category
    SET g.product_category_4 = r.standardized_category
    WHERE g.product_category_4 != r.standardized_category;
    
    SELECT CONCAT('Slot 4 Updated: ', ROW_COUNT()) AS Status;

    -- Update Slot 5
    UPDATE general_payments g
    JOIN ref_product_categories r ON g.product_category_5 = r.original_category
    SET g.product_category_5 = r.standardized_category
    WHERE g.product_category_5 != r.standardized_category;
    
    SELECT CONCAT('Slot 5 Updated: ', ROW_COUNT()) AS Status;

    -- Log Completion
    INSERT INTO migration_log (process_name, log_type, rows_processed, notes) 
    VALUES ('ApplyCategoryMappings', 'SUMMARY', 0, 'Applied standardized categories from reference table');

    SELECT 'Category Standardization Complete.' AS FinalStatus;
END $$

DELIMITER ;

CALL ApplyCategoryMappings();
DROP PROCEDURE IF EXISTS ApplyCategoryMappings;
