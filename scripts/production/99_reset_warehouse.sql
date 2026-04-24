-- =========================================================================
-- RESET WAREHOUSE (TRUNCATE ALL)
-- Purpose: Safely empty the warehouse to restart the load process.
-- WARNING: THIS DELETES ALL DATA IN THE STAR SCHEMA.
-- =========================================================================

-- 1. Disable Foreign Key Checks (Crucial to avoid FK errors during truncate)
SET FOREIGN_KEY_CHECKS = 0;

SELECT 'Truncating Tables...' AS Status;

-- 2. Truncate Fact Table First (Best Practice, though FK checks=0 makes it optional)
TRUNCATE TABLE fact_payments;

-- 3. Truncate Dimensions
TRUNCATE TABLE dim_recipient;
TRUNCATE TABLE dim_payer;
TRUNCATE TABLE dim_product;
TRUNCATE TABLE dim_date;

-- 4. Reset Migration Log (So the scripts don't think they are already done)
DELETE FROM migration_log WHERE process_name IN ('PopulateFactPayments', 'PopulateWarehouse', 'PopulateDimensions');
-- OR: TRUNCATE TABLE migration_log; (If you want to clear absolutely everything including staging logs)

-- 5. Re-enable Foreign Key Checks
SET FOREIGN_KEY_CHECKS = 1;

SELECT 'Warehouse Reset Complete. Ready to reload.' AS Status;
