-- =========================================================================
-- DATA INTEGRITY CHECKSUM (The "CFO Test")
-- Purpose: Verify validation of Row Counts and Total Spend across the pipeline.
-- =========================================================================

/* 
    MENTOR NOTE: 
    Why this script? 
    Before we start analyzing "Risk" or "ROI", we must prove we haven't lost data.
    If the Warehouse spend matches the Source spend (within rounding), we trust the model.
*/

SELECT 
    '1. Raw Staging' as Stage,
    'stg_general_payments' as TableName,
    COUNT(*) as Row_Count, 
    FORMAT(SUM(Total_Amount_of_Payment_USDollars), 2) as Total_Spend_Formatted,
    SUM(Total_Amount_of_Payment_USDollars) as Total_Spend_Raw
FROM stg_general_payments

UNION ALL

SELECT 
    '2. Cleaned Schema' as Stage,
    'general_payments' as TableName,
    COUNT(*) as Row_Count, 
    FORMAT(SUM(amount_usd), 2) as Total_Spend_Formatted,
    SUM(amount_usd) as Total_Spend_Raw
FROM general_payments

UNION ALL

SELECT 
    '3. Star Warehouse' as Stage,
    'fact_payments' as TableName,
    COUNT(*) as Row_Count, 
    FORMAT(SUM(amount_usd), 2) as Total_Spend_Formatted,
    SUM(amount_usd) as Total_Spend_Raw
FROM fact_payments;

-- =========================================================================
-- DRILL DOWN: MISSING PAYMENTS CHECK
-- Purpose: If there is a variance, where is it?
-- =========================================================================
/*
SELECT 'Missing In Fact' as Issue, COUNT(*) as Count, SUM(amount_usd) as Value
FROM general_payments g
LEFT JOIN fact_payments f ON g.payment_id = f.record_id -- Assuming semantic link, or use Join logic
WHERE f.payment_key IS NULL;
*/
