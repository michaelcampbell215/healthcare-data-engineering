-- =========================================================================
-- TEST SCRIPT: Specialty Cleaning (Regex Syntax Fix)
-- =========================================================================

SELECT 
    recipient_specialty as raw_specialty,
    
    TRIM(BOTH '|' FROM REPLACE(recipient_specialty, 'Allopathic & Osteopathic Physicians', '')) as target_allopathic,
    
    TRIM(BOTH '|' FROM REPLACE(recipient_specialty, 'Physician Assistants & Advanced Practice Nursing Providers', '')) as target_pa,
    
    TRIM(BOTH '|' FROM REPLACE(recipient_specialty, 'Podiatric Medicine & Surgery Service Providers', '')) as target_podiatry

FROM general_payments
WHERE recipient_specialty LIKE '%&%'
LIMIT 20;
