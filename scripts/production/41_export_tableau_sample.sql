-- =========================================================================
-- TABLEAU EXPORT SCRIPT: RAPID PROTOTYPE SAMPLE
-- Purpose: Generate small sample datasets (5,000 rows) for Dashboard Design
-- Note: This is NOT the full dataset. Use this to build the UI while the main job runs.
-- =========================================================================

-- VIEW 1: CRO GROWTH ENGINE (SAMPLE)
SELECT 'Exporting Growth Metrics (SAMPLE)...' as Status;

WITH recipient_spend AS (
    SELECT 
        recipient_key,
        SUM(amount_usd) as total_lifetime_spend
    FROM fact_payments
    -- LIMIT for speed
    WHERE date_key BETWEEN 20240101 AND 20240331 -- Q1 Only
    GROUP BY recipient_key
)
SELECT 
    d.month_name,
    d.month,
    prod.product_name,
    g.payment_nature,
    CASE 
        WHEN rs.total_lifetime_spend > 50000 THEN 'Platinum (Top 1%)'
        WHEN rs.total_lifetime_spend > 10000 THEN 'Gold (Top 10%)'
        ELSE 'Standard'
    END as recipient_tier,
    SUM(f.amount_usd) as total_spend,
    COUNT(DISTINCT f.fact_key) as total_transactions
FROM fact_payments f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product prod ON f.product_key = prod.product_key
JOIN recipient_spend rs ON f.recipient_key = rs.recipient_key
JOIN general_payments g ON f.record_id = g.record_id 
WHERE d.year = 2024 AND d.month <= 3 -- Q1 Only
GROUP BY d.month_name, d.month, prod.product_name, g.payment_nature, recipient_tier
LIMIT 5000;

-- VIEW 2: CCO RISK MATRIX (SAMPLE)
SELECT 'Exporting Risk Metrics (SAMPLE)...' as Status;
SELECT 
    r.recipient_zip,
    r.specialty,
    CASE WHEN prod.product_name IS NULL THEN 'Unallocated' ELSE 'Allocated' END as allocation_status,
    AVG(f.amount_usd) as avg_payment_size,
    MAX(f.amount_usd) as max_payment_size,
    SUM(f.amount_usd) as total_risk_exposure
FROM fact_payments f
JOIN dim_recipient r ON f.recipient_key = r.recipient_key
LEFT JOIN dim_product prod ON f.product_key = prod.product_key
WHERE f.amount_usd > 100 -- Only look at material payments for the sample
GROUP BY 1, 2, 3
HAVING total_risk_exposure > 5000
LIMIT 5000;

-- VIEW 3: COO LOGISTICS NETWORK (SAMPLE)
SELECT 'Exporting Logistics Clusters (SAMPLE)...' as Status;
-- Simplified: No UNION ALL, just Product 1 for speed
SELECT 
    ROUND(r.lat, 1) as lat_cluster,
    ROUND(r.lng, 1) as lng_cluster,
    CASE 
        WHEN r.lat BETWEEN 28 AND 32 AND r.lng BETWEEN -98 AND -94 THEN 'Hub: Houston'
        WHEN r.lat BETWEEN 39 AND 42 AND r.lng BETWEEN -75 AND -72 THEN 'Hub: NYC'
        WHEN r.lat BETWEEN 40 AND 43 AND r.lng BETWEEN -89 AND -86 THEN 'Hub: Chicago'
        ELSE 'served_by_national_dc'
    END as logistics_hub_assignment,
    COUNT(*) as total_unit_volume
FROM general_payments p
JOIN dim_recipient r ON COALESCE(NULLIF(p.recipient_profile_id, ''), p.teaching_hospital_ccn) = r.recipient_id
WHERE (p.product_category_1 LIKE '%Device%' OR p.product_category_1 LIKE '%Supply%')
  AND r.lat IS NOT NULL
GROUP BY 1, 2, 3
LIMIT 5000;
