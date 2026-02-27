-- =========================================================================
-- 1. GROWTH ENGINE (CRO VIEW) - LORENZ CURVE (RECIPIENT GRAIN)
-- Grain: Recipient (1.2M Rows). Optimized for "Whale Curve".
-- =========================================================================

WITH recipient_totals AS (
    SELECT 
        f.recipient_key,
        SUM(f.amount_usd) as total_life_spend
    FROM fact_payments f
    GROUP BY f.recipient_key
),
lorenz_calc AS (
    SELECT 
        recipient_key,
        total_life_spend,
        PERCENT_RANK() OVER (ORDER BY total_life_spend ASC) as pct_of_doctor,
        SUM(total_life_spend) OVER (ORDER BY total_life_spend ASC) / 
        SUM(total_life_spend) OVER () as pct_of_spend
    FROM recipient_totals
)
SELECT 
    l.recipient_key,
    l.total_life_spend,
    l.pct_of_doctor,
    l.pct_of_spend,
    CASE 
        WHEN l.total_life_spend > 50000 THEN 'Platinum (Top 1%)'
        WHEN l.total_life_spend > 10000 THEN 'Gold (Top 10%)'
        ELSE 'Standard'
    END as recipient_tier,
    r.recipient_name, 
    r.city,           
    r.state,
    CASE WHEN r.specialty IS NULL OR r.specialty = '' THEN 'Unknown Specialty' ELSE r.specialty END as specialty   
FROM lorenz_calc l
JOIN dim_recipient r ON l.recipient_key = r.recipient_key
WHERE l.total_life_spend > 0;


-- =========================================================================
-- 2. GROWTH ENGINE (CRO VIEW) - PRODUCT & NATURE (DIMENSION GRAIN)
-- Optimization: Uses dim_nature (INT Join) instead of TEXT Join.
-- =========================================================================
SELECT 
    d.month_name,
    d.month,
    COALESCE(prod.product_name, 'Unknown Product') as product_name,
    COALESCE(prod.product_category, 'Uncategorized') as product_category,
    COALESCE(n.payment_nature, 'Unknown Nature') as payment_nature,
    CASE WHEN r.specialty IS NULL OR r.specialty = '' THEN 'Unknown Specialty' ELSE r.specialty END as specialty,
    r.state, 
    SUM(f.amount_usd) as total_spend,
    COUNT(DISTINCT f.recipient_key) as total_physicians
FROM fact_payments f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product prod ON f.product_key = prod.product_key
JOIN dim_nature n ON f.nature_key = n.nature_key
JOIN dim_recipient r ON f.recipient_key = r.recipient_key
GROUP BY 1, 2, 3, 4, 5, 6, 7;


-- =========================================================================
-- 3. RISK MATRIX (CCO VIEW) - ZIP + SPECIALTY + NATURE GRAIN
-- Grain: Specialty + Zip + Nature (Slightly larger ~1M Rows). Optimized for Scatter + Map.
-- =========================================================================
SELECT 
    LEFT(r.zip, 5) as zip, -- Normalize to 5-digit for mapping
    -- Coalesce: Trust the Zip Code Map first, fallback to raw data
    COALESCE(ref.state_id, r.state) as state, 
    -- Fix: Handle both NULL and Empty String
    CASE WHEN r.specialty IS NULL OR r.specialty = '' THEN 'Unknown Specialty' ELSE r.specialty END as specialty,
    COALESCE(n.payment_nature, 'Unknown Nature') as payment_nature,
    COUNT(DISTINCT f.recipient_key) as total_physicians,
    SUM(f.amount_usd) as total_risk_exposure,
    AVG(f.amount_usd) as avg_payment_size,
    MAX(f.amount_usd) as max_payment_size
FROM fact_payments f
JOIN dim_recipient r ON f.recipient_key = r.recipient_key
LEFT JOIN ref_zip_city ref ON LEFT(r.zip, 5) = ref.zip_code
JOIN dim_nature n ON f.nature_key = n.nature_key
WHERE r.zip IS NOT NULL AND r.zip != '' 
GROUP BY 1, 2, 3, 4
HAVING total_risk_exposure > 5000;


-- =========================================================================
-- 4. LOGISTICS NETWORK (COO VIEW) - LAT/LNG CLUSTERS
-- Grain: Lat/Lng + State (Aggregated).
-- =========================================================================
SELECT 
    ROUND(r.lat, 2) as lat_cluster,
    ROUND(r.lng, 2) as lng_cluster,
    r.state,
    r.city,
    p.product_category, 
    COALESCE(tdf.forecast_signal, 'NO SIGNAL') as forecast_signal,
    COUNT(*) as total_unit_volume,
    SUM(f.amount_usd) as total_cluster_value,
    LEAST(
        (ST_Distance_Sphere(POINT(r.lng, r.lat), POINT(-95.36, 29.76)) / 1609.34), -- Houston
        (ST_Distance_Sphere(POINT(r.lng, r.lat), POINT(-87.62, 41.87)) / 1609.34), -- Chicago
        (ST_Distance_Sphere(POINT(r.lng, r.lat), POINT(-74.00, 40.71)) / 1609.34), -- New York
        (ST_Distance_Sphere(POINT(r.lng, r.lat), POINT(-75.16, 39.95)) / 1609.34), -- Philadelphia
        (ST_Distance_Sphere(POINT(r.lng, r.lat), POINT(-96.79, 32.77)) / 1609.34)  -- Dallas
    ) as nearest_hub_distance_miles
FROM fact_payments f
JOIN dim_recipient r ON f.recipient_key = r.recipient_key
JOIN dim_product p ON f.product_key = p.product_key
LEFT JOIN tableau_demand_forecast tdf 
    ON r.city = tdf.city 
    AND r.state = tdf.state 
    AND p.product_name = tdf.product_name
WHERE p.product_type LIKE '%Device%' 
  AND r.lat IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 9
HAVING nearest_hub_distance_miles > 250;

-- =========================================================================
-- 5. DEMAND SENSING (SUPPLY CHAIN VIEW) - FORECAST SIGNALS
-- Grain: City + Product (Aggregated). 
-- PREREQUISITE: Run 'SOURCE analysis/27_demand_signaling.sql;' first!
-- =========================================================================
SELECT * FROM tableau_demand_forecast 
WHERE forecast_signal != 'MAINTENANCE (Stable Demand)'
  AND state IS NOT NULL 
  AND state != ''
  AND UPPER(city) NOT IN ('APO', 'FPO', 'DPO')
  AND state NOT IN ('AE', 'AP', 'AA');

