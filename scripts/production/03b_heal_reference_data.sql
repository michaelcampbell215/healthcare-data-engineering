-- =========================================================================
-- HEAL REFERENCE DATA FROM RAW TRANSACTIONS
-- Purpose: Find Zips in our data that are missing from ref_zip_city,
--          and "learn" their correct City/State from the crowd consensus.
-- Rationale: Doctors report their mailing address correctly 99% of the time.
--            If a zip (e.g. 44512) is missing from our reference map
--            but shows up 1,000 times in OH and once in TN, it belongs in OH.
-- =========================================================================

INSERT IGNORE INTO ref_zip_city (zip_code, city, state_id, lat, lng)
SELECT 
    zip_candidate,
    city_candidate,
    state_candidate,
    NULL as lat, -- We can't guess coordinates, but we can fix the State mapping
    NULL as lng
FROM (
    SELECT 
        LEFT(recipient_zip, 5) as zip_candidate,
        recipient_city as city_candidate,
        recipient_state as state_candidate,
        COUNT(*) as frequency,
        ROW_NUMBER() OVER(PARTITION BY LEFT(recipient_zip, 5) ORDER BY COUNT(*) DESC) as rn
    FROM general_payments
    WHERE recipient_zip IS NOT NULL 
      AND recipient_zip != ''
      AND recipient_state IS NOT NULL
      AND recipient_state != ''
      -- Only look for Zips we don't know about yet
      AND LEFT(recipient_zip, 5) NOT IN (SELECT zip_code FROM ref_zip_city)
    GROUP BY 1, 2, 3
) ranked
WHERE rn = 1 -- Take the most frequent City/State combo for this Zip
  AND frequency > 5; -- Ignore typos (must appear at least 5 times)

SELECT ROW_COUNT() as 'New Zips Learned from Raw Data';
