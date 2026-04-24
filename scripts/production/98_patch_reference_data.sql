-- =========================================================================
-- PATCH REFERENCE DATA - HOTFIX
-- Purpose: Manually insert missing Zip Codes into ref_zip_city.
-- Source: Manual correction for known missing zips (e.g., Youngstown, OH 44512)
-- =========================================================================

-- 1. Insert 44512 (Youngstown, OH) - The "TN" Ghost Dot
INSERT IGNORE INTO ref_zip_city (zip_code, city, state_id, county_name, lat, lng)
VALUES ('44512', 'Youngstown', 'OH', 'Mahoning', 41.056, -80.669);

-- Add more as discovered:
-- INSERT IGNORE INTO ref_zip_city ...

SELECT 'Patched ref_zip_city with missing Zips.' as Status;
