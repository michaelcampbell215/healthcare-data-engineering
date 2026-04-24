DROP TABLE IF EXISTS ref_zip_city;
CREATE TABLE ref_zip_city (
	zip_code VARCHAR(5),                                                                                          
	city VARCHAR(100),                                                                                                                 
    state_id CHAR(2),        
    county_name VARCHAR(100), 
    population VARCHAR(10),
    ranking int(2),
    military VARCHAR(10),
    lat DECIMAL(10, 6),
    lng DECIMAL(10, 6),
    PRIMARY KEY (zip_code),
    INDEX idx_city (city),                                                                       
    INDEX idx_state (state_id)                                                                                                        
 );      


INSERT INTO ref_zip_city (zip_code, city, state_id, county_name, population, ranking, military, lat, lng)
WITH RECURSIVE zip_splitter AS (
    SELECT
        city, state_id, county_name, military, population, ranking, lat, lng,
        SUBSTRING_INDEX(zips, ' ', 1) AS zip_code,
        SUBSTRING(zips, LENGTH(SUBSTRING_INDEX(zips, ' ', 1)) + 2) AS remainder
    FROM stg_us_cities
    WHERE zips != ''

    UNION ALL

    SELECT
        city, state_id, county_name, military, population, ranking, lat, lng,
        SUBSTRING_INDEX(remainder, ' ', 1),
        IF(LOCATE(' ', remainder) > 0, SUBSTRING(remainder, LOCATE(' ', remainder) + 1), '')
    FROM zip_splitter
    WHERE remainder != ''
),
ranked_zips AS (
	SELECT
		zip_code,
		city,
		state_id,
		county_name,
		military,
		population,
		ranking,
		lat,
		lng,
		ROW_NUMBER() OVER(PARTITION BY zip_code ORDER BY population DESC, ranking ASC) AS rn
	FROM zip_splitter
)
SELECT  zip_code, city, state_id, county_name, population, ranking, military, lat, lng FROM ranked_zips WHERE rn = 1;
   

SELECT * FROM ref_zip_city limit 100;   
SELECT COUNT(*) as total_mapped_zips FROM ref_zip_city;