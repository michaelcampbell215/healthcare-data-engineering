DROP TABLE IF EXISTS stg_us_cities;
CREATE TABLE stg_us_cities (
    city TEXT,
    state_id TEXT,
    county_name TEXT,
    lat TEXT,
    lng TEXT,
    population TEXT,
    military TEXT,
    ranking TEXT,
    zips TEXT
);



LOAD DATA LOCAL INFILE 'C:/Users/Mike/Documents/Analysis Projects/healthcare spend/uscities.csv'
INTO TABLE stg_us_cities
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
    city,
    @dummy,
    state_id,
    @dummy,
    @dummy,
    county_name,
    lat,
    lng,
    population,
    @dummy,
    @dummy,
    military,
    @dummy,
    @dummy,
    ranking,
    @zips_raw,
    @dummy
) 
SET zips = @zips_raw;

select * from stg_us_cities limit 10;