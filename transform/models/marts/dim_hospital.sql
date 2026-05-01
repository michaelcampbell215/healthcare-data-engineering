WITH source AS (
    SELECT * FROM {{ ref('stg_general_payments') }}
    WHERE recipient_type = 'Covered Recipient Teaching Hospital'
        AND teaching_hospital_ccn IS NOT NULL
),
cities AS (
    SELECT * FROM {{ ref('stg_us_cities') }}
),
city_lookup AS (
    SELECT
        UPPER(TRIM(city))     AS city_upper,
        UPPER(TRIM(state_id)) AS state_upper,
        ARRAY_AGG(city ORDER BY population DESC LIMIT 1)[OFFSET(0)] AS city,
        ARRAY_AGG(lat  ORDER BY population DESC LIMIT 1)[OFFSET(0)] AS lat,
        ARRAY_AGG(lng  ORDER BY population DESC LIMIT 1)[OFFSET(0)] AS lng,
        MAX(population)                                              AS population
    FROM {{ ref('stg_us_cities') }}
    GROUP BY UPPER(TRIM(city)), UPPER(TRIM(state_id))
),
dim_hospital AS (
    SELECT DISTINCT
        source.teaching_hospital_ccn,
        {{ clean_hospital_name('source.teaching_hospital_name', 'source.recipient_city', 'source.recipient_state') }} AS teaching_hospital_name,
        source.recipient_type,
        source.recipient_address_line1,
        source.recipient_address_line2,
        COALESCE(cities.city, city_lookup.city, source.recipient_city) AS recipient_city,
        source.recipient_state,
        source.recipient_zip,
        source.recipient_country,
        COALESCE(cities.lat,        city_lookup.lat)        AS lat,
        COALESCE(cities.lng,        city_lookup.lng)        AS lng,
        COALESCE(cities.population, city_lookup.population) AS population
    FROM source
    LEFT JOIN cities
        ON cities.zip_code = SUBSTR(source.recipient_zip, 1, 5)
    LEFT JOIN city_lookup
        ON UPPER(TRIM(source.recipient_city))  = city_lookup.city_upper
        AND UPPER(TRIM(source.recipient_state)) = city_lookup.state_upper
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source.teaching_hospital_ccn
        ORDER BY source.program_year DESC
    ) = 1
)

SELECT * FROM dim_hospital
