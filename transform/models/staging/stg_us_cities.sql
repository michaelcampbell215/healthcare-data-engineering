{{ config(materialized='view') }}
WITH source AS (
    select * from {{ ref('uscities') }}
),
-- BigQuery replaces your recursive CTE with UNNEST(SPLIT())
zip_splitter AS (
    select
        city,
        state_id,
        county_name,
        military,
        cast(population as int64) as population,
        cast(ranking as int64) as ranking,
        cast(lat as numeric) as lat,
        cast(lng as numeric) as lng,
        zip_code
    from source,
    UNNEST(SPLIT(zips, ' ')) AS zip_code
    where zips is not null
),
ranked_zips AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY zip_code ORDER BY population DESC, ranking ASC) as rn
    FROM zip_splitter
)

SELECT * EXCEPT(rn) FROM ranked_zips where rn = 1