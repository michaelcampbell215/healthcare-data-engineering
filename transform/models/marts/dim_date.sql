{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2013-01-01' as date)",
        end_date   = "cast('2026-12-31' as date)"
    ) }}
)

SELECT
    date_day                                                        AS full_date,
    EXTRACT(YEAR    FROM date_day)                                  AS year,
    EXTRACT(QUARTER FROM date_day)                                  AS quarter,
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date_day) AS STRING))    AS quarter_name,
    EXTRACT(MONTH   FROM date_day)                                  AS month,
    FORMAT_DATE('%B', date_day)                                     AS month_name,
    FORMAT_DATE('%b', date_day)                                     AS month_short,
    EXTRACT(WEEK    FROM date_day)                                  AS week_of_year,
    EXTRACT(DAY     FROM date_day)                                  AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date_day)                               AS day_of_week,
    FORMAT_DATE('%A', date_day)                                     AS day_name,
    FORMAT_DATE('%a', date_day)                                     AS day_short,
    EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7)                     AS is_weekend,
    FORMAT_DATE('%Y-%m', date_day)                                  AS year_month
FROM date_spine
