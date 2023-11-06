WITH dim_date AS (
    SELECT
        file_date AS date,
        constraints_date,
        EXTRACT(YEAR FROM file_date) AS year,
        EXTRACT(MONTH FROM file_date) AS month,
        EXTRACT(DAY FROM file_date) AS day,
        FORMAT_DATE('%A', file_date) AS weekday,
        EXTRACT(QUARTER FROM file_date) AS quarter,
        EXTRACT(WEEK FROM file_date) AS week_number,
        CASE WHEN EXTRACT(DAYOFWEEK FROM file_date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS weekday_weekend,
        CASE WHEN EXTRACT(DAY FROM file_date) IN (1, 2, 3) AND EXTRACT(MONTH FROM file_date) = 12 THEN 'Holiday' ELSE 'Non-Holiday' END AS holiday,
        IF(EXTRACT(MONTH FROM file_date) = 2 AND MOD(EXTRACT(YEAR FROM file_date), 4) = 0, 'Leap Year', 'Non-Leap Year') AS leap_year,
        CASE
        WHEN EXTRACT(MONTH FROM file_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM file_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM file_date) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM file_date) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Unknown'
        END AS season
    FROM {{ ref("stg_eviction") }}
)

SELECT DISTINCT *
FROM dim_date
