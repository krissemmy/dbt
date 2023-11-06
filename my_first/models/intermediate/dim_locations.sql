WITH dim_location AS (
    SELECT
        CONCAT(zip, '_', address) AS location_id,
        address,
        city,
        state,
        zip,
        longitude,
        latitude
    FROM {{ ref("stg_eviction") }}
)

--SELECT DISTINCT *
--FROM dim_location

select * from  dim_location