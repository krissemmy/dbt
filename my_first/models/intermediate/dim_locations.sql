WITH dim_location AS (
    SELECT
        CONCAT(zip, '_', address) AS location_id,
        address,
        city,
        state,
        zip,
        client_location.longitude AS longitude,
        client_location.latitude AS latitude
    FROM {{ ref("stg_eviction") }}
)

--SELECT DISTINCT *
--FROM dim_location

select * from  dim_location