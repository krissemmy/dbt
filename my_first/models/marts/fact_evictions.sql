WITH source AS (
    SELECT
        eviction_id,
        supervisor_district,
        ROW_NUMBER() OVER () AS source_row_number
    FROM {{ ref('stg_eviction') }}
),
location_data AS (
    SELECT
        location_id,
        ROW_NUMBER() OVER () AS location_row_number
    FROM {{ ref('dim_location') }}
),
reason_data AS (
    SELECT
        reason_id,
        ROW_NUMBER() OVER () AS reason_row_number
    FROM {{ ref('dim_reason') }}
),
date_data AS (
    SELECT
        date,
        ROW_NUMBER() OVER () AS date_row_number
    FROM {{ ref('dim_date') }}
)
SELECT
    source.eviction_id as eviction_id,
    location_data.location_id as location_id,
    reason_data.reason_id as reason_id,
    date_data.date as date_id,
    source.supervisor_district as supervisor_district
FROM source
JOIN location_data ON source.source_row_number = location_data.location_row_number
JOIN reason_data ON source.source_row_number = reason_data.reason_row_number
JOIN date_data ON source.source_row_number = date_data.date_row_number