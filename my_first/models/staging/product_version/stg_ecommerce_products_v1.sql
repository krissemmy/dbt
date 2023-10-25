-- Set the materialization method for this model
{{ config(
    materialized='table'
) }}

with source as (

    select * from {{ source('thelook_ecommerce', 'products') }}

)
    select
        id as product_id,
        cost,
        retail_price,
        department,
        -- brand -- new column added in v2
        
        --unused columns
        {# category,
        name,
        brand,
        retail_price,
        department,
        sku,

        distribution_center_id #}

    from source