-- Set the materialization method for this model
{{ config(
    materialized='table'
) }}

with source as (

    select * from {{ source('thelook_ecommerce', 'order_items') }}

)

    select
        id as order_item_id,
        order_id,
        user_id,
        product_id,
        sale_price as item_sale_price

        {# inventory_item_id,
        status,
        created_at,
        shipped_at,
        delivered_at,
        returned_at,
        sale_price #}

    from source