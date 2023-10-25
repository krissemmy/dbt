-- Set the materialization method for this model , v='1'
{{ config(
    materialized='table'
) }}


WITH products AS (
    SELECT 
        product_id,
        department AS product_department,
        cost AS product_cost,
        retail_price AS product_retail_price
    FROM {{ ref('my_first',"stg_ecommerce_products") }} 
)

SELECT
    order_items.order_item_id,
    order_items.order_id,
    order_items.user_id,
    order_items.product_id,

    --Order Price
    order_items.item_sale_price,

    --Product details
    products.product_department,
    products.product_cost,
    products.product_retail_price,

    --Estimated Columns
    order_items.item_sale_price - products.product_cost AS item_profit,
    products.product_retail_price - order_items.item_sale_price AS item_discount

FROM {{ ref("stg_ecommerce_order_items") }} AS order_items
LEFT JOIN products
ON order_items.product_id = products.product_id