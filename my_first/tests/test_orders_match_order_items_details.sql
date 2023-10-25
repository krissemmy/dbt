{{ config(severity='warn') }}

WITH order_details AS (
    SELECT 
        order_id,
        COUNT(*) AS num_of_items_in_order

    FROM {{ ref('stg_ecommerce_order_items') }}
    GROUP BY 1
)

SELECT 
    o.*,
    od.*
FROM {{ ref("stg_ecommerce_orders") }} AS o
FULL OUTER JOIN order_details AS od USING(order_id)
WHERE 
    -- All orders should have at least 1 item, and every item should tie to an order
    o.order_id IS NULL
    OR od.order_id IS NULL
    -- The number of items ordered per order should be the same on both tables
    OR o.num_of_item != od.num_of_items_in_order
