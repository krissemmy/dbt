-- Set the materialization method for this model
{{ config(
    materialized='table'
) }}

WITH

  order_items_metrics AS (
	SELECT
	  order_id,
	  SUM(item_sale_price) AS total_sale_price,
	  SUM(product_cost) AS total_product_cost,
	  SUM(item_profit) AS total_profit,
	  SUM(item_discount) AS total_discount,
    FROM {{ ref("int_orders_items_products") }}
	   GROUP BY
	      order_id
  )

  SELECT
    -- Dimension of the oders
	o.order_id,
	o.created_at AS order_created_at,
	o.shipped_at AS order_shipped_at,
	o.delivered_at AS order_delivered_at,
	o.returned_at AS order_returned_at,
	o.status AS order_status,
	o.num_of_item AS num_items_ordered,

	-- Metrics or fact details of the orders
	m.total_product_cost,
	m.total_sale_price,
	m.total_discount,
	m.total_profit,

	-- Time between first order and current order
	TIMESTAMP_DIFF(o.created_at, user_order_data.first_order_created_at, DAY) AS days_since_first_order

  FROM
      {{ ref("stg_ecommerce_orders") }} AS o
  LEFT JOIN order_items_metrics AS  m
  ON o.order_id= m.order_id
  LEFT JOIN {{ ref("int_initial_order_created") }} AS user_order_data
  ON o.user_id = user_order_data.user_id