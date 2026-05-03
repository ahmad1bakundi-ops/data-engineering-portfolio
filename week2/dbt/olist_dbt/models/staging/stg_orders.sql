-- Staging model: Clean and standardise raw orders
-- Source: olist_dataset.orders_transformed

WITH source AS (
    SELECT * FROM {{ source('olist', 'orders_transformed') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        TIMESTAMP(order_purchase_timestamp) AS order_purchase_timestamp,
        TIMESTAMP(order_delivered_timestamp) AS order_delivered_timestamp,
        TIMESTAMP(order_estimated_delivery_date) AS order_estimated_delivery_date,
        CAST(delivery_days AS INT64) AS delivery_days,
        CAST(is_late AS BOOL) AS is_late,
        CAST(order_month AS INT64) AS order_month,
        CAST(order_year AS INT64) AS order_year,
        order_value_category,
        customer_city,
        customer_state,
        payment_type,
        CAST(payment_value AS FLOAT64) AS payment_value
    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM cleaned
