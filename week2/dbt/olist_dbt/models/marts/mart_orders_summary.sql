-- Mart model: Business-ready orders summary
-- Used for dashboards and executive reporting

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

summary AS (
    SELECT
        customer_city,
        order_status,
        order_value_category,
        payment_type,
        order_year,
        order_month,
        COUNT(*) AS total_orders,
        ROUND(SUM(payment_value), 2) AS total_revenue,
        ROUND(AVG(payment_value), 2) AS avg_order_value,
        ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
        COUNTIF(is_late) AS late_orders,
        ROUND(COUNTIF(is_late) / COUNT(*) * 100, 1) AS late_pct
    FROM orders
    GROUP BY
        customer_city,
        order_status,
        order_value_category,
        payment_type,
        order_year,
        order_month
)

SELECT * FROM summary
ORDER BY total_revenue DESC
