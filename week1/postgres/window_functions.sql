-- =============================================
-- Window Functions — Advanced SQL
-- Week 1 Day 6: DE Interview Preparation
-- Author: Ahmad Bakundi
-- =============================================

-- Query 1: ROW_NUMBER — rank each customer's orders by date
SELECT order_id,
       customer_id,
       order_purchase_timestamp,
       ROW_NUMBER() OVER (
           PARTITION BY customer_id 
           ORDER BY order_purchase_timestamp
       ) as order_number
FROM orders_transformed
LIMIT 15;

-- Query 2: RANK — top 3 highest paying orders per city
SELECT * FROM (
    SELECT customer_city,
           order_id,
           payment_value,
           RANK() OVER (
               PARTITION BY customer_city 
               ORDER BY payment_value DESC
           ) as rank
    FROM orders_transformed
) ranked
WHERE rank <= 3
ORDER BY customer_city, rank;

-- Query 3: LAG — compare each order value to previous order
SELECT order_id,
       customer_city,
       order_purchase_timestamp,
       payment_value,
       LAG(payment_value) OVER (
           PARTITION BY customer_city
           ORDER BY order_purchase_timestamp
       ) as previous_order_value,
       payment_value - LAG(payment_value) OVER (
           PARTITION BY customer_city
           ORDER BY order_purchase_timestamp
       ) as value_difference
FROM orders_transformed
LIMIT 15;

-- Query 4: Running total revenue per city
SELECT customer_city,
       order_purchase_timestamp,
       payment_value,
       ROUND(SUM(payment_value) OVER (
           PARTITION BY customer_city
           ORDER BY order_purchase_timestamp
       )::numeric, 2) as running_total_revenue
FROM orders_transformed
ORDER BY customer_city, order_purchase_timestamp
LIMIT 20;

-- Query 5: Month over month revenue growth
SELECT order_month,
       order_year,
       ROUND(SUM(payment_value)::numeric, 2) as monthly_revenue,
       ROUND(LAG(SUM(payment_value)) OVER (
           ORDER BY order_year, order_month
       )::numeric, 2) as previous_month_revenue,
       ROUND((SUM(payment_value) - LAG(SUM(payment_value)) OVER (
           ORDER BY order_year, order_month
       ))::numeric, 2) as revenue_growth
FROM orders_transformed
GROUP BY order_month, order_year
ORDER BY order_year, order_month;
