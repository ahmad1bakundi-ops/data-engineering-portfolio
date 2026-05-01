-- =============================================
-- Olist Analytical Queries
-- Week 1 Day 3: Multi-table JOINs
-- Author: Ahmad Bakundi
-- =============================================

-- Query 1: Orders with payment details
SELECT o.order_id, o.customer_id, o.order_status, 
       p.payment_type, p.payment_value
FROM orders o
JOIN payments p ON o.order_id = p.order_id
LIMIT 10;

-- Query 2: Total revenue by payment type
SELECT p.payment_type, 
       COUNT(*) as total_orders,
       ROUND(SUM(p.payment_value)::numeric, 2) as total_revenue,
       ROUND(AVG(p.payment_value)::numeric, 2) as avg_order_value
FROM orders o
JOIN payments p ON o.order_id = p.order_id
GROUP BY p.payment_type
ORDER BY total_revenue DESC;

-- Query 3: Revenue by customer city and order status
SELECT c.customer_city,
       o.order_status,
       COUNT(*) as total_orders,
       ROUND(SUM(p.payment_value)::numeric, 2) as total_revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN payments p ON o.order_id = p.order_id
GROUP BY c.customer_city, o.order_status
ORDER BY total_revenue DESC
LIMIT 10;
