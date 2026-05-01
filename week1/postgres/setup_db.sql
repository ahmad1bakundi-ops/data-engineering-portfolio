-- Olist Database Setup
-- Week 1: Data Engineering Portfolio
-- Author: Ahmad Bakundi

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_delivered_timestamp TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp)
VALUES ('ord_001', 'cust_abc', 'delivered', '2024-01-15 10:30:00');

SELECT * FROM orders;
