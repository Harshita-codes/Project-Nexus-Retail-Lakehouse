-- Adjust the catalog and schema to match the notebook widget values.
USE CATALOG nexus;
USE SCHEMA retail_nexus;

-- Category and channel performance
SELECT
  category,
  sales_channel,
  total_orders,
  total_units_sold,
  total_net_revenue
FROM gold_category_sales
ORDER BY total_net_revenue DESC;

-- Highest-spend customers
SELECT
  customer_id,
  customer_name,
  city,
  valid_orders,
  lifetime_net_spend
FROM gold_customer_spend
ORDER BY lifetime_net_spend DESC
LIMIT 20;

-- Validate the curated batch order status mix
SELECT order_status, COUNT(*) AS order_count
FROM silver_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- Review data-quality dispositions
SELECT 'products' AS entity, COUNT(*) AS rejected_rows
FROM quarantine_products
UNION ALL
SELECT 'customers', COUNT(*) FROM quarantine_customers
UNION ALL
SELECT 'orders', COUNT(*) FROM quarantine_orders;
