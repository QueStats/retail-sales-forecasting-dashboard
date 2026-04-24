-- ============================================================
-- Retail Sales Dashboard SQL Queries
-- Purpose:
-- Build Tableau-ready sales summary tables from the cleaned
-- superstore table loaded by the R pipeline.
--
-- Notes:
-- - This script assumes a cleaned table named superstore already exists.
-- - Date parsing is handled in R before the data is written to SQLite.
-- - Forecasting, regression modeling, anomaly detection, and decision
--   labels are handled in R because those steps are more readable there.
-- ============================================================

-- ----------------------------
-- 1. Monthly sales table
-- ----------------------------

DROP TABLE IF EXISTS monthly_sales;

CREATE TABLE monthly_sales AS
SELECT
  strftime('%Y-%m', Order_Date) AS month,
  ROUND(SUM(Sales), 2) AS revenue
FROM superstore
GROUP BY strftime('%Y-%m', Order_Date)
ORDER BY month;

-- ----------------------------
-- 2. Time-series feature base
--    No leakage: only prior months are used
-- ----------------------------

DROP TABLE IF EXISTS sales_features_base;

CREATE TABLE sales_features_base AS
SELECT
  month,
  revenue,
  CAST(substr(month, 1, 4) AS INTEGER) AS year,
  CAST(substr(month, 6, 2) AS INTEGER) AS month_num,
  LAG(revenue, 1) OVER (ORDER BY month) AS prev_month,
  LAG(revenue, 2) OVER (ORDER BY month) AS two_months_ago,
  ROUND(
    AVG(revenue) OVER (
      ORDER BY month
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ),
    2
  ) AS rolling_avg_3
FROM monthly_sales
ORDER BY month;

-- ----------------------------
-- 3. Category performance
-- ----------------------------

DROP TABLE IF EXISTS category_sales;

CREATE TABLE category_sales AS
SELECT
  Category,
  ROUND(SUM(Sales), 2) AS revenue,
  ROUND(
    100.0 * SUM(Sales) / SUM(SUM(Sales)) OVER (),
    2
  ) AS share_pct
FROM superstore
GROUP BY Category
ORDER BY revenue DESC;

-- ----------------------------
-- 4. Region performance
-- ----------------------------

DROP TABLE IF EXISTS region_sales;

CREATE TABLE region_sales AS
SELECT
  Region,
  ROUND(SUM(Sales), 2) AS revenue,
  ROUND(
    100.0 * SUM(Sales) / SUM(SUM(Sales)) OVER (),
    2
  ) AS share_pct
FROM superstore
GROUP BY Region
ORDER BY revenue DESC;

-- ----------------------------
-- 5. Top customers
--    Share is calculated against total company revenue.
-- ----------------------------

DROP TABLE IF EXISTS top_customers;

CREATE TABLE top_customers AS
WITH customer_totals AS (
  SELECT
    Customer_Name,
    SUM(Sales) AS revenue
  FROM superstore
  GROUP BY Customer_Name
),
company_total AS (
  SELECT SUM(Sales) AS total_revenue
  FROM superstore
)
SELECT
  c.Customer_Name,
  ROUND(c.revenue, 2) AS revenue,
  ROUND(100.0 * c.revenue / t.total_revenue, 2) AS total_share_pct
FROM customer_totals c
CROSS JOIN company_total t
ORDER BY c.revenue DESC
LIMIT 10;

-- ----------------------------
-- 6. Shipping performance
-- ----------------------------

DROP TABLE IF EXISTS shipping_summary;

CREATE TABLE shipping_summary AS
SELECT
  Ship_Mode,
  ROUND(AVG(julianday(Ship_Date) - julianday(Order_Date)), 2) AS avg_ship_days,
  COUNT(*) AS order_count
FROM superstore
GROUP BY Ship_Mode
ORDER BY avg_ship_days;

-- ----------------------------
-- 7. Subcategory performance
-- ----------------------------

DROP TABLE IF EXISTS subcategory_sales;

CREATE TABLE subcategory_sales AS
SELECT
  Sub_Category,
  ROUND(SUM(Sales), 2) AS revenue,
  ROUND(
    100.0 * SUM(Sales) / SUM(SUM(Sales)) OVER (),
    2
  ) AS share_pct
FROM superstore
GROUP BY Sub_Category
ORDER BY revenue DESC;

-- ----------------------------
-- 8. Region by month
-- ----------------------------

DROP TABLE IF EXISTS region_month_sales;

CREATE TABLE region_month_sales AS
SELECT
  strftime('%Y-%m', Order_Date) AS month,
  Region,
  ROUND(SUM(Sales), 2) AS revenue
FROM superstore
GROUP BY strftime('%Y-%m', Order_Date), Region
ORDER BY month, Region;

-- ----------------------------
-- 9. Subcategory by month
-- ----------------------------

DROP TABLE IF EXISTS subcategory_month_sales;

CREATE TABLE subcategory_month_sales AS
SELECT
  strftime('%Y-%m', Order_Date) AS month,
  Sub_Category,
  ROUND(SUM(Sales), 2) AS revenue
FROM superstore
GROUP BY strftime('%Y-%m', Order_Date), Sub_Category
ORDER BY month, Sub_Category;
