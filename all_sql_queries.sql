-- =====================================================
-- RETAIL ANALYTICS - COMPLETE SQL COLLECTION
-- BigQuery + thelook_ecommerce dataset
-- =====================================================

--First Investigation: "How Big Is This Dataset?"
SELECT 'orders' AS table_name, COUNT(*) AS row_count 
FROM `bigquery-public-data.thelook_ecommerce.orders`
UNION ALL
SELECT 'order_items', COUNT(*) 
FROM `bigquery-public-data.thelook_ecommerce.order_items`
UNION ALL
SELECT 'products', COUNT(*) 
FROM `bigquery-public-data.thelook_ecommerce.products`
UNION ALL
SELECT 'users', COUNT(*) 
FROM `bigquery-public-data.thelook_ecommerce.users`;

--Second Investigation: "What Does the Data Look Like?"
SELECT * 
FROM `bigquery-public-data.thelook_ecommerce.order_items` 
LIMIT 20;

--Third Investigation: "What Time Period Do We Have?"
SELECT 
  MIN(created_at) AS first_sale, 
  MAX(created_at) AS last_sale
FROM `bigquery-public-data.thelook_ecommerce.order_items`;

--Query 1: Monthly Revenue Trends
WITH monthly_data AS (
  SELECT
    FORMAT_DATE('%Y-%m', DATE(created_at)) AS month,
    order_id,
    sale_price
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
  WHERE sale_price > 0 
    AND status = 'Complete'
)
SELECT 
  month,
  ROUND(SUM(sale_price), 2) AS revenue,
  COUNT(DISTINCT order_id) AS orders,
  ROUND(SUM(sale_price) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM monthly_data
GROUP BY month
ORDER BY month;

--Query 2: Best-Selling Products
SELECT
  p.category,
  p.brand,
  ROUND(SUM(oi.sale_price), 2) AS total_revenue,
  COUNT(*) AS units_sold,
  ROUND(AVG(oi.sale_price), 2) AS avg_price
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.products` p
  ON oi.product_id = p.id
WHERE oi.sale_price > 0 
  AND oi.status = 'Complete'
GROUP BY p.category, p.brand
ORDER BY total_revenue DESC
LIMIT 30;

--Query 3: Customer Loyalty Analysis
WITH customer_summary AS (
  SELECT 
    user_id,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(sale_price), 2) AS lifetime_spending,
    MIN(DATE(created_at)) AS first_purchase,
    MAX(DATE(created_at)) AS last_purchase
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
  WHERE sale_price > 0 
    AND status = 'Complete'
  GROUP BY user_id
)
SELECT
  CASE 
    WHEN total_orders = 1 THEN 'One-time buyer'
    WHEN total_orders = 2 THEN 'Bought twice'
    WHEN total_orders <= 5 THEN 'Regular (3-5 orders)'
    ELSE 'Super loyal (6+ orders)'
  END AS customer_type,
  COUNT(*) AS number_of_customers,
  ROUND(AVG(lifetime_spending), 2) AS avg_lifetime_value,
  ROUND(SUM(lifetime_spending), 2) AS total_revenue_from_segment
FROM customer_summary
GROUP BY customer_type
ORDER BY total_revenue_from_segment DESC;

--Query 4: Geographic Performance
SELECT
  u.country,
  COUNT(DISTINCT oi.user_id) AS customers,
  COUNT(DISTINCT oi.order_id) AS orders,
  ROUND(SUM(oi.sale_price), 2) AS total_revenue,
  ROUND(AVG(oi.sale_price), 2) AS avg_item_price
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.users` u
  ON oi.user_id = u.id
WHERE oi.sale_price > 0 
  AND oi.status = 'Complete'
GROUP BY u.country
HAVING total_revenue > 1000
ORDER BY total_revenue DESC
LIMIT 15;

--Advanced Query 1: A/B Test Analysis
WITH promo_window AS (
  SELECT 
    DATE('2023-11-15') AS promo_start,
    DATE('2023-11-30') AS promo_end
),
labeled_orders AS (
  SELECT
    oi.*,
    CASE 
      WHEN DATE(oi.created_at) BETWEEN pw.promo_start AND pw.promo_end THEN 'During Promo'
      WHEN DATE(oi.created_at) BETWEEN DATE_SUB(pw.promo_start, INTERVAL 15 DAY) 
           AND DATE_SUB(pw.promo_start, INTERVAL 1 DAY) THEN 'Pre Promo'
      WHEN DATE(oi.created_at) BETWEEN DATE_ADD(pw.promo_end, INTERVAL 1 DAY)
           AND DATE_ADD(pw.promo_end, INTERVAL 15 DAY) THEN 'Post Promo'
      ELSE 'Other'
    END AS period
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  CROSS JOIN promo_window pw
  WHERE oi.sale_price > 0 AND oi.status = 'Complete'
)
SELECT
  period,
  COUNT(DISTINCT order_id) AS orders,
  ROUND(AVG(sale_price), 2) AS avg_item_value,
  ROUND(SUM(sale_price) / COUNT(DISTINCT order_id), 2) AS avg_order_value,
  COUNT(*) AS total_items
FROM labeled_orders
WHERE period IN ('Pre Promo', 'During Promo', 'Post Promo')
GROUP BY period
ORDER BY 
  CASE period 
    WHEN 'Pre Promo' THEN 1 
    WHEN 'During Promo' THEN 2 
    WHEN 'Post Promo' THEN 3 
  END;

--Advanced Query 2: Customer Churn Risk
WITH last_purchase AS (
  SELECT 
    user_id,
    MAX(DATE(created_at)) AS last_order_date,
    DATE_DIFF(CURRENT_DATE(), MAX(DATE(created_at)), DAY) AS days_since_last_order,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(sale_price), 2) AS lifetime_value
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
  WHERE sale_price > 0 AND status = 'Complete'
  GROUP BY user_id
),
churn_segments AS (
  SELECT
    *,
    CASE 
      WHEN days_since_last_order <= 30 THEN 'Active (0-30 days)'
      WHEN days_since_last_order <= 90 THEN 'At Risk (31-90 days)'
      WHEN days_since_last_order <= 180 THEN 'Churning (91-180 days)'
      ELSE 'Churned (180+ days)'
    END AS churn_segment
  FROM last_purchase
)
SELECT
  churn_segment,
  COUNT(*) AS customers,
  ROUND(AVG(days_since_last_order), 1) AS avg_days_since_last_order,
  ROUND(AVG(total_orders), 1) AS avg_orders_per_customer,
  ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value,
  ROUND(SUM(lifetime_value), 2) AS segment_revenue
FROM churn_segments
GROUP BY churn_segment
ORDER BY 
  CASE churn_segment
    WHEN 'Active (0-30 days)' THEN 1
    WHEN 'At Risk (31-90 days)' THEN 2
    WHEN 'Churning (91-180 days)' THEN 3
    WHEN 'Churned (180+ days)' THEN 4
  END;

--Advanced Query 3: Customer Retention Cohorts
WITH first_purchase AS (
  SELECT 
    user_id,
    FORMAT_DATE('%Y-%m', MIN(DATE(created_at))) AS cohort_month,
    MIN(DATE(created_at)) AS first_order_date
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
  WHERE sale_price > 0 AND status = 'Complete'
  GROUP BY user_id
),
user_activities AS (
  SELECT 
    fp.user_id,
    fp.cohort_month,
    fp.first_order_date,
    FORMAT_DATE('%Y-%m', DATE(oi.created_at)) AS order_month,
    DATE_DIFF(
      DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE(oi.created_at)), '-01')),
      DATE(CONCAT(fp.cohort_month, '-01')),
      MONTH
    ) AS period_number
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN first_purchase fp ON oi.user_id = fp.user_id
  WHERE oi.sale_price > 0 AND oi.status = 'Complete'
),
cohort_table AS (
  SELECT
    cohort_month,
    period_number,
    COUNT(DISTINCT user_id) AS customers_in_period
  FROM user_activities
  GROUP BY cohort_month, period_number
),
cohort_sizes AS (
  SELECT
    cohort_month,
    COUNT(DISTINCT user_id) AS cohort_size
  FROM first_purchase
  GROUP BY cohort_month
)
SELECT
  ct.cohort_month,
  cs.cohort_size,
  ct.period_number,
  ct.customers_in_period,
  ROUND(100.0 * ct.customers_in_period / cs.cohort_size, 2) AS retention_rate
FROM cohort_table ct
JOIN cohort_sizes cs ON ct.cohort_month = cs.cohort_month
WHERE ct.period_number <= 12
ORDER BY ct.cohort_month, ct.period_number;

--Advanced Query 4: Revenue Forecasting
WITH monthly_data AS (
  SELECT
    FORMAT_DATE('%Y-%m', DATE(created_at)) AS month,
    DATE(CONCAT(FORMAT_DATE('%Y-%m', DATE(created_at)), '-01')) AS month_date,
    SUM(sale_price) AS revenue
  FROM `bigquery-public-data.thelook_ecommerce.order_items`
  WHERE sale_price > 0 AND status = 'Complete'
  GROUP BY month, month_date
  ORDER BY month_date
),
with_trends AS (
  SELECT
    *,
    LAG(revenue, 1) OVER (ORDER BY month_date) AS prev_month_revenue,
    LAG(revenue, 12) OVER (ORDER BY month_date) AS same_month_prev_year
  FROM monthly_data
)
SELECT
  month,
  revenue,
  prev_month_revenue,
  same_month_prev_year,
  ROUND((revenue - prev_month_revenue) / NULLIF(prev_month_revenue, 0) * 100, 2) AS month_over_month_growth,
  ROUND((revenue - same_month_prev_year) / NULLIF(same_month_prev_year, 0) * 100, 2) AS year_over_year_growth,
  ROUND(revenue * 1.05, 2) AS simple_forecast_next_month
FROM with_trends
WHERE prev_month_revenue IS NOT NULL
ORDER BY month_date DESC
LIMIT 12;


