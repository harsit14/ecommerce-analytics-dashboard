SELECT 
    schemaname, 
    matviewname, 
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size
FROM pg_matviews
WHERE matviewname LIKE 'mv_%'
ORDER BY matviewname;

SELECT * FROM mv_sales_funnel;
SELECT * FROM mv_product_conversion_rates LIMIT 20;
SELECT * FROM mv_abandoned_carts LIMIT 20;
SELECT * FROM mv_user_session_analytics;
SELECT brand, SUM(purchases) FROM mv_brand_popularity_trends GROUP BY brand LIMIT 10;