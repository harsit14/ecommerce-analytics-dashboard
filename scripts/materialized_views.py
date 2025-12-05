import psycopg2
from dotenv import load_dotenv
import os
import time

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database='ecommerce_analytics',
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
cursor = conn.cursor()

print("="*60)
print("CREATING MATERIALIZED VIEWS FOR ANALYTICAL FEATURES")
print("="*60)

partitions = [
    "events_2019_10",
    "events_2019_11",
    "events_2019_12",
    "events_2020_01",
    "events_2020_02",
    "events_2020_03",
    "events_2020_04"
]

# ============================================================
# FEATURE 1: SALES FUNNEL VISUALIZATION
# ============================================================
print("\n" + "="*60)
print("FEATURE 1: SALES FUNNEL VISUALIZATION")
print("="*60)
print("Creating mv_sales_funnel...")

start_time = time.time()

cursor.execute("DROP MATERIALIZED VIEW IF EXISTS mv_sales_funnel CASCADE")

# Build UNION ALL query for all partitions
union_parts = []
for partition in partitions:
    union_parts.append(f"""
    SELECT 
        event_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT product_id) as unique_products
    FROM {partition}
    WHERE event_type IN ('view', 'cart', 'purchase')
    GROUP BY event_type
    """)

union_query = " UNION ALL ".join(union_parts)

cursor.execute(f"""
CREATE MATERIALIZED VIEW mv_sales_funnel AS
SELECT 
    event_type,
    SUM(event_count) as event_count,
    SUM(unique_users) as unique_users,
    SUM(unique_products) as unique_products
FROM (
    {union_query}
) combined
GROUP BY event_type
ORDER BY 
    CASE event_type
        WHEN 'view' THEN 1
        WHEN 'cart' THEN 2
        WHEN 'purchase' THEN 3
    END
""")

cursor.execute("CREATE INDEX idx_mv_sales_funnel_type ON mv_sales_funnel(event_type)")
conn.commit()

elapsed = time.time() - start_time
print(f"✓ mv_sales_funnel created in {elapsed:.2f} seconds")

# Verify
cursor.execute("SELECT * FROM mv_sales_funnel")
results = cursor.fetchall()
print("\nSales Funnel Preview:")
for row in results:
    print(f"  {row[0]:10s}: {row[1]:,} events, {row[2]:,} users, {row[3]:,} products")

# ============================================================
# FEATURE 2: PRODUCT CONVERSION RATE LEADERBOARD
# ============================================================
print("\n" + "="*60)
print("FEATURE 2: PRODUCT CONVERSION RATE LEADERBOARD")
print("="*60)
print("Creating mv_product_conversion_rates...")

start_time = time.time()

cursor.execute("DROP MATERIALIZED VIEW IF EXISTS mv_product_conversion_rates CASCADE")
cursor.execute("""
CREATE MATERIALIZED VIEW mv_product_conversion_rates AS
SELECT 
    p.product_id,
    p.brand_id,
    b.brand_name,
    p.category_id,
    c.category_level_1,
    c.category_level_2,
    p.current_price,
    p.total_views,
    p.total_carts,
    p.total_purchases,
    CASE 
        WHEN p.total_views > 0 THEN 
            ROUND((p.total_purchases::NUMERIC / p.total_views::NUMERIC * 100), 2)
        ELSE 0 
    END as conversion_rate,
    CASE 
        WHEN p.total_views > 0 THEN 
            ROUND((p.total_carts::NUMERIC / p.total_views::NUMERIC * 100), 2)
        ELSE 0 
    END as cart_rate
FROM products p
LEFT JOIN brands b ON p.brand_id = b.brand_id
LEFT JOIN categories c ON p.category_id = c.category_id
WHERE p.total_views >= 100
ORDER BY conversion_rate DESC, total_purchases DESC
LIMIT 1000
""")

cursor.execute("CREATE INDEX idx_mv_product_conv_rate ON mv_product_conversion_rates(conversion_rate DESC)")
cursor.execute("CREATE INDEX idx_mv_product_conv_views ON mv_product_conversion_rates(total_views DESC)")
cursor.execute("CREATE INDEX idx_mv_product_conv_purchases ON mv_product_conversion_rates(total_purchases DESC)")
conn.commit()

elapsed = time.time() - start_time
print(f"✓ mv_product_conversion_rates created in {elapsed:.2f} seconds")

# Verify
cursor.execute("SELECT product_id, brand_name, total_views, total_purchases, conversion_rate FROM mv_product_conversion_rates LIMIT 5")
results = cursor.fetchall()
print("\nTop 5 Products by Conversion Rate:")
for i, row in enumerate(results, 1):
    brand = row[1] if row[1] else "Unknown"
    print(f"  {i}. Product {row[0]} ({brand}): {row[2]:,} views → {row[3]:,} purchases ({row[4]}%)")

# ============================================================
# FEATURE 3: ABANDONED CART ANALYSIS
# ============================================================
print("\n" + "="*60)
print("FEATURE 3: ABANDONED CART ANALYSIS")
print("="*60)
print("Creating mv_abandoned_carts...")

start_time = time.time()

cursor.execute("DROP MATERIALIZED VIEW IF EXISTS mv_abandoned_carts CASCADE")
cursor.execute("""
CREATE MATERIALIZED VIEW mv_abandoned_carts AS
SELECT 
    p.product_id,
    p.brand_id,
    b.brand_name,
    p.category_id,
    c.category_level_1,
    c.category_level_2,
    p.current_price,
    p.total_carts,
    p.total_purchases,
    (p.total_carts - p.total_purchases) as abandoned_count,
    CASE 
        WHEN p.total_carts > 0 THEN 
            ROUND(((p.total_carts - p.total_purchases)::NUMERIC / p.total_carts::NUMERIC * 100), 2)
        ELSE 0 
    END as abandonment_rate
FROM products p
LEFT JOIN brands b ON p.brand_id = b.brand_id
LEFT JOIN categories c ON p.category_id = c.category_id
WHERE p.total_carts > 10
ORDER BY abandoned_count DESC
LIMIT 1000
""")

cursor.execute("CREATE INDEX idx_mv_abandoned_count ON mv_abandoned_carts(abandoned_count DESC)")
cursor.execute("CREATE INDEX idx_mv_abandoned_rate ON mv_abandoned_carts(abandonment_rate DESC)")
cursor.execute("CREATE INDEX idx_mv_abandoned_brand ON mv_abandoned_carts(brand_id)")
conn.commit()

elapsed = time.time() - start_time
print(f"✓ mv_abandoned_carts created in {elapsed:.2f} seconds")

# Verify
cursor.execute("SELECT product_id, brand_name, total_carts, total_purchases, abandoned_count, abandonment_rate FROM mv_abandoned_carts LIMIT 5")
results = cursor.fetchall()
print("\nTop 5 Most Abandoned Products:")
for i, row in enumerate(results, 1):
    brand = row[1] if row[1] else "Unknown"
    print(f"  {i}. Product {row[0]} ({brand}): {row[2]:,} carts, {row[3]:,} purchases, {row[4]:,} abandoned ({row[5]}%)")

# ============================================================
# FEATURE 4: USER SESSION ANALYTICS
# ============================================================
print("\n" + "="*60)
print("FEATURE 4: USER SESSION ANALYTICS")
print("="*60)
print("Creating mv_user_session_analytics...")

start_time = time.time()

cursor.execute("DROP MATERIALIZED VIEW IF EXISTS mv_user_session_analytics CASCADE")
cursor.execute("""
CREATE MATERIALIZED VIEW mv_user_session_analytics AS
SELECT 
    'purchasers' as user_type,
    COUNT(DISTINCT s.user_id) as user_count,
    COUNT(DISTINCT s.session_id) as session_count,
    ROUND(AVG(s.session_duration_seconds), 2) as avg_session_duration_seconds,
    ROUND(AVG(s.event_count), 2) as avg_events_per_session,
    ROUND(AVG(s.total_revenue), 2) as avg_revenue_per_session,
    SUM(s.total_revenue) as total_revenue
FROM sessions s
WHERE s.has_purchase = TRUE

UNION ALL

SELECT 
    'non_purchasers' as user_type,
    COUNT(DISTINCT s.user_id) as user_count,
    COUNT(DISTINCT s.session_id) as session_count,
    ROUND(AVG(s.session_duration_seconds), 2) as avg_session_duration_seconds,
    ROUND(AVG(s.event_count), 2) as avg_events_per_session,
    0 as avg_revenue_per_session,
    0 as total_revenue
FROM sessions s
WHERE s.has_purchase = FALSE

UNION ALL

SELECT 
    'all_users' as user_type,
    COUNT(DISTINCT s.user_id) as user_count,
    COUNT(DISTINCT s.session_id) as session_count,
    ROUND(AVG(s.session_duration_seconds), 2) as avg_session_duration_seconds,
    ROUND(AVG(s.event_count), 2) as avg_events_per_session,
    ROUND(AVG(s.total_revenue), 2) as avg_revenue_per_session,
    SUM(s.total_revenue) as total_revenue
FROM sessions s
""")

cursor.execute("CREATE INDEX idx_mv_session_analytics_type ON mv_user_session_analytics(user_type)")
conn.commit()

elapsed = time.time() - start_time
print(f"✓ mv_user_session_analytics created in {elapsed:.2f} seconds")

# Verify
cursor.execute("SELECT * FROM mv_user_session_analytics ORDER BY user_type")
results = cursor.fetchall()
print("\nUser Session Analytics:")
for row in results:
    print(f"\n  {row[0].upper()}:")
    print(f"    Users: {row[1]:,}")
    print(f"    Sessions: {row[2]:,}")
    print(f"    Avg Duration: {row[3]:.2f} seconds")
    print(f"    Avg Events/Session: {row[4]:.2f}")
    print(f"    Avg Revenue/Session: ${row[5]:.2f}")
    print(f"    Total Revenue: ${row[6]:,.2f}")

# ============================================================
# FEATURE 5: BRAND POPULARITY TRENDS
# ============================================================
print("\n" + "="*60)
print("FEATURE 5: BRAND POPULARITY TRENDS")
print("="*60)
print("Creating mv_brand_popularity_trends...")
print("This will take 5-10 minutes...")

start_time = time.time()

cursor.execute("DROP MATERIALIZED VIEW IF EXISTS mv_brand_popularity_trends CASCADE")

# Build UNION ALL query for all partitions
union_parts = []
for partition in partitions:
    union_parts.append(f"""
    SELECT 
        DATE(e.event_time) as date,
        e.brand,
        b.brand_id,
        COUNT(*) FILTER (WHERE e.event_type = 'view') as views,
        COUNT(*) FILTER (WHERE e.event_type = 'cart') as carts,
        COUNT(*) FILTER (WHERE e.event_type = 'purchase') as purchases,
        COUNT(DISTINCT e.user_id) as unique_users,
        SUM(CASE WHEN e.event_type = 'purchase' THEN e.price ELSE 0 END) as revenue
    FROM {partition} e
    LEFT JOIN brands b ON e.brand = b.brand_name
    WHERE e.brand IS NOT NULL
    GROUP BY DATE(e.event_time), e.brand, b.brand_id
    """)

union_query = " UNION ALL ".join(union_parts)

cursor.execute(f"""
CREATE MATERIALIZED VIEW mv_brand_popularity_trends AS
SELECT 
    date,
    brand,
    brand_id,
    SUM(views) as views,
    SUM(carts) as carts,
    SUM(purchases) as purchases,
    SUM(unique_users) as unique_users,
    SUM(revenue) as revenue
FROM (
    {union_query}
) combined
GROUP BY date, brand, brand_id
ORDER BY date DESC, purchases DESC
""")

cursor.execute("CREATE INDEX idx_mv_brand_trends_date ON mv_brand_popularity_trends(date DESC)")
cursor.execute("CREATE INDEX idx_mv_brand_trends_brand ON mv_brand_popularity_trends(brand)")
cursor.execute("CREATE INDEX idx_mv_brand_trends_purchases ON mv_brand_popularity_trends(purchases DESC)")
conn.commit()

elapsed = time.time() - start_time
print(f"✓ mv_brand_popularity_trends created in {elapsed:.2f} seconds")

# Verify
cursor.execute("""
SELECT brand, SUM(purchases) as total_purchases, SUM(revenue) as total_revenue
FROM mv_brand_popularity_trends
GROUP BY brand
ORDER BY total_purchases DESC
LIMIT 5
""")
results = cursor.fetchall()
print("\nTop 5 Brands by Total Purchases:")
for i, row in enumerate(results, 1):
    print(f"  {i}. {row[0]}: {row[1]:,} purchases, ${row[2]:,.2f} revenue")

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "="*60)
print("MATERIALIZED VIEWS SUMMARY")
print("="*60)

views_info = [
    ("mv_sales_funnel", "Sales funnel event counts"),
    ("mv_product_conversion_rates", "Top 1000 products by conversion rate"),
    ("mv_abandoned_carts", "Top 1000 products by abandoned carts"),
    ("mv_user_session_analytics", "Session metrics by user type"),
    ("mv_brand_popularity_trends", "Daily brand performance metrics")
]

for view_name, description in views_info:
    cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
    count = cursor.fetchone()[0]
    print(f"  ✓ {view_name}: {count:,} rows - {description}")

print("\n" + "="*60)
print("✓ ALL MATERIALIZED VIEWS CREATED SUCCESSFULLY!")
print("="*60)

print("\n" + "="*60)
print("REFRESH INSTRUCTIONS")
print("="*60)
print("To refresh materialized views after data updates, run:")
print("  python scripts/refresh_materialized_views.py")
print("\nOr manually in SQL:")
print("  REFRESH MATERIALIZED VIEW mv_sales_funnel;")
print("  REFRESH MATERIALIZED VIEW mv_product_conversion_rates;")
print("  REFRESH MATERIALIZED VIEW mv_abandoned_carts;")
print("  REFRESH MATERIALIZED VIEW mv_user_session_analytics;")
print("  REFRESH MATERIALIZED VIEW mv_brand_popularity_trends;")
print("="*60)

cursor.close()
conn.close()