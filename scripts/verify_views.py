import psycopg2
from dotenv import load_dotenv
import os

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
print("MATERIALIZED VIEWS VERIFICATION")
print("="*60)

# 1. List all materialized views
print("\n1. List all materialized views:")
cursor.execute("""
    SELECT matviewname, 
           pg_size_pretty(pg_total_relation_size('public.'||matviewname)) as size
    FROM pg_matviews 
    WHERE matviewname LIKE 'mv_%' 
    ORDER BY matviewname
""")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}")

# 2. Sales Funnel
print("\n2. Sales Funnel:")
cursor.execute("SELECT * FROM mv_sales_funnel ORDER BY CASE event_type WHEN 'view' THEN 1 WHEN 'cart' THEN 2 WHEN 'purchase' THEN 3 END")
for row in cursor.fetchall():
    print(f"  {row[0]:10s}: {row[1]:,} events, {row[2]:,} unique users")

# 3. Product Conversion (Top 5)
print("\n3. Product Conversion Rates (Top 5):")
cursor.execute("""
    SELECT product_id, brand_name, total_views, total_purchases, conversion_rate 
    FROM mv_product_conversion_rates LIMIT 5
""")
for i, row in enumerate(cursor.fetchall(), 1):
    brand = row[1] if row[1] else "Unknown"
    print(f"  {i}. Product {row[0]} ({brand}): {row[2]:,} views → {row[3]:,} purchases ({row[4]}%)")

# 4. Abandoned Carts (Top 5)
print("\n4. Abandoned Carts (Top 5):")
cursor.execute("""
    SELECT product_id, brand_name, total_carts, abandoned_count, abandonment_rate 
    FROM mv_abandoned_carts LIMIT 5
""")
for i, row in enumerate(cursor.fetchall(), 1):
    brand = row[1] if row[1] else "Unknown"
    print(f"  {i}. Product {row[0]} ({brand}): {row[2]:,} carts, {row[3]:,} abandoned ({row[4]}%)")

# 5. Session Analytics
print("\n5. Session Analytics:")
cursor.execute("SELECT * FROM mv_user_session_analytics ORDER BY user_type")
for row in cursor.fetchall():
    print(f"\n  {row[0].upper()}:")
    print(f"    Users: {row[1]:,}")
    print(f"    Sessions: {row[2]:,}")
    print(f"    Avg Duration: {row[3]:.2f} seconds ({row[3]/60:.2f} minutes)")
    print(f"    Avg Events/Session: {row[4]:.2f}")
    print(f"    Avg Revenue/Session: ${row[5]:.2f}")

# 6. Brand Trends Summary
print("\n6. Brand Trends Summary:")
cursor.execute("""
    SELECT COUNT(*) as total_rows, 
           COUNT(DISTINCT brand) as num_brands,
           MIN(date) as earliest, 
           MAX(date) as latest
    FROM mv_brand_popularity_trends
""")
row = cursor.fetchone()
print(f"  Total rows: {row[0]:,}")
print(f"  Unique brands: {row[1]:,}")
print(f"  Date range: {row[2]} to {row[3]}")

# 7. Top 10 Brands
print("\n7. Top 10 Brands by Purchases:")
cursor.execute("""
    SELECT brand, SUM(purchases) as total_purchases, SUM(revenue) as total_revenue
    FROM mv_brand_popularity_trends
    GROUP BY brand
    ORDER BY total_purchases DESC
    LIMIT 10
""")
for i, row in enumerate(cursor.fetchall(), 1):
    print(f"  {i:2d}. {row[0]:20s}: {row[1]:,} purchases, ${row[2]:,.2f} revenue")

# 8. Row counts for all views
print("\n8. Row Counts:")
views = [
    "mv_sales_funnel",
    "mv_product_conversion_rates",
    "mv_abandoned_carts",
    "mv_user_session_analytics",
    "mv_brand_popularity_trends"
]
for view in views:
    cursor.execute(f"SELECT COUNT(*) FROM {view}")
    count = cursor.fetchone()[0]
    print(f"  {view:30s}: {count:,} rows")

print("\n" + "="*60)
print("✓ VERIFICATION COMPLETE!")
print("="*60)

cursor.close()
conn.close()