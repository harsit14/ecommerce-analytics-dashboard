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
print("REFRESHING MATERIALIZED VIEWS")
print("="*60)

views = [
    "mv_sales_funnel",
    "mv_product_conversion_rates",
    "mv_abandoned_carts",
    "mv_user_session_analytics",
    "mv_brand_popularity_trends"
]

total_start = time.time()

for view in views:
    print(f"\nRefreshing {view}...")
    start_time = time.time()
    
    try:
        # Use CONCURRENTLY to allow reads during refresh (requires unique index)
        cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
        conn.commit()
        
        elapsed = time.time() - start_time
        print(f"✓ {view} refreshed in {elapsed:.2f} seconds")
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {view}")
        count = cursor.fetchone()[0]
        print(f"  Rows: {count:,}")
        
    except Exception as e:
        print(f"✗ Error refreshing {view}: {e}")
        print(f"  Trying without CONCURRENTLY...")
        
        # Fallback: refresh without CONCURRENTLY
        try:
            cursor.execute(f"REFRESH MATERIALIZED VIEW {view}")
            conn.commit()
            
            elapsed = time.time() - start_time
            print(f"✓ {view} refreshed in {elapsed:.2f} seconds")
            
            cursor.execute(f"SELECT COUNT(*) FROM {view}")
            count = cursor.fetchone()[0]
            print(f"  Rows: {count:,}")
            
        except Exception as e2:
            print(f"✗ Failed to refresh {view}: {e2}")
            conn.rollback()

total_elapsed = time.time() - total_start

print("\n" + "="*60)
print(f"✓ REFRESH COMPLETED IN {total_elapsed:.2f} SECONDS")
print("="*60)

cursor.close()
conn.close()