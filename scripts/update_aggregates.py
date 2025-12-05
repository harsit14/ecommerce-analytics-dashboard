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
print("UPDATING PRODUCT AND USER AGGREGATES")
print("="*60)

# Define partitions
partitions = [
    ("2019-10", "events_2019_10"),
    ("2019-11", "events_2019_11"),
    ("2019-12", "events_2019_12"),
    ("2020-01", "events_2020_01"),
    ("2020-02", "events_2020_02"),
    ("2020-03", "events_2020_03"),
    ("2020-04", "events_2020_04")
]

# ============================================================
# PART 1: UPDATE PRODUCT AGGREGATES
# ============================================================
print("\n" + "="*60)
print("PART 1: UPDATING PRODUCT AGGREGATES")
print("="*60)

# Reset product aggregates
print("\nResetting product aggregate columns...")
cursor.execute("""
UPDATE products 
SET total_views = 0, 
    total_carts = 0, 
    total_purchases = 0
""")
conn.commit()
print("✓ Product aggregates reset")

# Process each partition
for month, partition in partitions:
    print(f"\nProcessing {month}...")
    
    # Update views
    cursor.execute(f"""
    UPDATE products p
    SET total_views = total_views + e.view_count
    FROM (
        SELECT product_id, COUNT(*) as view_count
        FROM {partition}
        WHERE event_type = 'view'
        GROUP BY product_id
    ) e
    WHERE p.product_id = e.product_id
    """)
    views_updated = cursor.rowcount
    
    # Update carts
    cursor.execute(f"""
    UPDATE products p
    SET total_carts = total_carts + e.cart_count
    FROM (
        SELECT product_id, COUNT(*) as cart_count
        FROM {partition}
        WHERE event_type = 'cart'
        GROUP BY product_id
    ) e
    WHERE p.product_id = e.product_id
    """)
    carts_updated = cursor.rowcount
    
    # Update purchases
    cursor.execute(f"""
    UPDATE products p
    SET total_purchases = total_purchases + e.purchase_count
    FROM (
        SELECT product_id, COUNT(*) as purchase_count
        FROM {partition}
        WHERE event_type = 'purchase'
        GROUP BY product_id
    ) e
    WHERE p.product_id = e.product_id
    """)
    purchases_updated = cursor.rowcount
    
    conn.commit()
    
    print(f"  ✓ Views: {views_updated:,} products updated")
    print(f"  ✓ Carts: {carts_updated:,} products updated")
    print(f"  ✓ Purchases: {purchases_updated:,} products updated")

# Verify product aggregates
cursor.execute("""
SELECT 
    SUM(total_views) as total_views,
    SUM(total_carts) as total_carts,
    SUM(total_purchases) as total_purchases,
    COUNT(*) as total_products
FROM products
""")
result = cursor.fetchone()
print(f"\n{'='*60}")
print("PRODUCT AGGREGATES SUMMARY:")
print(f"  Total Views: {result[0]:,}")
print(f"  Total Carts: {result[1]:,}")
print(f"  Total Purchases: {result[2]:,}")
print(f"  Products with data: {result[3]:,}")
print("="*60)

# ============================================================
# PART 2: UPDATE USER AGGREGATES
# ============================================================
print("\n" + "="*60)
print("PART 2: UPDATING USER AGGREGATES")
print("="*60)

# Reset user aggregates
print("\nResetting user aggregate columns...")
cursor.execute("""
UPDATE users 
SET total_sessions = 0, 
    total_events = 0, 
    total_purchases = 0
""")
conn.commit()
print("✓ User aggregates reset")

# Update total_sessions from sessions table
print("\nUpdating total_sessions from sessions table...")
cursor.execute("""
UPDATE users u
SET total_sessions = s.session_count
FROM (
    SELECT user_id, COUNT(*) as session_count
    FROM sessions
    GROUP BY user_id
) s
WHERE u.user_id = s.user_id
""")
sessions_updated = cursor.rowcount
conn.commit()
print(f"✓ Sessions updated for {sessions_updated:,} users")

# Update total_events and total_purchases from events table (by partition)
print("\nUpdating total_events and total_purchases from events...")
for month, partition in partitions:
    print(f"\nProcessing {month}...")
    
    # Update total_events
    cursor.execute(f"""
    UPDATE users u
    SET total_events = total_events + e.event_count
    FROM (
        SELECT user_id, COUNT(*) as event_count
        FROM {partition}
        GROUP BY user_id
    ) e
    WHERE u.user_id = e.user_id
    """)
    events_updated = cursor.rowcount
    
    # Update total_purchases
    cursor.execute(f"""
    UPDATE users u
    SET total_purchases = total_purchases + e.purchase_count
    FROM (
        SELECT user_id, COUNT(*) as purchase_count
        FROM {partition}
        WHERE event_type = 'purchase'
        GROUP BY user_id
    ) e
    WHERE u.user_id = e.user_id
    """)
    purchases_updated = cursor.rowcount
    
    conn.commit()
    
    print(f"  ✓ Events: {events_updated:,} users updated")
    print(f"  ✓ Purchases: {purchases_updated:,} users updated")

# Verify user aggregates
cursor.execute("""
SELECT 
    SUM(total_sessions) as total_sessions,
    SUM(total_events) as total_events,
    SUM(total_purchases) as total_purchases,
    COUNT(*) as total_users,
    COUNT(CASE WHEN total_purchases > 0 THEN 1 END) as users_with_purchases
FROM users
""")
result = cursor.fetchone()
print(f"\n{'='*60}")
print("USER AGGREGATES SUMMARY:")
print(f"  Total Sessions: {result[0]:,}")
print(f"  Total Events: {result[1]:,}")
print(f"  Total Purchases: {result[2]:,}")
print(f"  Total Users: {result[3]:,}")
print(f"  Users with purchases: {result[4]:,}")
print("="*60)

print("\n" + "="*60)
print("✓ ALL AGGREGATES UPDATED SUCCESSFULLY!")
print("="*60)

cursor.close()
conn.close()