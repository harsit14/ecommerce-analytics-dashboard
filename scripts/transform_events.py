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
print("TRANSFORMING AND LOADING EVENTS")
print("="*60)

# First, check how many rows in staging
cursor.execute("SELECT COUNT(*) FROM events_staging")
staging_count = cursor.fetchone()[0]
print(f"\nRows in staging table: {staging_count:,}")

if staging_count == 0:
    print("⚠ No data in staging table. Run imports first.")
    exit(1)

print("\nThis will take 20-40 minutes...")
print("Inserting into partitioned events table...")

# Transform and insert
cursor.execute("""
INSERT INTO events (event_time, event_type, product_id, category_code, brand, price, user_id, user_session)
SELECT 
    event_time,
    event_type,
    product_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
FROM events_staging
""")

rows_inserted = cursor.rowcount
conn.commit()

print(f"✓ Inserted {rows_inserted:,} rows into partitioned events table")

# Verify distribution across partitions
print("\nPartition distribution:")
partitions = [
    "events_2019_10", "events_2019_11", "events_2019_12",
    "events_2020_01", "events_2020_02", "events_2020_03", "events_2020_04"
]

for partition in partitions:
    cursor.execute(f"SELECT COUNT(*) FROM {partition}")
    count = cursor.fetchone()[0]
    print(f"  {partition}: {count:,} rows")

# Drop staging table to save space
print("\nCleaning up staging table...")
cursor.execute("DROP TABLE events_staging")
conn.commit()

print("\n✓ Events table loaded successfully!")

cursor.close()
conn.close()