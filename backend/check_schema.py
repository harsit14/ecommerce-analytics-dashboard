"""
Check actual column names in materialized views
"""
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

cursor = conn.cursor()

views = [
    'mv_sales_funnel',
    'mv_product_conversion_rates',
    'mv_abandoned_carts',
    'mv_user_session_analytics',
    'mv_brand_popularity_trends'
]

for view in views:
    print(f"\n{'='*60}")
    print(f"VIEW: {view}")
    print('='*60)
    
    # Get column names
    cursor.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{view}'
        ORDER BY ordinal_position
    """)
    
    columns = cursor.fetchall()
    print("Columns:")
    for col_name, col_type in columns:
        print(f"  - {col_name} ({col_type})")
    
    # Get sample data
    cursor.execute(f"SELECT * FROM {view} LIMIT 2")
    sample = cursor.fetchall()
    
    print("\nSample data (first 2 rows):")
    col_names = [desc[0] for desc in cursor.description]
    print(f"  Columns: {col_names}")
    for i, row in enumerate(sample, 1):
        print(f"  Row {i}: {row}")

cursor.close()
conn.close()

print("\n" + "="*60)
print("✅ Schema check complete!")
