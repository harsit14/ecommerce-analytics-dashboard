import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()
   
try:
    conn = psycopg2.connect(
           host=os.getenv('DB_HOST'),
           port=os.getenv('DB_PORT'),
           database=os.getenv('DB_NAME'),
           user=os.getenv('DB_USER'),
           password=os.getenv('DB_PASSWORD')
    )
    print("✅ Database connection successful!")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM mv_sales_funnel")
    print(f"✅ Sales funnel has {cursor.fetchone()[0]} rows")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")