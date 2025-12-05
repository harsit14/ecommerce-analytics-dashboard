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

# Make user_session nullable in events table
cursor.execute("ALTER TABLE events ALTER COLUMN user_session DROP NOT NULL")
conn.commit()
print("✓ Fixed")
cursor.close()
conn.close()
exit()