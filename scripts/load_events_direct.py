import psycopg2
from dotenv import load_dotenv
import os
from tqdm import tqdm
from google.cloud import storage

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database='ecommerce_analytics',
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

# Initialize GCS client
try:
    storage_client = storage.Client(project='database-project-477917')
    bucket = storage_client.bucket('ecom-behaviour-data')
    use_gcs = True
except:
    print("GCS auth not set up. Will use local files from 'data' folder.")
    use_gcs = False

csv_files = [
    "2019-Oct.csv", "2019-Nov.csv", "2019-Dec.csv",
    "2020-Jan.csv", "2020-Feb.csv", "2020-Mar.csv", "2020-Apr.csv"
]

conn = get_connection()
cursor = conn.cursor()

# Recreate staging table with nullable user_session
print("Creating staging table...")
cursor.execute("DROP TABLE IF EXISTS events_staging")
cursor.execute("""
CREATE TABLE events_staging (
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    product_id BIGINT NOT NULL,
    category_id BIGINT,
    category_code VARCHAR(500),
    brand VARCHAR(255),
    price DECIMAL(10,2),
    user_id BIGINT NOT NULL,
    user_session UUID
)
""")
conn.commit()
print("✓ Staging table created (user_session nullable)")

print("\n" + "="*60)
print("LOADING EVENTS FROM CSV FILES")
print("="*60)

total_rows = 0

for csv_file in csv_files:
    print(f"\n{'='*50}")
    print(f"Loading {csv_file}...")
    print('='*50)
    
    local_file = csv_file
    
    # Download from GCS if available
    if use_gcs:
        print("  Downloading from GCS...")
        blob = bucket.blob(csv_file)
        blob.download_to_filename(csv_file)
        print("  ✓ Downloaded")
    else:
        local_file = os.path.join('data', csv_file)
    
    # Use PostgreSQL COPY command
    print("  Importing to database...")
    with open(local_file, 'r', encoding='utf-8') as f:
        cursor.copy_expert(
            """
            COPY events_staging (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)
            FROM STDIN WITH (FORMAT CSV, HEADER true, NULL '')
            """,
            f
        )
    
    rows = cursor.rowcount
    total_rows += rows
    conn.commit()
    
    print(f"  ✓ Loaded {rows:,} rows")
    
    # Clean up
    if use_gcs and os.path.exists(csv_file):
        os.remove(csv_file)

print(f"\n{'='*60}")
print(f"✓ Total rows loaded: {total_rows:,}")
print("="*60)

cursor.close()
conn.close()

print("\nNext step: Run python scripts\\transform_events.py")