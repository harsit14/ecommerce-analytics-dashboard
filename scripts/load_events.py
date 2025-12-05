import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import pandas as pd
from tqdm import tqdm
from google.cloud import storage
import time

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database='ecommerce_analytics',
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

# Initialize GCS
try:
    storage_client = storage.Client(project='database-project-477917')
    bucket = storage_client.bucket('ecom-behaviour-data')
    use_gcs = True
except:
    use_gcs = False

csv_files = [
    "2019-Oct.csv", "2019-Nov.csv", "2019-Dec.csv",
    "2020-Jan.csv", "2020-Feb.csv", "2020-Mar.csv", "2020-Apr.csv"
]

conn = get_connection()
cursor = conn.cursor()

# Recreate staging table
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
print("✓ Staging table created")

print("\n" + "="*60)
print("LOADING EVENTS IN CHUNKS")
print("="*60)

total_rows = 0

for csv_file in csv_files:
    print(f"\n{'='*50}")
    print(f"Loading {csv_file}...")
    print('='*50)
    
    local_file = csv_file
    
    # Download from GCS
    if use_gcs:
        print("  Downloading from GCS...")
        blob = bucket.blob(csv_file)
        blob.download_to_filename(csv_file)
        print("  ✓ Downloaded")
    else:
        local_file = os.path.join('data', csv_file)
    
    # Process in chunks of 100k rows
    chunk_size = 100000
    chunk_num = 0
    
    for chunk in tqdm(pd.read_csv(local_file, chunksize=chunk_size), desc="  Processing chunks"):
        chunk_num += 1
        
        # Prepare data
        data = []
        for _, row in chunk.iterrows():
            data.append((
                row['event_time'],
                row['event_type'],
                row['product_id'],
                row['category_id'] if pd.notna(row['category_id']) else None,
                row['category_code'] if pd.notna(row['category_code']) else None,
                row['brand'] if pd.notna(row['brand']) else None,
                row['price'] if pd.notna(row['price']) else None,
                row['user_id'],
                row['user_session'] if pd.notna(row['user_session']) else None
            ))
        
        # Insert with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                execute_values(
                    cursor,
                    """
                    INSERT INTO events_staging 
                    (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)
                    VALUES %s
                    """,
                    data,
                    page_size=5000
                )
                conn.commit()
                total_rows += len(data)
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                print(f"\n  Connection lost on chunk {chunk_num}, attempt {attempt+1}/{max_retries}")
                time.sleep(5)
                conn = get_connection()
                cursor = conn.cursor()
                if attempt == max_retries - 1:
                    raise
        
        # Progress update every 10 chunks
        if chunk_num % 10 == 0:
            print(f"\n  Progress: {chunk_num} chunks, {total_rows:,} rows total")
    
    print(f"\n  ✓ Completed {csv_file}: {chunk_num} chunks")
    
    # Clean up
    if use_gcs and os.path.exists(csv_file):
        os.remove(csv_file)

print(f"\n{'='*60}")
print(f"✓ Total rows loaded: {total_rows:,}")
print("="*60)

cursor.close()
conn.close()

print("\nNext step: Run python scripts\\transform_events.py")