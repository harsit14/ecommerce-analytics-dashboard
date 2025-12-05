import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
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
storage_client = storage.Client(project='database-project-477917')
bucket = storage_client.bucket('ecom-behaviour-data')

csv_files = [
    "2019-Oct.csv", "2019-Nov.csv", "2019-Dec.csv",
    "2020-Jan.csv", "2020-Feb.csv", "2020-Mar.csv", "2020-Apr.csv"
]

print("=" * 60)
print("PHASE 1: EXTRACTING DIMENSION DATA FROM CSV FILES")
print("=" * 60)

all_brands = set()
all_categories = {}
all_users = {}
all_products = {}

print("\nDownloading and scanning CSV files...")

for csv_file in csv_files:
    print(f"\n{'='*50}")
    print(f"Processing {csv_file}...")
    print('='*50)
    
    # Download from GCS
    print(f"Downloading from GCS...")
    blob = bucket.blob(csv_file)
    blob.download_to_filename(csv_file)
    print(f"✓ Downloaded {csv_file}")
    
    # Process in chunks
    chunk_size = 1000000
    
    for chunk_num, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunk_size)):
        print(f"  Chunk {chunk_num + 1}: {len(chunk):,} rows", end=" ")
        
        # Brands
        brands = chunk[chunk['brand'].notna()]['brand'].unique()
        all_brands.update(brands)
        
        # Categories
        for cat_code in chunk[chunk['category_code'].notna()]['category_code'].unique():
            if cat_code not in all_categories:
                parts = str(cat_code).split('.')
                all_categories[cat_code] = {
                    'level_1': parts[0] if len(parts) > 0 else None,
                    'level_2': parts[1] if len(parts) > 1 else None,
                    'level_3': parts[2] if len(parts) > 2 else None
                }
        
        # Users
        user_times = chunk.groupby('user_id')['event_time'].agg(['min', 'max'])
        for user_id, row in user_times.iterrows():
            if user_id not in all_users:
                all_users[user_id] = {'first_seen': row['min'], 'last_seen': row['max']}
            else:
                if row['min'] < all_users[user_id]['first_seen']:
                    all_users[user_id]['first_seen'] = row['min']
                if row['max'] > all_users[user_id]['last_seen']:
                    all_users[user_id]['last_seen'] = row['max']
        
        # Products
        product_info = chunk.groupby('product_id').agg({
            'category_code': 'first',
            'brand': 'first',
            'price': 'mean'
        })
        for prod_id, row in product_info.iterrows():
            if prod_id not in all_products:
                all_products[prod_id] = {
                    'category_code': row['category_code'] if pd.notna(row['category_code']) else None,
                    'brand': row['brand'] if pd.notna(row['brand']) else None,
                    'price': row['price'] if pd.notna(row['price']) else None
                }
        
        print("✓")
    
    # Clean up
    os.remove(csv_file)
    print(f"✓ Completed {csv_file}")

print(f"\n{'='*60}")
print("EXTRACTION SUMMARY")
print('='*60)
print(f"  Brands: {len(all_brands):,}")
print(f"  Categories: {len(all_categories):,}")
print(f"  Users: {len(all_users):,}")
print(f"  Products: {len(all_products):,}")

print("\n" + "=" * 60)
print("PHASE 2: LOADING DIMENSION TABLES")
print("=" * 60)

conn = get_connection()
cursor = conn.cursor()

# BRANDS
print("\nLoading BRANDS table...")
cursor.execute("TRUNCATE TABLE brands RESTART IDENTITY CASCADE")
brands_data = [(brand,) for brand in sorted(all_brands) if brand]
execute_values(cursor, "INSERT INTO brands (brand_name) VALUES %s", brands_data, page_size=1000)
conn.commit()

cursor.execute("SELECT brand_id, brand_name FROM brands")
brand_mapping = {name: id for id, name in cursor.fetchall()}
print(f"✓ Loaded {len(brand_mapping):,} brands")

# CATEGORIES
print("\nLoading CATEGORIES table...")
cursor.execute("TRUNCATE TABLE categories RESTART IDENTITY CASCADE")
categories_data = [
    (code, lvl['level_1'], lvl['level_2'], lvl['level_3'])
    for code, lvl in all_categories.items()
]
execute_values(
    cursor,
    "INSERT INTO categories (category_code, category_level_1, category_level_2, category_level_3) VALUES %s",
    categories_data,
    page_size=1000
)
conn.commit()

cursor.execute("SELECT category_id, category_code FROM categories")
category_mapping = {code: id for id, code in cursor.fetchall() if code}

# Add NULL category
cursor.execute("INSERT INTO categories (category_code) VALUES (NULL) RETURNING category_id")
null_category_id = cursor.fetchone()[0]
conn.commit()

print(f"✓ Loaded {len(category_mapping):,} categories")

# USERS
print("\nLoading USERS table...")
cursor.execute("TRUNCATE TABLE users CASCADE")
users_data = [
    (uid, info['first_seen'], info['last_seen'], 0, 0, 0)
    for uid, info in all_users.items()
]
print(f"  Inserting {len(users_data):,} users in batches...")
for i in tqdm(range(0, len(users_data), 10000)):
    batch = users_data[i:i+10000]
    execute_values(
        cursor,
        "INSERT INTO users (user_id, first_seen, last_seen, total_sessions, total_events, total_purchases) VALUES %s",
        batch,
        page_size=5000
    )
    conn.commit()

print(f"✓ Loaded {len(all_users):,} users")

# PRODUCTS
print("\nLoading PRODUCTS table...")
cursor.execute("TRUNCATE TABLE products CASCADE")
products_data = []
for pid, info in all_products.items():
    cat_id = category_mapping.get(info['category_code'], null_category_id)
    brand_id = brand_mapping.get(info['brand'])
    products_data.append((pid, cat_id, brand_id, info['price'], 0, 0, 0))

print(f"  Inserting {len(products_data):,} products in batches...")
for i in tqdm(range(0, len(products_data), 10000)):
    batch = products_data[i:i+10000]
    execute_values(
        cursor,
        "INSERT INTO products (product_id, category_id, brand_id, current_price, total_views, total_carts, total_purchases) VALUES %s",
        batch,
        page_size=5000
    )
    conn.commit()

print(f"✓ Loaded {len(all_products):,} products")

cursor.close()
conn.close()

print("\n" + "=" * 60)
print("✓ ALL DIMENSION TABLES LOADED SUCCESSFULLY!")
print("=" * 60)
print("\nNext step: Load events table")