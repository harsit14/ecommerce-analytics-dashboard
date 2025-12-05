import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os

load_dotenv()

# Connect to postgres database
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database='postgres',
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

# Create database
try:
    cursor.execute("CREATE DATABASE ecommerce_analytics")
    print("✓ Database created")
except Exception as e:
    print(f"✓ Database already exists: {e}")

cursor.close()
conn.close()

# Connect to new database
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database='ecommerce_analytics',
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
cursor = conn.cursor()

print("\nCreating schema...")

# BRANDS
cursor.execute("""
CREATE TABLE IF NOT EXISTS brands (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(255) UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_brands_name ON brands(brand_name);
""")

# CATEGORIES  
cursor.execute("""
CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    category_code VARCHAR(500),
    category_level_1 VARCHAR(100),
    category_level_2 VARCHAR(100),
    category_level_3 VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_categories_code ON categories(category_code);
CREATE INDEX IF NOT EXISTS idx_categories_level1 ON categories(category_level_1);
""")

# USERS
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    total_sessions INTEGER DEFAULT 0,
    total_events INTEGER DEFAULT 0,
    total_purchases INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen);
""")

# SESSIONS
cursor.execute("""
CREATE TABLE IF NOT EXISTS sessions (
    session_id BIGINT PRIMARY KEY,
    user_id BIGINT REFERENCES users(user_id),
    session_start TIMESTAMP NOT NULL,
    session_duration_seconds INTEGER,
    event_count INTEGER DEFAULT 0,
    has_purchase BOOLEAN DEFAULT FALSE,
    total_revenue DECIMAL(10,2) DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_start ON sessions(session_start);
CREATE INDEX IF NOT EXISTS idx_sessions_purchase ON sessions(has_purchase);
""")

# PRODUCTS
cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    product_id BIGINT PRIMARY KEY,
    category_id INTEGER REFERENCES categories(category_id),
    brand_id INTEGER REFERENCES brands(brand_id),
    current_price DECIMAL(10,2),
    total_views INTEGER DEFAULT 0,
    total_carts INTEGER DEFAULT 0,
    total_purchases INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id);
CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand_id);
CREATE INDEX IF NOT EXISTS idx_products_conversion ON products(total_purchases, total_views);
""")

conn.commit()
print("✓ Dimension tables created")

# EVENTS partitioned table - DO NOT create foreign keys yet (for fast import)
try:
    cursor.execute("""
        CREATE TABLE events (
            event_time TIMESTAMP NOT NULL,
            event_type VARCHAR(20) NOT NULL,
            product_id BIGINT NOT NULL,
            category_code VARCHAR(500),
            brand VARCHAR(255),
            price DECIMAL(10,2),
            user_id BIGINT NOT NULL,
            user_session UUID NOT NULL
        ) PARTITION BY RANGE (event_time);
    """)
    print("✓ Events table created")
except Exception as e:
    print(f"✓ Events table exists: {e}")

# Create partitions
partitions = [
    ("events_2019_10", "2019-10-01", "2019-11-01"),
    ("events_2019_11", "2019-11-01", "2019-12-01"),
    ("events_2019_12", "2019-12-01", "2020-01-01"),
    ("events_2020_01", "2020-01-01", "2020-02-01"),
    ("events_2020_02", "2020-02-01", "2020-03-01"),
    ("events_2020_03", "2020-03-01", "2020-04-01"),
    ("events_2020_04", "2020-04-01", "2020-05-01")
]

for partition_name, start_date, end_date in partitions:
    try:
        cursor.execute(f"""
            CREATE TABLE {partition_name} PARTITION OF events
            FOR VALUES FROM ('{start_date}') TO ('{end_date}');
        """)
        print(f"✓ Partition {partition_name} created")
    except Exception as e:
        print(f"✓ Partition {partition_name} exists")

conn.commit()
print("\n✓ Schema created successfully!")

cursor.close()
conn.close()