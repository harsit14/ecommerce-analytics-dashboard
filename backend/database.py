"""
Database connection management for E-commerce Analytics API
"""
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
import os
from contextlib import contextmanager

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '104.198.184.12'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_analytics'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD')
}

# Connection pool
connection_pool = None

def init_connection_pool(minconn=1, maxconn=10):
    """Initialize the database connection pool"""
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn,
            maxconn,
            **DB_CONFIG
        )
        print("✅ Database connection pool initialized successfully")
    except Exception as e:
        print(f"❌ Error initializing connection pool: {e}")
        raise

def get_connection():
    """Get a connection from the pool"""
    global connection_pool
    # Auto-initialize pool on first use (for Cloud Run)
    if connection_pool is None:
        init_connection_pool(minconn=1, maxconn=10)
    return connection_pool.getconn()

def return_connection(conn):
    """Return a connection to the pool"""
    if connection_pool:
        connection_pool.putconn(conn)

@contextmanager
def get_db_cursor():
    """
    Context manager for database operations
    Usage:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT ...")
            results = cursor.fetchall()
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        return_connection(conn)

def close_connection_pool():
    """Close all connections in the pool"""
    if connection_pool:
        connection_pool.closeall()
        print("✅ Database connection pool closed")
