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
print("COMPUTING SESSION METRICS (CHUNKED)")
print("="*60)

# Truncate sessions table
cursor.execute("TRUNCATE TABLE sessions CASCADE")
conn.commit()

# Process each partition separately
partitions = [
    ("2019-10", "events_2019_10"),
    ("2019-11", "events_2019_11"),
    ("2019-12", "events_2019_12"),
    ("2020-01", "events_2020_01"),
    ("2020-02", "events_2020_02"),
    ("2020-03", "events_2020_03"),
    ("2020-04", "events_2020_04")
]

total_sessions = 0

for month, partition in partitions:
    print(f"\nProcessing {month}...")
    
    cursor.execute(f"""
    INSERT INTO sessions (session_id, user_id, session_start, session_duration_seconds, event_count, has_purchase, total_revenue)
    SELECT 
        ABS(('x' || substr(md5(user_session::TEXT || user_id::TEXT), 1, 15))::bit(60)::bigint) as session_id,
        user_id,
        MIN(event_time) as session_start,
        EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time)))::INTEGER as session_duration_seconds,
        COUNT(*) as event_count,
        BOOL_OR(event_type = 'purchase') as has_purchase,
        COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 0) as total_revenue
    FROM {partition}
    WHERE user_session IS NOT NULL
    GROUP BY user_session, user_id
    ON CONFLICT (session_id) DO UPDATE SET
        session_start = LEAST(sessions.session_start, EXCLUDED.session_start),
        session_duration_seconds = GREATEST(sessions.session_duration_seconds, EXCLUDED.session_duration_seconds),
        event_count = sessions.event_count + EXCLUDED.event_count,
        has_purchase = sessions.has_purchase OR EXCLUDED.has_purchase,
        total_revenue = sessions.total_revenue + EXCLUDED.total_revenue
    """)
    
    rows = cursor.rowcount
    total_sessions += rows
    conn.commit()
    
    print(f"  ✓ {rows:,} sessions from {month}")

print(f"\n{'='*60}")
print(f"✓ Total sessions computed: {total_sessions:,}")
print("="*60)

cursor.close()
conn.close()