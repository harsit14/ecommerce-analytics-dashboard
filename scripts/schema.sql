-- Create database
CREATE DATABASE ecommerce_analytics;

-- Connect to it (in psql: \c ecommerce_analytics)

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- 1. BRANDS dimension
CREATE TABLE brands (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(255) UNIQUE NOT NULL
);
CREATE INDEX idx_brands_name ON brands(brand_name);

-- 2. CATEGORIES dimension
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_code VARCHAR(500),
    category_level_1 VARCHAR(100),
    category_level_2 VARCHAR(100),
    category_level_3 VARCHAR(100)
);
CREATE INDEX idx_categories_code ON categories(category_code);
CREATE INDEX idx_categories_level1 ON categories(category_level_1);

-- 3. USERS dimension
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    total_sessions INTEGER DEFAULT 0,
    total_events INTEGER DEFAULT 0,
    total_purchases INTEGER DEFAULT 0
);
CREATE INDEX idx_users_last_seen ON users(last_seen);

-- 4. SESSIONS dimension
CREATE TABLE sessions (
    session_id BIGINT PRIMARY KEY,
    user_id BIGINT REFERENCES users(user_id),
    session_start TIMESTAMP NOT NULL,
    session_duration_seconds INTEGER,
    event_count INTEGER DEFAULT 0,
    has_purchase BOOLEAN DEFAULT FALSE,
    total_revenue DECIMAL(10,2) DEFAULT 0
);
CREATE INDEX idx_sessions_user ON sessions(user_id);
CREATE INDEX idx_sessions_start ON sessions(session_start);
CREATE INDEX idx_sessions_purchase ON sessions(has_purchase);

-- 5. PRODUCTS dimension
CREATE TABLE products (
    product_id BIGINT PRIMARY KEY,
    category_id INTEGER REFERENCES categories(category_id),
    brand_id INTEGER REFERENCES brands(brand_id),
    current_price DECIMAL(10,2),
    total_views INTEGER DEFAULT 0,
    total_carts INTEGER DEFAULT 0,
    total_purchases INTEGER DEFAULT 0
);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_brand ON products(brand_id);
CREATE INDEX idx_products_conversion ON products(total_purchases, total_views);

-- 6. EVENTS fact table (PARTITIONED by month)
CREATE TABLE events (
    event_id BIGSERIAL,
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    product_id BIGINT REFERENCES products(product_id),
    category_id INTEGER REFERENCES categories(category_id),
    brand_id INTEGER REFERENCES brands(brand_id),
    price DECIMAL(10,2),
    user_id BIGINT REFERENCES users(user_id),
    session_id BIGINT REFERENCES sessions(session_id)
) PARTITION BY RANGE (event_time);

-- Create monthly partitions (Oct 2019 - Apr 2020)
CREATE TABLE events_2019_10 PARTITION OF events
    FOR VALUES FROM ('2019-10-01') TO ('2019-11-01');
    
CREATE TABLE events_2019_11 PARTITION OF events
    FOR VALUES FROM ('2019-11-01') TO ('2019-12-01');
    
CREATE TABLE events_2019_12 PARTITION OF events
    FOR VALUES FROM ('2019-12-01') TO ('2020-01-01');
    
CREATE TABLE events_2020_01 PARTITION OF events
    FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
    
CREATE TABLE events_2020_02 PARTITION OF events
    FOR VALUES FROM ('2020-02-01') TO ('2020-03-01');
    
CREATE TABLE events_2020_03 PARTITION OF events
    FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');
    
CREATE TABLE events_2020_04 PARTITION OF events
    FOR VALUES FROM ('2020-04-01') TO ('2020-05-01');

-- Indexes on partitioned table
CREATE INDEX idx_events_time ON events(event_time);
CREATE INDEX idx_events_type_time ON events(event_type, event_time);
CREATE INDEX idx_events_product_type ON events(product_id, event_type);
CREATE INDEX idx_events_session ON events(session_id);
CREATE INDEX idx_events_user ON events(user_id);

-- 7. Materialized Views
CREATE MATERIALIZED VIEW mv_daily_product_stats AS
SELECT 
    DATE(event_time) as date,
    product_id,
    COUNT(*) FILTER (WHERE event_type = 'view') as views,
    COUNT(*) FILTER (WHERE event_type = 'cart') as carts,
    COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases
FROM events
GROUP BY DATE(event_time), product_id;

CREATE INDEX idx_mv_daily_product_date ON mv_daily_product_stats(date);
CREATE INDEX idx_mv_daily_product_id ON mv_daily_product_stats(product_id);

CREATE MATERIALIZED VIEW mv_daily_brand_stats AS
SELECT 
    DATE(event_time) as date,
    b.brand_name,
    COUNT(*) FILTER (WHERE event_type = 'view') as views,
    COUNT(*) FILTER (WHERE event_type = 'cart') as carts,
    COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases
FROM events e
JOIN brands b ON e.brand_id = b.brand_id
GROUP BY DATE(event_time), b.brand_name;

CREATE INDEX idx_mv_daily_brand_date ON mv_daily_brand_stats(date);
CREATE INDEX idx_mv_daily_brand_name ON mv_daily_brand_stats(brand_name);