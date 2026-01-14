-- Kontra PostgreSQL Test Data
-- Creates tables with ~1000 rows and intentional data quality issues for testing

-- Main test table: users
CREATE TABLE public.users (
    user_id SERIAL PRIMARY KEY,
    email VARCHAR(255),
    username VARCHAR(100) NOT NULL,
    age INTEGER,
    status VARCHAR(50),
    balance DECIMAL(10,2),
    is_premium BOOLEAN,
    created_at TIMESTAMP DEFAULT NOW(),
    country_code CHAR(2)
);

-- Insert 1000 rows with controlled patterns:
-- - email: NULL every 50th row (2% nulls)
-- - age: 18-77 range
-- - status: 4 values (low cardinality)
-- - balance: random 0-10000
-- - is_premium: alternating true/false
-- - country_code: 5 values (low cardinality)
INSERT INTO public.users (email, username, age, status, balance, is_premium, country_code)
SELECT
    CASE WHEN i % 50 = 0 THEN NULL ELSE 'user' || i || '@example.com' END,
    'user_' || i,
    18 + (i % 60),
    (ARRAY['active', 'inactive', 'pending', 'suspended'])[1 + (i % 4)],
    ROUND((random() * 10000)::numeric, 2),
    (i % 2 = 0),
    (ARRAY['US', 'UK', 'DE', 'FR', 'JP'])[1 + (i % 5)]
FROM generate_series(1, 1000) AS i;

-- Add duplicate emails for testing unique rule
INSERT INTO public.users (email, username, age, status, balance, is_premium, country_code)
VALUES
    ('duplicate@example.com', 'dup_user_1', 30, 'active', 100.00, true, 'US'),
    ('duplicate@example.com', 'dup_user_2', 31, 'active', 200.00, false, 'US');

-- Secondary table: products (for testing different schemas)
CREATE TABLE public.products (
    product_id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(100),
    in_stock BOOLEAN DEFAULT true
);

INSERT INTO public.products (sku, name, price, category, in_stock)
SELECT
    'SKU-' || LPAD(i::text, 5, '0'),
    'Product ' || i,
    ROUND((5 + random() * 495)::numeric, 2),
    (ARRAY['Electronics', 'Clothing', 'Books', 'Home', 'Sports'])[1 + (i % 5)],
    (i % 10 != 0)
FROM generate_series(1, 500) AS i;

-- Table with all-unique column (for unique rule testing)
CREATE TABLE public.orders (
    order_id SERIAL PRIMARY KEY,
    order_number VARCHAR(20) UNIQUE NOT NULL,
    user_id INTEGER REFERENCES public.users(user_id),
    total_amount DECIMAL(12,2) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL
);

INSERT INTO public.orders (order_number, user_id, total_amount, order_date, status)
SELECT
    'ORD-' || TO_CHAR(NOW() - (i || ' days')::interval, 'YYYYMMDD') || '-' || LPAD(i::text, 4, '0'),
    1 + (i % 1000),
    ROUND((10 + random() * 990)::numeric, 2),
    NOW() - (i || ' days')::interval,
    (ARRAY['pending', 'processing', 'shipped', 'delivered', 'cancelled'])[1 + (i % 5)]
FROM generate_series(1, 2000) AS i;

-- Run ANALYZE to populate pg_stats
ANALYZE public.users;
ANALYZE public.products;
ANALYZE public.orders;

-- Verify data was inserted
DO $$
BEGIN
    RAISE NOTICE 'Test data created:';
    RAISE NOTICE '  - users: % rows', (SELECT COUNT(*) FROM public.users);
    RAISE NOTICE '  - products: % rows', (SELECT COUNT(*) FROM public.products);
    RAISE NOTICE '  - orders: % rows', (SELECT COUNT(*) FROM public.orders);
END $$;
