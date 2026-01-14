-- SQL Server test database initialization
-- Matches PostgreSQL test data structure

-- Create database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'kontra_test')
BEGIN
    CREATE DATABASE kontra_test;
END
GO

USE kontra_test;
GO

-- Drop tables if exist
IF OBJECT_ID('dbo.orders', 'U') IS NOT NULL DROP TABLE dbo.orders;
IF OBJECT_ID('dbo.products', 'U') IS NOT NULL DROP TABLE dbo.products;
IF OBJECT_ID('dbo.users', 'U') IS NOT NULL DROP TABLE dbo.users;
GO

-- Create users table
CREATE TABLE dbo.users (
    user_id INT IDENTITY(1,1) PRIMARY KEY,
    email NVARCHAR(255),
    username NVARCHAR(100) NOT NULL,
    age INT NOT NULL,
    status NVARCHAR(50) NOT NULL,
    balance DECIMAL(10,2) NOT NULL,
    is_premium BIT NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE(),
    country_code NCHAR(2) NOT NULL
);
GO

-- Insert 1002 rows matching PostgreSQL test data
-- user_id 1-1000: normal data
-- user_id 1001-1002: duplicates for testing
DECLARE @i INT = 1;
WHILE @i <= 1000
BEGIN
    INSERT INTO dbo.users (email, username, age, status, balance, is_premium, created_at, country_code)
    VALUES (
        CASE WHEN @i % 50 = 0 THEN NULL ELSE CONCAT('user', @i, '@example.com') END,
        CONCAT('user_', @i),
        18 + (@i % 63),
        CASE (@i % 4)
            WHEN 0 THEN 'active'
            WHEN 1 THEN 'inactive'
            WHEN 2 THEN 'pending'
            ELSE 'suspended'
        END,
        ROUND(RAND(CHECKSUM(NEWID())) * 10000, 2),
        CASE WHEN @i % 2 = 0 THEN 1 ELSE 0 END,
        DATEADD(SECOND, -@i * 60, GETUTCDATE()),
        CASE (@i % 5)
            WHEN 0 THEN 'US'
            WHEN 1 THEN 'UK'
            WHEN 2 THEN 'DE'
            WHEN 3 THEN 'FR'
            ELSE 'JP'
        END
    );
    SET @i = @i + 1;
END
GO

-- Add 2 duplicate emails for unique rule testing
INSERT INTO dbo.users (email, username, age, status, balance, is_premium, country_code)
VALUES
    ('duplicate@example.com', 'dup_user_1', 30, 'active', 100.00, 1, 'US'),
    ('duplicate@example.com', 'dup_user_2', 30, 'active', 100.00, 1, 'US');
GO

-- Create products table (500 rows)
CREATE TABLE dbo.products (
    product_id INT IDENTITY(1,1) PRIMARY KEY,
    sku NVARCHAR(50) NOT NULL,
    name NVARCHAR(255) NOT NULL,
    category NVARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    in_stock BIT NOT NULL
);
GO

DECLARE @j INT = 1;
WHILE @j <= 500
BEGIN
    INSERT INTO dbo.products (sku, name, category, price, in_stock)
    VALUES (
        CONCAT('SKU-', FORMAT(@j, '00000')),
        CONCAT('Product ', @j),
        CASE (@j % 5)
            WHEN 0 THEN 'Electronics'
            WHEN 1 THEN 'Clothing'
            WHEN 2 THEN 'Home'
            WHEN 3 THEN 'Sports'
            ELSE 'Books'
        END,
        ROUND(RAND(CHECKSUM(NEWID())) * 500 + 10, 2),
        CASE WHEN @j % 10 = 0 THEN 0 ELSE 1 END
    );
    SET @j = @j + 1;
END
GO

-- Create orders table (2000 rows)
CREATE TABLE dbo.orders (
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    order_number NVARCHAR(50) NOT NULL,
    user_id INT NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status NVARCHAR(50) NOT NULL,
    created_at DATETIME2 DEFAULT GETUTCDATE()
);
GO

DECLARE @k INT = 1;
WHILE @k <= 2000
BEGIN
    INSERT INTO dbo.orders (order_number, user_id, total_amount, status, created_at)
    VALUES (
        CONCAT('ORD-', FORMAT(@k, '000000')),
        (@k % 1000) + 1,
        ROUND(RAND(CHECKSUM(NEWID())) * 1000 + 10, 2),
        CASE (@k % 4)
            WHEN 0 THEN 'pending'
            WHEN 1 THEN 'shipped'
            WHEN 2 THEN 'delivered'
            ELSE 'cancelled'
        END,
        DATEADD(SECOND, -@k * 30, GETUTCDATE())
    );
    SET @k = @k + 1;
END
GO

-- Update statistics for accurate metadata
UPDATE STATISTICS dbo.users;
UPDATE STATISTICS dbo.products;
UPDATE STATISTICS dbo.orders;
GO

-- Verify data
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM dbo.users
UNION ALL
SELECT 'products', COUNT(*) FROM dbo.products
UNION ALL
SELECT 'orders', COUNT(*) FROM dbo.orders;
GO
