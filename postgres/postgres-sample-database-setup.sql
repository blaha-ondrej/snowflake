-- =============================================
-- POSTGRES SAMPLE DATABASE SETUP SCRIPT
-- Run this script in DBeaver after connecting to your Snowflake Postgres instance
-- =============================================
-- 
-- Connection Details (get from Snowflake DESCRIBE command):
-- - Host: [your-instance].postgres.snowflake.app
-- - Port: 5432
-- - Database: postgres
-- - Username: snowflake_admin
-- - Password: [from DESCRIBE output]
-- - SSL Mode: require


-- =============================================
-- PART 1: CREATE DATABASE SCHEMA
-- =============================================
-- E-Commerce Data Model: Customers, Products, Orders, Order Items

-- Table 1: Customers
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE customers IS 'Customer master data';
COMMENT ON COLUMN customers.customer_id IS 'Unique customer identifier';
COMMENT ON COLUMN customers.email IS 'Customer email address (must be unique)';


-- Table 2: Products
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE products IS 'Product catalog';
COMMENT ON COLUMN products.price IS 'Product price in USD';
COMMENT ON COLUMN products.stock_quantity IS 'Available inventory count';


-- Table 3: Orders
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    total_amount DECIMAL(10, 2) CHECK (total_amount >= 0),
    shipping_address TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE RESTRICT
);

COMMENT ON TABLE orders IS 'Customer orders';
COMMENT ON COLUMN orders.status IS 'Order status: pending, processing, shipped, delivered, cancelled';
COMMENT ON COLUMN orders.total_amount IS 'Total order amount in USD';


-- Table 4: Order Items (junction table with order details)
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    subtotal DECIMAL(10, 2) NOT NULL CHECK (subtotal >= 0),
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE RESTRICT
);

COMMENT ON TABLE order_items IS 'Line items for each order';
COMMENT ON COLUMN order_items.unit_price IS 'Price per unit at time of order';
COMMENT ON COLUMN order_items.subtotal IS 'Total for this line item (quantity * unit_price)';


-- Create indexes for better query performance
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_products_category ON products(category);


-- =============================================
-- PART 2: INSERT SAMPLE DATA
-- =============================================

-- Insert Customers (10 records)
INSERT INTO customers (first_name, last_name, email, phone, city, country) VALUES
('John', 'Smith', 'john.smith@email.com', '+1-555-0101', 'New York', 'USA'),
('Emma', 'Johnson', 'emma.j@email.com', '+1-555-0102', 'Los Angeles', 'USA'),
('Michael', 'Williams', 'michael.w@email.com', '+44-20-5501', 'London', 'UK'),
('Sophie', 'Brown', 'sophie.b@email.com', '+44-20-5502', 'Manchester', 'UK'),
('Hans', 'Mueller', 'hans.m@email.com', '+49-30-5501', 'Berlin', 'Germany'),
('Marie', 'Dubois', 'marie.d@email.com', '+33-1-5501', 'Paris', 'France'),
('Carlos', 'Garcia', 'carlos.g@email.com', '+34-91-5501', 'Madrid', 'Spain'),
('Anna', 'Kowalski', 'anna.k@email.com', '+48-22-5501', 'Warsaw', 'Poland'),
('Yuki', 'Tanaka', 'yuki.t@email.com', '+81-3-5501', 'Tokyo', 'Japan'),
('Sarah', 'Davis', 'sarah.d@email.com', '+1-555-0103', 'Chicago', 'USA');

-- Verify customer insert
SELECT COUNT(*) as customer_count FROM customers;


-- Insert Products (15 records across different categories)
INSERT INTO products (product_name, category, price, stock_quantity, description) VALUES
('Laptop Pro 15"', 'Electronics', 1299.99, 50, 'High-performance laptop for professionals'),
('Wireless Mouse', 'Electronics', 29.99, 200, 'Ergonomic wireless mouse with USB receiver'),
('USB-C Hub', 'Electronics', 49.99, 150, '7-in-1 USB-C hub with multiple ports'),
('Desk Chair Pro', 'Furniture', 349.99, 30, 'Ergonomic office chair with lumbar support'),
('Standing Desk', 'Furniture', 599.99, 20, 'Adjustable height standing desk (electric)'),
('Notebook Set', 'Stationery', 15.99, 500, 'Set of 3 premium notebooks - ruled paper'),
('Pen Collection', 'Stationery', 24.99, 300, 'Professional pen collection with storage case'),
('Desk Lamp LED', 'Furniture', 79.99, 80, 'LED desk lamp with adjustable brightness levels'),
('Keyboard Mechanical', 'Electronics', 149.99, 100, 'Mechanical keyboard with RGB lighting'),
('Monitor 27"', 'Electronics', 399.99, 40, '4K UHD monitor 27 inch with HDR'),
('Headphones Pro', 'Electronics', 249.99, 75, 'Noise-cancelling wireless headphones'),
('Backpack Laptop', 'Accessories', 89.99, 120, 'Water-resistant laptop backpack with USB port'),
('Mouse Pad XL', 'Accessories', 19.99, 250, 'Extra large gaming mouse pad'),
('Webcam HD', 'Electronics', 129.99, 60, '1080p HD webcam with built-in microphone'),
('Cable Organizer', 'Accessories', 12.99, 400, 'Desktop cable management solution kit');

-- Verify product insert
SELECT COUNT(*) as product_count FROM products;


-- Insert Orders (20 records with realistic dates)
INSERT INTO orders (customer_id, order_date, status, total_amount, shipping_address) VALUES
(1, '2024-01-15 10:30:00', 'delivered', 1379.97, '123 Main St, Apt 4B, New York, NY 10001, USA'),
(2, '2024-01-16 14:20:00', 'delivered', 449.98, '456 Oak Ave, Los Angeles, CA 90001, USA'),
(3, '2024-01-17 09:15:00', 'shipped', 1749.97, '789 High St, London, UK SW1A 1AA'),
(4, '2024-01-18 16:45:00', 'delivered', 79.99, '321 Park Ln, Manchester, UK M1 1AA'),
(5, '2024-01-19 11:00:00', 'processing', 949.98, '654 Unter den Linden, Berlin, Germany 10115'),
(1, '2024-01-20 13:30:00', 'delivered', 290.97, '123 Main St, Apt 4B, New York, NY 10001, USA'),
(6, '2024-01-21 10:10:00', 'shipped', 649.98, '987 Rue de la Paix, Paris, France 75001'),
(7, '2024-01-22 15:25:00', 'delivered', 189.96, '147 Gran Via, Madrid, Spain 28013'),
(8, '2024-01-23 12:40:00', 'processing', 1549.97, '258 Nowy Swiat, Warsaw, Poland 00-001'),
(9, '2024-01-24 09:55:00', 'shipped', 529.98, '369 Shibuya Crossing, Tokyo, Japan 150-0002'),
(2, '2024-01-25 14:15:00', 'delivered', 189.96, '456 Oak Ave, Los Angeles, CA 90001, USA'),
(3, '2024-01-26 11:30:00', 'processing', 399.99, '789 High St, London, UK SW1A 1AA'),
(10, '2024-01-27 16:20:00', 'delivered', 699.97, '741 Lake Shore Dr, Chicago, IL 60611, USA'),
(4, '2024-01-28 10:45:00', 'shipped', 279.98, '321 Park Ln, Manchester, UK M1 1AA'),
(5, '2024-01-29 13:00:00', 'delivered', 89.99, '654 Unter den Linden, Berlin, Germany 10115'),
(6, '2024-01-30 09:30:00', 'processing', 1299.99, '987 Rue de la Paix, Paris, France 75001'),
(7, '2024-01-31 15:10:00', 'delivered', 499.96, '147 Gran Via, Madrid, Spain 28013'),
(8, '2024-02-01 12:25:00', 'shipped', 198.92, '258 Nowy Swiat, Warsaw, Poland 00-001'),
(9, '2024-02-02 10:40:00', 'delivered', 149.99, '369 Shibuya Crossing, Tokyo, Japan 150-0002'),
(10, '2024-02-03 14:55:00', 'processing', 949.96, '741 Lake Shore Dr, Chicago, IL 60611, USA');

-- Verify order insert
SELECT COUNT(*) as order_count FROM orders;


-- Insert Order Items (maintaining referential integrity)
-- Each order can have multiple items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal) VALUES
-- Order 1 (Customer 1 - John Smith)
(1, 1, 1, 1299.99, 1299.99),    -- Laptop
(1, 2, 1, 29.99, 29.99),         -- Wireless Mouse
(1, 3, 1, 49.99, 49.99),         -- USB-C Hub
-- Order 2 (Customer 2 - Emma Johnson)
(2, 9, 1, 149.99, 149.99),       -- Keyboard
(2, 10, 1, 399.99, 399.99),      -- Monitor
-- Order 3 (Customer 3 - Michael Williams)
(3, 1, 1, 1299.99, 1299.99),     -- Laptop
(3, 9, 1, 149.99, 149.99),       -- Keyboard
(3, 10, 1, 399.99, 399.99),      -- Monitor
-- Order 4 (Customer 4 - Sophie Brown)
(4, 8, 1, 79.99, 79.99),         -- Desk Lamp
-- Order 5 (Customer 5 - Hans Mueller)
(5, 4, 1, 349.99, 349.99),       -- Desk Chair
(5, 5, 1, 599.99, 599.99),       -- Standing Desk
-- Order 6 (Customer 1 - John Smith, repeat customer)
(6, 11, 1, 249.99, 249.99),      -- Headphones
(6, 6, 1, 15.99, 15.99),         -- Notebook Set
(6, 7, 1, 24.99, 24.99),         -- Pen Collection
-- Order 7 (Customer 6 - Marie Dubois)
(7, 5, 1, 599.99, 599.99),       -- Standing Desk
(7, 3, 1, 49.99, 49.99),         -- USB-C Hub
-- Order 8 (Customer 7 - Carlos Garcia)
(8, 2, 2, 29.99, 59.98),         -- Wireless Mouse x2
(8, 6, 5, 15.99, 79.95),         -- Notebook Set x5
(8, 7, 2, 24.99, 49.98),         -- Pen Collection x2
-- Order 9 (Customer 8 - Anna Kowalski)
(9, 1, 1, 1299.99, 1299.99),     -- Laptop
(9, 11, 1, 249.99, 249.99),      -- Headphones
-- Order 10 (Customer 9 - Yuki Tanaka)
(10, 4, 1, 349.99, 349.99),      -- Desk Chair
(10, 12, 2, 89.99, 179.98),      -- Backpack x2
-- Order 11 (Customer 2 - Emma Johnson, repeat customer)
(11, 13, 3, 19.99, 59.97),       -- Mouse Pad x3
(11, 14, 1, 129.99, 129.99),     -- Webcam
-- Order 12 (Customer 3 - Michael Williams, repeat customer)
(12, 10, 1, 399.99, 399.99),     -- Monitor
-- Order 13 (Customer 10 - Sarah Davis)
(13, 5, 1, 599.99, 599.99),      -- Standing Desk
(13, 8, 1, 79.99, 79.99),        -- Desk Lamp
(13, 13, 1, 19.99, 19.99),       -- Mouse Pad
-- Order 14 (Customer 4 - Sophie Brown, repeat customer)
(14, 11, 1, 249.99, 249.99),     -- Headphones
(14, 2, 1, 29.99, 29.99),        -- Wireless Mouse
-- Order 15 (Customer 5 - Hans Mueller, repeat customer)
(15, 12, 1, 89.99, 89.99),       -- Backpack
-- Order 16 (Customer 6 - Marie Dubois, repeat customer)
(16, 1, 1, 1299.99, 1299.99),    -- Laptop
-- Order 17 (Customer 7 - Carlos Garcia, repeat customer)
(17, 9, 1, 149.99, 149.99),      -- Keyboard
(17, 3, 2, 49.99, 99.98),        -- USB-C Hub x2
(17, 11, 1, 249.99, 249.99),     -- Headphones
-- Order 18 (Customer 8 - Anna Kowalski, repeat customer)
(18, 2, 2, 29.99, 59.98),        -- Wireless Mouse x2
(18, 6, 5, 15.99, 79.95),        -- Notebook Set x5
(18, 15, 3, 12.99, 38.97),       -- Cable Organizer x3
-- Order 19 (Customer 9 - Yuki Tanaka, repeat customer)
(19, 9, 1, 149.99, 149.99),      -- Keyboard
-- Order 20 (Customer 10 - Sarah Davis, repeat customer)
(20, 4, 2, 349.99, 699.98),      -- Desk Chair x2
(20, 11, 1, 249.99, 249.99);     -- Headphones

-- Verify order items insert
SELECT COUNT(*) as order_item_count FROM order_items;


-- =============================================
-- PART 3: DATA VALIDATION
-- =============================================

-- Verify record counts
SELECT 
    'Customers' AS table_name, 
    COUNT(*) AS record_count 
FROM customers
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Order Items', COUNT(*) FROM order_items
ORDER BY table_name;

-- Verify referential integrity (should return no rows if integrity is maintained)
-- Check for orders without customers
SELECT o.order_id, o.customer_id
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Check for order items without valid orders
SELECT oi.order_item_id, oi.order_id
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Check for order items without valid products
SELECT oi.order_item_id, oi.product_id
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Verify order totals match sum of order items (should return no rows if data is correct)
SELECT 
    o.order_id,
    o.total_amount AS order_total,
    COALESCE(SUM(oi.subtotal), 0) AS items_total,
    o.total_amount - COALESCE(SUM(oi.subtotal), 0) AS difference
FROM orders o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.total_amount
HAVING ABS(o.total_amount - COALESCE(SUM(oi.subtotal), 0)) > 0.01;


-- =============================================
-- PART 4: ANALYTICAL QUERY 1
-- Customer Purchase Analysis
-- =============================================
-- Purpose: Analyze customer behavior, spending patterns, and segment customers
-- Useful for: Marketing campaigns, customer retention, VIP identification

SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.city,
    c.country,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS total_spent,
    COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    MAX(o.order_date) - MIN(o.order_date) AS customer_lifetime_days,
    CASE 
        WHEN COALESCE(SUM(o.total_amount), 0) > 1500 THEN 'VIP Customer'
        WHEN COALESCE(SUM(o.total_amount), 0) > 500 THEN 'High Value'
        WHEN COALESCE(SUM(o.total_amount), 0) > 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment,
    CASE 
        WHEN COUNT(DISTINCT o.order_id) >= 3 THEN 'Loyal'
        WHEN COUNT(DISTINCT o.order_id) = 2 THEN 'Returning'
        WHEN COUNT(DISTINCT o.order_id) = 1 THEN 'One-time'
        ELSE 'No Orders'
    END AS customer_loyalty
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.country
ORDER BY total_spent DESC, total_orders DESC;

-- Key Insights from this query:
-- 1. Top spending customers for VIP treatment
-- 2. Average order value helps set marketing budgets
-- 3. Customer segments for targeted campaigns
-- 4. Loyal vs one
