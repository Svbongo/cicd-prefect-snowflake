-- =============================================
-- INITIAL DATA LOAD SCRIPT
-- Combines all DML operations into a single file
-- =============================================

-- Set the context to DATA_PIPELINE schema
USE SCHEMA DATA_PIPELINE;

-- =============================================
-- 1. INITIAL DATA LOAD
-- =============================================
-- Insert sample products
INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, DESCRIPTION, PRICE, CATEGORY)
VALUES 
    (1, 'Laptop', 'High-performance laptop', 999.99, 'Electronics'),
    (2, 'Smartphone', 'Latest smartphone model', 699.99, 'Electronics'),
    (3, 'Headphones', 'Wireless noise-canceling', 199.99, 'Accessories'),
    (4, 'Desk', 'Ergonomic office desk', 249.99, 'Furniture'),
    (5, 'Office Chair', 'Comfortable office chair', 149.99, 'Furniture'),
    (6, 'Monitor', '27-inch 4K monitor', 349.99, 'Electronics'),
    (7, 'Keyboard', 'Mechanical keyboard', 129.99, 'Accessories'),
    (8, 'Mouse', 'Wireless ergonomic mouse', 59.99, 'Accessories'),
    (9, 'Notebook', 'Hardcover notebook', 12.99, 'Office Supplies'),
    (10, 'Pen Set', 'Premium ballpoint pen set', 24.99, 'Office Supplies');

-- Insert sample customers
INSERT INTO CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, ADDRESS)
VALUES 
    (1, 'John Doe', 'john.doe@example.com', '555-0101', '123 Main St, Anytown, USA'),
    (2, 'Jane Smith', 'jane.smith@example.com', '555-0102', '456 Oak Ave, Somewhere, USA'),
    (3, 'Acme Corp', 'contact@acmecorp.com', '555-0201', '789 Business Blvd, Industry City, USA'),
    (4, 'Tech Solutions', 'info@techsolutions.com', '555-0301', '321 Tech Park, Silicon Valley, USA'),
    (5, 'Global Retail', 'support@globalretail.com', '555-0401', '654 Market St, Commerce City, USA');

-- =============================================
-- 2. DATA UPDATES
-- =============================================
-- Update prices for Electronics category (5% increase)
UPDATE PRODUCTS 
SET 
    PRICE = ROUND(PRICE * 1.05, 2),
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE 
    CATEGORY = 'Electronics';

-- Add new products
INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, DESCRIPTION, PRICE, CATEGORY)
VALUES 
    (11, 'Wireless Earbuds', 'True wireless earbuds with charging case', 129.99, 'Electronics'),
    (12, 'Desk Lamp', 'Adjustable LED desk lamp', 39.99, 'Office Supplies');

