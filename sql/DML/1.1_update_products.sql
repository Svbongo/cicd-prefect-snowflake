-- =============================================
-- PRODUCT DATA UPDATE SCRIPT (v1.1)
-- Updates existing products and adds new ones
-- =============================================

-- Set the context to DATA_PIPELINE schema
USE SCHEMA DATA_PIPELINE;

-- =============================================
-- 1. PRICE UPDATES FOR EXISTING PRODUCTS
-- =============================================
-- Apply a 10% price increase to all Electronics
UPDATE PRODUCTS 
SET 
    PRICE = ROUND(PRICE * 1.10, 2),
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE 
    CATEGORY = 'Electronics';

-- Apply a 5% price increase to Furniture
UPDATE PRODUCTS 
SET 
    PRICE = ROUND(PRICE * 1.05, 2),
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE 
    CATEGORY = 'Furniture';

-- =============================================
-- 2. ADD NEW PRODUCTS
-- =============================================
-- Add new electronics
INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, DESCRIPTION, PRICE, CATEGORY)
VALUES 
    (13, 'Tablet', '10-inch Android tablet with 128GB storage', 299.99, 'Electronics'),
    (14, 'Smart Watch', 'Fitness tracking and notifications', 199.99, 'Electronics'),
    (15, 'Wireless Speaker', 'Portable Bluetooth speaker', 89.99, 'Accessories');

-- Add new office supplies
INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, DESCRIPTION, PRICE, CATEGORY)
VALUES 
    (16, 'Stapler', 'Heavy-duty stapler', 12.99, 'Office Supplies'),
    (17, 'Whiteboard', '4x6 feet magnetic whiteboard', 79.99, 'Office Supplies');

-- =============================================
-- 3. UPDATE PRODUCT DESCRIPTIONS
-- =============================================
-- Improve descriptions for better clarity
UPDATE PRODUCTS 
SET 
    DESCRIPTION = 'High-performance laptop with 16GB RAM and 512GB SSD',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE 
    PRODUCT_ID = 1;  -- Laptop

UPDATE PRODUCTS 
SET 
    DESCRIPTION = 'Latest smartphone model with 128GB storage and 48MP camera',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE 
    PRODUCT_ID = 2;  -- Smartphone

-- =============================================
-- 4. VALIDATION QUERIES
-- =============================================
-- Show updated product counts by category
SELECT 
    CATEGORY,
    COUNT(*) AS PRODUCT_COUNT,
    ROUND(AVG(PRICE), 2) AS AVG_PRICE,
    ROUND(MIN(PRICE), 2) AS MIN_PRICE,
    ROUND(MAX(PRICE), 2) AS MAX_PRICE
FROM 
    PRODUCTS
GROUP BY 
    CATEGORY
ORDER BY 
    PRODUCT_COUNT DESC;

-- Show the 5 most expensive products
SELECT 
    PRODUCT_NAME,
    CATEGORY,
    PRICE
FROM 
    PRODUCTS
ORDER BY 
    PRICE DESC
LIMIT 5;

-- =============================================
-- 5. UPDATE COMPLETION STATUS
-- =============================================
SELECT 'Product data update v1.1 completed successfully' AS STATUS,
       CURRENT_TIMESTAMP() AS COMPLETED_AT,
       (SELECT COUNT(*) FROM PRODUCTS) AS TOTAL_PRODUCTS,
       (SELECT COUNT(DISTINCT CATEGORY) FROM PRODUCTS) AS UNIQUE_CATEGORIES;
