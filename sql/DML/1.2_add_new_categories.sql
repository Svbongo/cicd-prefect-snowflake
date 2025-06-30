-- =============================================
-- PRODUCT CATEGORY UPDATE SCRIPT (v1.2)
-- Adds new product categories and updates existing products
-- =============================================

-- Set the context to DATA_PIPELINE schema
USE SCHEMA DATA_PIPELINE;

-- =============================================
-- 1. ADD NEW PRODUCT CATEGORIES
-- =============================================
-- First, let's create a categories table if it doesn't exist
CREATE TABLE IF NOT EXISTS CATEGORIES (
    CATEGORY_ID NUMBER AUTOINCREMENT,
    CATEGORY_NAME VARCHAR(100) NOT NULL,
    DESCRIPTION VARCHAR(500),
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (CATEGORY_ID),
    UNIQUE (CATEGORY_NAME)
);

-- Add new categories
INSERT INTO CATEGORIES (CATEGORY_NAME, DESCRIPTION)
SELECT * FROM (
    SELECT 'Electronics', 'Electronic devices and accessories' UNION ALL
    SELECT 'Furniture', 'Home and office furniture' UNION ALL
    SELECT 'Clothing', 'Apparel and accessories' UNION ALL
    SELECT 'Books', 'Physical and digital books' UNION ALL
    SELECT 'Home & Garden', 'Home improvement and garden supplies' UNION ALL
    SELECT 'Toys & Games', 'Toys, games, and collectibles' UNION ALL
    SELECT 'Sports & Outdoors', 'Sports equipment and outdoor gear' UNION ALL
    SELECT 'Beauty & Personal Care', 'Beauty and personal care products' UNION ALL
    SELECT 'Health & Household', 'Health and household essentials' UNION ALL
    SELECT 'Automotive', 'Automotive parts and accessories'
) AS new_categories
WHERE NOT EXISTS (
    SELECT 1 FROM CATEGORIES 
    WHERE CATEGORY_NAME = new_categories.CATEGORY_NAME
);

-- =============================================
-- 2. ADD NEW PRODUCTS IN NEW CATEGORIES
-- =============================================
-- Add new products in the new categories
INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, DESCRIPTION, PRICE, CATEGORY)
VALUES 
    -- New Electronics
    (18, 'Wireless Earbuds', 'Noise-cancelling wireless earbuds with 24h battery', 129.99, 'Electronics'),
    (19, 'Fitness Tracker', 'Waterproof fitness tracker with heart rate monitor', 79.99, 'Electronics'),
    
    -- New Clothing
    (20, 'Cotton T-Shirt', '100% cotton crew neck t-shirt', 19.99, 'Clothing'),
    (21, 'Denim Jeans', 'Slim fit denim jeans', 49.99, 'Clothing'),
    
    -- New Home & Garden
    (22, 'Coffee Maker', '12-cup programmable coffee maker', 59.99, 'Home & Garden'),
    (23, 'Garden Hose', '50ft expandable garden hose', 39.99, 'Home & Garden'),
    
    -- New Sports & Outdoors
    (24, 'Yoga Mat', 'Eco-friendly non-slip yoga mat', 29.99, 'Sports & Outdoors'),
    (25, 'Camping Tent', '4-person waterproof camping tent', 149.99, 'Sports & Outdoors');

-- =============================================
-- 3. UPDATE EXISTING PRODUCT CATEGORIES
-- =============================================
-- Update existing products to use the new categories
UPDATE PRODUCTS 
SET 
    CATEGORY = 'Electronics',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE 
    CATEGORY = 'Electronics' AND
    PRODUCT_ID IN (1, 2, 13, 14, 15);

UPDATE PRODUCTS 
SET 
    CATEGORY = 'Furniture',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE 
    CATEGORY = 'Furniture' AND
    PRODUCT_ID IN (3, 4, 5);

-- =============================================
-- 4. ADD STOCK INFORMATION
-- =============================================
-- Create inventory table if it doesn't exist
CREATE TABLE IF NOT EXISTS INVENTORY (
    PRODUCT_ID NUMBER,
    QUANTITY_IN_STOCK NUMBER NOT NULL,
    LAST_STOCK_UPDATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (PRODUCT_ID),
    FOREIGN KEY (PRODUCT_ID) REFERENCES PRODUCTS(PRODUCT_ID)
);

-- Add initial stock levels
MERGE INTO INVENTORY AS target
USING (
    SELECT 
        p.PRODUCT_ID,
        CASE 
            WHEN p.PRICE < 50 THEN FLOOR(RANDOM() * 100) + 50  -- 50-150 for cheaper items
            WHEN p.PRICE BETWEEN 50 AND 150 THEN FLOOR(RANDOM() * 30) + 20  -- 20-50 for mid-range
            ELSE FLOOR(RANDOM() * 15) + 5  -- 5-20 for expensive items
        END AS QUANTITY
    FROM PRODUCTS p
) AS source
ON target.PRODUCT_ID = source.PRODUCT_ID
WHEN MATCHED THEN
    UPDATE SET 
        target.QUANTITY_IN_STOCK = source.QUANTITY,
        target.LAST_STOCK_UPDATE = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (PRODUCT_ID, QUANTITY_IN_STOCK)
    VALUES (source.PRODUCT_ID, source.QUANTITY);

-- =============================================
-- 5. VALIDATION QUERIES
-- =============================================
-- Show product counts by category
SELECT 
    p.CATEGORY,
    COUNT(*) AS PRODUCT_COUNT,
    ROUND(AVG(p.PRICE), 2) AS AVG_PRICE,
    SUM(i.QUANTITY_IN_STOCK) AS TOTAL_INVENTORY,
    ROUND(SUM(p.PRICE * i.QUANTITY_IN_STOCK), 2) AS INVENTORY_VALUE
FROM 
    PRODUCTS p
JOIN 
    INVENTORY i ON p.PRODUCT_ID = i.PRODUCT_ID
GROUP BY 
    p.CATEGORY
ORDER BY 
    INVENTORY_VALUE DESC;

-- Show low stock items (less than 20 in stock)
SELECT 
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.PRICE,
    i.QUANTITY_IN_STOCK,
    ROUND(p.PRICE * i.QUANTITY_IN_STOCK, 2) AS STOCK_VALUE
FROM 
    PRODUCTS p
JOIN 
    INVENTORY i ON p.PRODUCT_ID = i.PRODUCT_ID
WHERE 
    i.QUANTITY_IN_STOCK < 20
ORDER BY 
    i.QUANTITY_IN_STOCK ASC;

-- =============================================
-- 6. UPDATE COMPLETION STATUS
-- =============================================
SELECT 'Product category update v1.2 completed successfully' AS STATUS,
       CURRENT_TIMESTAMP() AS COMPLETED_AT,
       (SELECT COUNT(*) FROM PRODUCTS) AS TOTAL_PRODUCTS,
       (SELECT COUNT(DISTINCT CATEGORY) FROM PRODUCTS) AS UNIQUE_CATEGORIES,
       (SELECT SUM(QUANTITY_IN_STOCK) FROM INVENTORY) AS TOTAL_INVENTORY,
       (SELECT ROUND(SUM(p.PRICE * i.QUANTITY_IN_STOCK), 2) 
        FROM PRODUCTS p 
        JOIN INVENTORY i ON p.PRODUCT_ID = i.PRODUCT_ID) AS TOTAL_INVENTORY_VALUE;
