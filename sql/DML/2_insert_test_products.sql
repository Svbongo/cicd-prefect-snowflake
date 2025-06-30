-- 2_insert_test_products.sql

-- Insert sample product records
INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, CATEGORY, PRICE, CREATED_AT)
VALUES 
    (201, 'Test Widget A', 'Gadgets', 19.99, CURRENT_TIMESTAMP),
    (202, 'Test Widget B', 'Gadgets', 29.99, CURRENT_TIMESTAMP),
    (203, 'Test Widget C', 'Gadgets', 39.99, CURRENT_TIMESTAMP);
