-- Insert sample customers
INSERT INTO DATA_PIPELINE.CUSTOMERS (CUSTOMER_ID, CUSTOMER_NAME, EMAIL) VALUES
(1, 'Alice Johnson', 'alice@example.com'),
(2, 'Bob Smith', 'bob@example.com'),
(3, 'Carol Davis', 'carol@example.com');

-- Insert sample products
INSERT INTO DATA_PIPELINE.PRODUCTS (PRODUCT_ID, PRODUCT_NAME, PRICE) VALUES
(101, 'Laptop', 1299.99),
(102, 'Smartphone', 799.49),
(103, 'Smartphone', 899.49),
(104, 'Monitor', 249.89);

-- Insert sample orders
INSERT INTO DATA_PIPELINE.ORDERS (ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, ORDER_DATE) VALUES
(1001, 1, 101, 1, '2024-06-01'),
(1002, 2, 102, 2, '2024-06-02'),
(1003, 3, 103, 1, '2024-06-03');
