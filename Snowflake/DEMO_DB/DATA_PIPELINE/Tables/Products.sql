-- Create Products table
CREATE OR REPLACE TABLE DATA_PIPELINE.Products (
    ProductID INT PRIMARY KEY,
    ProductName STRING,
    Price FLOAT
);

-- Insert sample data into Products
INSERT INTO DATA_PIPELINE.Products (ProductID, ProductName, Price)
VALUES
    (1, 'Toaster', 99.99),
    (2, 'TV', 500.00),
    (3, 'Laptop', 999.99);