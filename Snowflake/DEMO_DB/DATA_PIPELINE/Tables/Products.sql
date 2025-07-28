USE DATABASE DEMO_DB;
USE SCHEMA DATA_PIPELINE;

-- Products Table
CREATE OR REPLACE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName STRING,
    Category STRING,
    Price DECIMAL(10,2),
    Stock INT
);

-- Insert into Products
INSERT INTO Products VALUES 
(100, 'Laptop', 'Electronics', 899.99, 20),
(101, 'Headphones', 'Electronics', 49.99, 100),
(102, 'Office Chair', 'Furniture', 149.99, 50);
