-- Products Table
CREATE OR REPLACE TABLE Products (
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(100),
    Price DECIMAL(10, 2),
    Stock INT
);

-- Insert into Products
INSERT INTO Products VALUES 
(100, 'Laptop', 999.99, 50),
(101, 'Mouse', 25.99, 150);
