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
(102, 'Office Chair', 'Furniture', 149.99, 50),
(103, 'Microwave', 'Appliances', 99.99, 20),
(104, 'Toaster', 'Appliances', 29.99, 50),
(105, 'Coffee Table', 'Furniture', 199.99, 30),
(106, 'Bookshelf', 'Furniture', 79.99, 20),
(107, 'Desk Lamp', 'Lighting', 19.99, 50),
(108, 'Standing Desk', 'Furniture', 249.99, 20);