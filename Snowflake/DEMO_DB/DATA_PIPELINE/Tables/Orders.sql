-- Create Orders table
CREATE OR REPLACE TABLE DATA_PIPELINE.Orders (
    OrderID INT AUTOINCREMENT,
    CustomerID INT,
    OrderAmount FLOAT,
    OrderDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES DATA_PIPELINE.Customers(CustomerID)
);

-- Insert sample data into Orders
INSERT INTO DATA_PIPELINE.Orders (CustomerID, OrderAmount, OrderDate)
VALUES
    (1, 250.75, '2024-06-01'),
    (2, 100.00, '2024-06-15'),
    (3, 300.50, '2024-06-20'),
    (1, 75.25,  '2024-06-25');
