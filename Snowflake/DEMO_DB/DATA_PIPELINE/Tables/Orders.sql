-- Create Orders table
CREATE OR REPLACE TABLE DATA_PIPELINE.Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    ProductID INT,
    OrderDate DATE,
    Quantity INT,
    FOREIGN KEY (CustomerID) REFERENCES DATA_PIPELINE.Customers(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES DATA_PIPELINE.Products(ProductID)
);

-- Insert sample data into Orders
INSERT INTO DATA_PIPELINE.Orders (OrderID, CustomerID, ProductID, OrderDate, Quantity)
VALUES
    (1, 1, 1, '2024-06-01', 2),
    (2, 2, 2, '2024-06-15', 1),
    (3, 3, 3, '2024-06-20', 3);
