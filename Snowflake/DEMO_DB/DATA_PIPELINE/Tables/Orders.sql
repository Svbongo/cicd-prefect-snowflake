-- Orders Table
CREATE OR REPLACE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    TotalAmount DECIMAL(10,2),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

-- Insert into Orders
INSERT INTO Orders VALUES 
(5001, 1, '2024-06-01', 999.98),
(5002, 2, '2024-06-03', 149.99);