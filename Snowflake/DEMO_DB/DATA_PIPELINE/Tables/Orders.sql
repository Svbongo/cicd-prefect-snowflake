
-- Create Orders table
CREATE OR REPLACE TABLE DATA_PIPELINE.Orders (
    OrderID INT AUTOINCREMENT,
    CustomerID INT,
    OrderAmount FLOAT,
    OrderDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES DATA_PIPELINE.Customers(CustomerID)
);
