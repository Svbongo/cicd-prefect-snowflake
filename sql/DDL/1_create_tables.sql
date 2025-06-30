-- Create Customers table
CREATE OR REPLACE TABLE DATA_PIPELINE.Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName STRING,
    Email STRING
);

-- Create Orders table
CREATE OR REPLACE TABLE DATA_PIPELINE.Orders (
    OrderID INT AUTOINCREMENT,
    CustomerID INT,
    OrderAmount FLOAT,
    OrderDate DATE,
    FOREIGN KEY (CustomerID) REFERENCES DATA_PIPELINE.Customers(CustomerID)
);
