-- Customers Table
CREATE OR REPLACE TABLE Customers (
    CustomerID INT PRIMARY KEY,
    FirstName STRING,
    LastName STRING,
    Email STRING,
    CreatedAt TIMESTAMP
);

-- Insert into Customers
INSERT INTO Customers VALUES 
(1, 'John', 'Doe', 'john@example.com', CURRENT_TIMESTAMP),
(2, 'Jane', 'Smith', 'jane@example.com', CURRENT_TIMESTAMP);