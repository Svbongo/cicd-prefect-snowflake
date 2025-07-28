-- Customers Table
CREATE OR REPLACE TABLE Customers (
    Customer_ID INT PRIMARY KEY,
    Name VARCHAR(100),
    Email VARCHAR(100),
    Phone VARCHAR(20),
    Address VARCHAR(255)
);

-- Insert into Customers
INSERT INTO Customers VALUES 
(1, 'Alice Johnson', 'alice@example.com', '123-456-7890', '123 Main St'),
(2, 'Bob Smith', 'bob@example.com', '234-567-8901', '456 Elm St');
