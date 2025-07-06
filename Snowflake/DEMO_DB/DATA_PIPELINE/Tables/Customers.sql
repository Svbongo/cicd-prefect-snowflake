-- Create Customers table
CREATE OR REPLACE TABLE DATA_PIPELINE.Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName STRING,
    Email STRING
);

-- Insert sample data into Customers
INSERT INTO DATA_PIPELINE.Customers (CustomerID, CustomerName, Email)
VALUES
    (1, 'Alice Smith', 'alice@example.com'),
    (2, 'Bob Johnson', 'bob@example.com'),
    (3, 'Carol Williams', 'carol@example.com');