-- Insert sample data into Customers
INSERT INTO DATA_PIPELINE.Customers (CustomerID, CustomerName, Email)
VALUES
    (1, 'Alice Smith', 'alice@example.com'),
    (2, 'Bob Johnson', 'bob@example.com'),
    (3, 'Carol Williams', 'carol@example.com');

-- Insert sample data into Orders
INSERT INTO DATA_PIPELINE.Orders (CustomerID, OrderAmount, OrderDate)
VALUES
    (1, 250.75, '2024-06-01'),
    (2, 100.00, '2024-06-15'),
    (3, 300.50, '2024-06-20'),
    (1, 75.25,  '2024-06-25');
