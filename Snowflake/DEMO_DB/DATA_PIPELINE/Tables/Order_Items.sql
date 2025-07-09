-- Order_Items Table
CREATE OR REPLACE TABLE Order_Items (
    ItemID INT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    LineTotal DECIMAL(10,2),
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

-- Insert into Order_Items
INSERT INTO Order_Items VALUES 
(1, 5001, 100, 1, 899.99),
(2, 5001, 101, 2, 99.99),
(3, 5002, 102, 1, 149.99);