CREATE OR REPLACE PROCEDURE Calculate_Total_Orders(Cust_ID INT)
RETURNS DECIMAL(10,2)
LANGUAGE SQL
AS
$$
    SELECT COALESCE(SUM(TotalAmount), 0)
    FROM Orders
    WHERE CustomerID = Cust_ID;
$$;
