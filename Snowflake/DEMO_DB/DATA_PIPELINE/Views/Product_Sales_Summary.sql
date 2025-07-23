-- View: Product Sales Summary
CREATE OR REPLACE VIEW vw_product_sales AS
SELECT 
    p.ProductID,
    p.ProductName,
    SUM(oi.Quantity) AS UnitsSold,
    SUM(oi.LineTotal) AS RevenueGenerated
FROM Products p
JOIN Order_Items oi ON p.ProductID = oi.ProductID
GROUP BY p.ProductID, p.ProductName;