CREATE OR REPLACE VIEW High_Earning_Employees AS
SELECT
    e.Employee_ID,
    e.Name,
    s.Base_Salary + s.Bonus AS Total_Compensation
FROM Employees e
JOIN Salaries s ON e.Employee_ID = s.Employee_ID
WHERE s.Base_Salary + s.Bonus > 100000;
