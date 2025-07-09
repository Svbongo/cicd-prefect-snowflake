CREATE OR REPLACE VIEW Employee_Department_View AS
SELECT
    e.Employee_ID,
    e.Name,
    d.Department_Name,
    e.Hire_Date
FROM Employees e
JOIN Departments d ON e.Department_ID = d.Department_ID;
