USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

-- Salaries Table
CREATE OR REPLACE TABLE Salaries (
    Employee_ID INT,
    Base_Salary DECIMAL(12, 2),
    Bonus DECIMAL(12, 2),
    Effective_Date DATE,
    CONSTRAINT FK_Employee FOREIGN KEY (Employee_ID) REFERENCES Employees(Employee_ID)
);

-- Insert into Salaries
INSERT INTO Salaries (Employee_ID, Base_Salary, Bonus, Effective_Date) VALUES
(1, 70000.00, 5000.00, '2023-01-01'),
(2, 80000.00, 7000.00, '2023-01-01');
