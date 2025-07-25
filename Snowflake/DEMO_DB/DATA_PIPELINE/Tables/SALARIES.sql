CREATE OR REPLACE TABLE Salaries (
    Employee_ID INT,
    Base_Salary FLOAT,
    Bonus FLOAT,
    Effective_Date DATE,
    FOREIGN KEY (Employee_ID) REFERENCES Employees(Employee_ID)
);

INSERT INTO Salaries (Employee_ID, Base_Salary, Bonus, Effective_Date) VALUES
(1, 90000, 5000, '2023-01-01'),
(2, 75000, 3000, '2023-01-01'),
(3, 120000, 10000, '2023-01-01');
(4, 65000, 4000, '2023-01-01'),
(5, 85000, 6000, '2023-01-01'),
(6, 70000, 5000, '2023-01-01');
