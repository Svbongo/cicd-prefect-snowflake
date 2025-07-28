USE DATABASE DEMO_DB;
USE SCHEMA DATA_PIPELINE;

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
