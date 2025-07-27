CREATE OR REPLACE TABLE Employees (
    Employee_ID INT PRIMARY KEY,
    Name STRING,
    Email STRING,
    Department_ID INT,
    Hire_Date DATE
);

INSERT INTO Employees (Employee_ID, Name, Email, Department_ID, Hire_Date) VALUES
(1, 'Alice Johnson', 'alice.johnson@example.com', 101, '2021-03-01'),
(2, 'Bob Smith', 'bob.smith@example.com', 102, '2022-07-15'),
(3, 'Charlie Lee', 'charlie.lee@example.com', 101, '2020-11-22');
