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
(4, 'David Kim', 'david.kim@example.com', 103, '2021-05-01'),
(5, 'Eve Martin', 'eve.martin@example.com', 101, '2020-08-01'),
(6, 'Frank Brown', 'frank.brown@example.com', 102, '2022-01-01');
