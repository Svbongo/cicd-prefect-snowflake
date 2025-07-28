USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

-- Employees Table
CREATE OR REPLACE TABLE Employees (
    Employee_ID INT PRIMARY KEY,
    Name VARCHAR(100),
    Email VARCHAR(100),
    Department_ID INT,
    Hire_Date DATE,
    CONSTRAINT FK_Department FOREIGN KEY (Department_ID) REFERENCES Departments(Department_ID)
);

-- Insert into Employees
INSERT INTO Employees (Employee_ID, Name, Email, Department_ID, Hire_Date) VALUES
(1, 'John Doe', 'john.doe@example.com', 2, '2020-01-15'),
(2, 'Jane Smith', 'jane.smith@example.com', 1, '2019-11-01');
