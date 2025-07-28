USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

-- Departments Table
CREATE OR REPLACE TABLE Departments (
    Department_ID INT PRIMARY KEY,
    Department_Name VARCHAR(100)
);

-- Insert into Departments
INSERT INTO Departments (Department_ID, Department_Name) VALUES
(1, 'HR'),
(2, 'Engineering'),
(3, 'Sales');
