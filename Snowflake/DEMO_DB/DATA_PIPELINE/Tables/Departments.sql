USE DATABASE DEMO_DB;
USE SCHEMA DATA_PIPELINE;

CREATE OR REPLACE TABLE Departments (
    Department_ID INT PRIMARY KEY,
    Department_Name STRING
);

INSERT INTO Departments (Department_ID, Department_Name) VALUES
(101, 'Engineering'),
(102, 'Marketing'),
(103, 'Human Resources');
