CREATE OR REPLACE PROCEDURE Assign_Department(emp_id INT, dept_id INT)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE Employees
    SET Department_ID = dept_id
    WHERE Employee_ID = emp_id;

    RETURN 'Department assigned';
END;
$$;
