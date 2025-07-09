CREATE OR REPLACE PROCEDURE Update_Bonus(percentage FLOAT)
RETURNS STRING
LANGUAGE SQL
AS
$$
    UPDATE Salaries
    SET Bonus = Bonus * (1 + percentage / 100);
    RETURN 'Bonus updated';
$$;

