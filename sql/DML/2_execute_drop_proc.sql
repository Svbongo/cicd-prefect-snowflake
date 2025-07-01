-- Execute the stored procedure to drop all user tables
-- Assumes the procedure has already been created in the target schema

BEGIN
    CALL drop_all_tables();
END;
