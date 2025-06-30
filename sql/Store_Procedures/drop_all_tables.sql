CREATE OR REPLACE PROCEDURE drop_all_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    cur CURSOR FOR
        SELECT 'DROP TABLE IF EXISTS "' || table_schema || '"."' || table_name || '";'
        FROM information_schema.tables
        WHERE table_schema NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
          AND table_type = 'BASE TABLE';

    sql_stmt STRING;
BEGIN
    FOR record IN cur DO
        LET sql_stmt = record.$1;
        EXECUTE IMMEDIATE :sql_stmt;
    END FOR;

    RETURN '✅ All user tables dropped.';
END;
$$;
