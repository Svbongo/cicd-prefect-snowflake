CREATE OR REPLACE PROCEDURE drop_all_tables()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_stmt STRING;
BEGIN
    FOR rec IN (
        SELECT 'DROP TABLE IF EXISTS "' || table_schema || '"."' || table_name || '";' AS stmt
        FROM information_schema.tables
        WHERE table_schema NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
          AND table_type = 'BASE TABLE'
    )
    DO
        LET sql_stmt = rec.stmt;
        EXECUTE IMMEDIATE :sql_stmt;
    END FOR;

    RETURN '✅ All user tables dropped.';
END;
$$;
