CREATE OR REPLACE PROCEDURE "DROP_ALL_TABLES"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    cur CURSOR FOR
        SELECT ''DROP TABLE IF EXISTS "'' || table_schema || ''"."'' || table_name || ''";'' AS stmt
        FROM information_schema.tables
        WHERE table_schema = CURRENT_SCHEMA()
          AND table_type = ''BASE TABLE'';
    sql_stmt STRING;
BEGIN
    FOR record IN cur DO
        sql_stmt := record.stmt;
        EXECUTE IMMEDIATE :sql_stmt;
    END FOR;

    RETURN ''✅ All base tables dropped successfully.'';
END;
';;

