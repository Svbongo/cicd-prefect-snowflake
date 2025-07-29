CREATE OR REPLACE PROCEDURE "DROP_ALL_TABLES"()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    cur CURSOR FOR
        SELECT ''DROP TABLE IF EXISTS "'' || TABLE_schema || ''"."'' || TABLE_name || ''";'' AS stmt
        FROM informatiON_schema.TABLEs
        WHERE TABLE_schema = CURRENT_SCHEMA()
          AND TABLE_type = ''BASE TABLE'';
    sql_stmt STRING;
BEGIN
    FOR record IN cur DO
        sql_stmt := record.stmt;
        EXECUTE IMMEDIATE :sql_stmt;
    END FOR;

    RETURN ''✅ All bASe TABLEs dropped successfully.'';
END;
';
