-- Drop the procedure if it exists
DROP PROCEDURE IF EXISTS DEMO_DB.DATA_PIPELINE.DROP_ALL_TABLES();

-- Create the procedure with proper Snowflake Scripting syntax
CREATE OR REPLACE PROCEDURE DEMO_DB.DATA_PIPELINE.DROP_ALL_TABLES()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        var tables = snowflake.execute({
            sqlText: `
                SELECT TABLE_SCHEMA, TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = CURRENT_SCHEMA() 
                AND TABLE_TYPE = 'BASE TABLE'`
        });
        
        var count = 0;
        while (tables.next()) {
            var schema = tables.getColumnValue(1);
            var table = tables.getColumnValue(2);
            var stmt = `DROP TABLE IF EXISTS "${schema}"."${table}"`;
            snowflake.execute({sqlText: stmt});
            count++;
        }
        
        return `✅ Successfully dropped ${count} tables.`;
    } catch (err) {
        return `❌ Error: ${err}`;
    }
$$;