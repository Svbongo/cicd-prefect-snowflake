-- =============================================
-- DROP ALL NON-SYSTEM SCHEMAS IN DATABASE
-- WARNING: This permanently deletes all schemas and their contents.
-- =============================================

USE DATABASE IDENTIFIER(CURRENT_DATABASE());

BEGIN
    LET drop_commands ARRAY;

    -- Get all user-created schemas, excluding system schemas
    LET schemas RESULTSET := (
        SELECT SCHEMA_NAME 
        FROM INFORMATION_SCHEMA.SCHEMATA 
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
          AND SCHEMA_OWNER != 'SYSTEM'
    );

    -- Loop through schemas and build DROP statements
    FOR s IN schemas DO
        LET schema_name VARCHAR := s.SCHEMA_NAME;
        LET drop_stmt VARCHAR := 'DROP SCHEMA IF EXISTS "' || schema_name || '" CASCADE';
        
        drop_commands := ARRAY_APPEND(drop_commands, drop_stmt);
    END FOR;

    -- Execute all DROP statements together
    IF (ARRAY_SIZE(drop_commands) > 0) THEN
        EXECUTE IMMEDIATE 'BEGIN ' || 
                          ARRAY_TO_STRING(drop_commands, '; ') || 
                          '; END;';
    END IF;

    RETURN '✅ Dropped ' || ARRAY_SIZE(drop_commands) || ' schemas (excluding system schemas)';
END;
