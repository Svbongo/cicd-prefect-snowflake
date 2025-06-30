-- Script to clean up all schemas in the database (except system schemas)
-- This should be the first script to run (version 0)

-- Set the database and schema context
USE DATABASE IDENTIFIER(CURRENT_DATABASE());

-- Generate and execute DROP SCHEMA statements for all non-system schemas
BEGIN
    LET drop_commands ARRAY;
    
    -- Get all non-system schemas
    LET schemas RESULTSET := (SELECT SCHEMA_NAME 
                             FROM INFORMATION_SCHEMA.SCHEMATA 
                             WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
                               AND SCHEMA_OWNER != 'SYSTEM');
    
    -- Build drop commands
    LET i INTEGER DEFAULT 0;
    FOR s IN schemas DO
        LET schema_name VARCHAR := s.SCHEMA_NAME;
        LET drop_stmt VARCHAR := 'DROP SCHEMA IF EXISTS ' || schema_name || ' CASCADE';
        
        -- Add to our array of commands
        drop_commands := ARRAY_APPEND(drop_commands, drop_stmt);
    END FOR;
    
    -- Execute all drop commands
    IF (ARRAY_SIZE(drop_commands) > 0) THEN
        EXECUTE IMMEDIATE 'BEGIN ' || 
                         ARRAY_TO_STRING(drop_commands, '; ') || 
                         '; END;';
    END IF;
    
    RETURN 'Dropped ' || ARRAY_SIZE(drop_commands) || ' schemas';
END;
