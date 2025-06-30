-- Cleanup: Drop all user-defined schemas and tables
-- Note: Must be run by a role with `OWNERSHIP` on target schemas

CALL drop_all_tables();

