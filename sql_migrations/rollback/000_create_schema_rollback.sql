-- ============================================================================
-- Rollback: 000_create_schema_rollback.sql
-- Description: Drop MY_SHOP schema (WARNING: This will drop all tables!)
-- Author: Schema management rollback
-- Date: 2025-07-01
-- ============================================================================

-- WARNING: This will drop the entire schema and all its contents
-- Use with extreme caution in production environments

-- Drop the schema and all its contents
DROP SCHEMA IF EXISTS MY_SHOP CASCADE;

COMMIT;