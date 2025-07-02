-- ============================================================================
-- Rollback: 001_initial_schema_rollback.sql  
-- Description: Drop all tables created in initial schema migration
-- Author: Generated from Alembic migration
-- Date: 2025-07-01
-- ============================================================================

-- Ensure we're working in the correct schema
USE SCHEMA MY_SHOP;

-- Drop tables in reverse order (respecting foreign keys)
DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS "order";
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS client;

COMMIT;