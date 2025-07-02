-- ============================================================================
-- Rollback: 002_add_product_category_rollback.sql
-- Description: Remove category column from products table
-- Author: Generated from Alembic migration
-- Date: 2025-07-01  
-- ============================================================================

-- Ensure we're working in the correct schema
USE SCHEMA MY_SHOP;

-- Drop category column
ALTER TABLE product DROP COLUMN category;

COMMIT;