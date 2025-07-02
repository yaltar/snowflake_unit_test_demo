-- ============================================================================
-- Migration: 000_create_schema.sql
-- Description: Create MY_SHOP schema for e-commerce application
-- Author: Schema management migration
-- Date: 2025-07-01
-- ============================================================================

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS MY_SHOP;

-- Set the current schema context for subsequent operations
USE SCHEMA MY_SHOP;

-- Add a comment to document the schema purpose
COMMENT ON SCHEMA MY_SHOP IS 'E-commerce application schema containing clients, products, orders, and order lines';

COMMIT;