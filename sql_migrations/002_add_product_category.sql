-- ============================================================================
-- Migration: 002_add_product_category.sql
-- Description: Add category column to products table
-- Author: Generated from Alembic migration  
-- Date: 2025-07-01
-- ============================================================================

-- Ensure we're working in the correct schema
USE SCHEMA MY_SHOP;

-- Add category column with default value
ALTER TABLE products ADD COLUMN category VARCHAR(100) DEFAULT 'electronics';

-- Note: Index creation commented out due to Snowflake limitations
-- CREATE INDEX idx_products_category ON product (category);

COMMIT;