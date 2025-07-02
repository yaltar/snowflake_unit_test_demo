-- ============================================================================
-- Migration: 001_initial_schema.sql
-- Description: Create initial e-commerce tables with proper constraints
-- Author: Generated from Alembic migration
-- Date: 2025-07-01
-- ============================================================================

-- Ensure we're working in the correct schema
USE SCHEMA MY_SHOP;

-- Create clients table
CREATE TABLE clients (
    id INTEGER NOT NULL AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    CONSTRAINT uq_clients_email UNIQUE (email)
);

-- Create products table  
CREATE TABLE products (
    id INTEGER NOT NULL AUTOINCREMENT,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Create orders table
CREATE TABLE orders (
    id INTEGER NOT NULL AUTOINCREMENT,
    client_id INTEGER NOT NULL,
    date TIMESTAMP NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Create order_lines table
CREATE TABLE order_lines (
    id INTEGER NOT NULL AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Note: Indexes are not created in this migration due to Snowflake limitations
-- with CREATE INDEX on standard tables. Consider using clustering keys or
-- Enterprise Edition features for better performance.

COMMIT;