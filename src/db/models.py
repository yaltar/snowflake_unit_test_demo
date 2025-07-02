"""
SQLAlchemy models for the e-commerce demo schema.

This module defines the database models for:
- clients: Customer information
- products: Product catalog
- orders: Customer orders
- order_lines: Individual items within orders

NOTE: This file is auto-generated from the Snowflake TEST_REF database
using sqlacodegen. Manual changes may be overwritten.

For DuckDB compatibility, use the metadata_adapter module to convert
these Snowflake-specific models to DuckDB-compatible metadata.
"""

from typing import Any, Optional

from snowflake.sqlalchemy.custom_types import TIMESTAMP_NTZ
from sqlalchemy import Column, DECIMAL, Identity, String, Table, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass


t_clients = Table(
    'clients', Base.metadata,
    Column('id', DECIMAL(38, 0), Identity(start=1, increment=1), nullable=False),
    Column('name', String(100), nullable=False),
    Column('email', String(255), nullable=False),
    Column('created_at', TIMESTAMP_NTZ),
    Column('updated_at', TIMESTAMP_NTZ)
)


t_order_lines = Table(
    'order_lines', Base.metadata,
    Column('id', DECIMAL(38, 0), Identity(start=1, increment=1), nullable=False),
    Column('order_id', DECIMAL(38, 0), nullable=False),
    Column('product_id', DECIMAL(38, 0), nullable=False),
    Column('quantity', DECIMAL(38, 0), nullable=False),
    Column('unit_price', DECIMAL(10, 2)),
    Column('created_at', TIMESTAMP_NTZ),
    Column('updated_at', TIMESTAMP_NTZ)
)


t_orders = Table(
    'orders', Base.metadata,
    Column('id', DECIMAL(38, 0), Identity(start=1, increment=1), nullable=False),
    Column('client_id', DECIMAL(38, 0), nullable=False),
    Column('date', TIMESTAMP_NTZ, nullable=False),
    Column('status', String(50), server_default=text("'pending'")),
    Column('created_at', TIMESTAMP_NTZ),
    Column('updated_at', TIMESTAMP_NTZ)
)


t_products = Table(
    'products', Base.metadata,
    Column('id', DECIMAL(38, 0), Identity(start=1, increment=1), nullable=False),
    Column('name', String(200), nullable=False),
    Column('price', DECIMAL(10, 2), nullable=False),
    Column('description', String(16777216)),
    Column('created_at', TIMESTAMP_NTZ),
    Column('updated_at', TIMESTAMP_NTZ),
    Column('category', String(100), server_default=text("'electronics'"))
)


class SchemaVersions(Base):
    __tablename__ = 'schema_versions'

    version_number: Mapped[str] = mapped_column(String(50), primary_key=True)
    migration_name: Mapped[str] = mapped_column(String(255))
    applied_at: Mapped[Optional[Any]] = mapped_column(TIMESTAMP_NTZ, server_default=text('CURRENT_TIMESTAMP()'))
    applied_by: Mapped[Optional[str]] = mapped_column(String(100), server_default=text('CURRENT_USER()'))
