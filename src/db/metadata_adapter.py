"""
Metadata adapter for cross-database compatibility.

This module provides functionality to adapt SQLAlchemy metadata between
different database engines, specifically converting Snowflake-specific
types and constraints to DuckDB-compatible equivalents.
"""

import re
from typing import Dict, Any, Optional
from sqlalchemy import MetaData, Table, Column, Integer, String, DECIMAL, DateTime, Boolean
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql.sqltypes import TypeEngine
from sqlalchemy.dialects.sqlite import INTEGER as SQLITE_INTEGER

# Handle imports for both package and standalone usage
try:
    from ..utils.logging import get_logger, vprint
except ImportError:
    # Fallback for direct execution or when not run as package
    import sys
    from pathlib import Path
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))
    from utils.logging import get_logger, vprint

# Create logger for this module
logger = get_logger("metadata_adapter")


class MetadataAdapter:
    """
    Adapts SQLAlchemy metadata for cross-database compatibility.
    
    This class takes metadata generated from one database (e.g., Snowflake)
    and converts it to be compatible with another database (e.g., DuckDB).
    """
    
    def __init__(self, source_metadata: MetaData):
        """
        Initialize the adapter with source metadata.
        
        Args:
            source_metadata: SQLAlchemy metadata to adapt
        """
        self.source_metadata = source_metadata
        self.type_mappings = self._get_default_type_mappings()
    
    def _get_default_type_mappings(self) -> Dict[str, Any]:
        """Get default type mappings for Snowflake to DuckDB conversion."""
        return {
            # Snowflake DECIMAL(38,0) used for IDs -> DuckDB INTEGER
            'DECIMAL_38_0': Integer,
            # Snowflake large strings -> DuckDB VARCHAR
            'STRING_16777216': String,
            # Keep standard types as-is
            'VARCHAR': String,
            'DECIMAL': DECIMAL,
            'TIMESTAMP': DateTime,
            'BOOLEAN': Boolean,
            'INTEGER': Integer,
        }
    
    def _convert_column_type(self, column: Column) -> TypeEngine:
        """
        Convert a column type from Snowflake to DuckDB compatible type.
        
        Args:
            column: SQLAlchemy column to convert
            
        Returns:
            DuckDB-compatible SQLAlchemy type
        """
        column_type = column.type
        type_name = str(column_type)
        
        # Handle DECIMAL(38, 0) - typically used for IDs in Snowflake
        if 'DECIMAL' in type_name and '38' in type_name and '0' in type_name:
            return Integer()
        
        # Handle large VARCHAR/STRING types from Snowflake
        if 'STRING' in type_name and '16777216' in type_name:
            return String()

        if 'TIMESTAMP_NTZ' in type_name or 'TIMESTAMP' in type_name:
            return DateTime()
        
        # Handle VARCHAR with length
        varchar_match = re.match(r'VARCHAR\((\d+)\)', type_name)
        if varchar_match:
            length = int(varchar_match.group(1))
            return String(length)
        
        # Handle DECIMAL with precision/scale
        decimal_match = re.match(r'DECIMAL\((\d+),\s*(\d+)\)', type_name)
        if decimal_match:
            precision = int(decimal_match.group(1))
            scale = int(decimal_match.group(2))
            return DECIMAL(precision, scale)
        
        # For types we don't specifically handle, try to keep similar type
        if hasattr(column_type, '_type_affinity'):
            affinity = column_type._type_affinity
            if affinity == Integer:
                return Integer()
            elif affinity == String:
                return String()
            elif affinity == DECIMAL:
                return DECIMAL()
            elif affinity == DateTime:
                return DateTime()
        
        # Default fallback - return the original type
        return column_type
    
    def _should_skip_constraint(self, constraint_name: Optional[str]) -> bool:
        """
        Determine if a constraint should be skipped for DuckDB compatibility.
        
        Args:
            constraint_name: Name of the constraint
            
        Returns:
            True if constraint should be skipped
        """
        if not constraint_name:
            return False
        
        # Skip complex named constraints that might not be supported
        skip_patterns = [
            'FK_',  # Named foreign key constraints
            'CHK_', # Named check constraints
            'UQ_',  # Named unique constraints (except simple ones)
        ]
        
        return any(pattern in constraint_name for pattern in skip_patterns)
    
    def _convert_column(self, source_column: Column, target_table: Table) -> Column:
        """
        Convert a single column for DuckDB compatibility.
        
        Args:
            source_column: Original column from Snowflake metadata
            target_table: Target table for the new column
            
        Returns:
            DuckDB-compatible column
        """
        # Convert the column type
        new_type = self._convert_column_type(source_column)
        
        # For DuckDB, avoid autoincrement as it causes SERIAL type issues
        # We'll use simple INTEGER and handle ID generation in the application
        autoincrement = False
        if source_column.primary_key and isinstance(new_type, Integer):
            # Use simple Integer without autoincrement to avoid SERIAL
            new_type = Integer()
        
        # Create new column with converted type
        new_column = Column(
            source_column.name,
            new_type,
            nullable=source_column.nullable,
            primary_key=source_column.primary_key,
            autoincrement=autoincrement,
            # Skip server_default as it may contain Snowflake-specific functions
            # Skip Identity and complex constraints
        )
        
        return new_column
    
    def create_duckdb_metadata(self) -> MetaData:
        """
        Create DuckDB-compatible metadata from Snowflake metadata.
        
        Returns:
            New MetaData object with DuckDB-compatible tables and columns
        """
        # Create new metadata object
        target_metadata = MetaData()
        
        # Convert each table
        for table_name, source_table in self.source_metadata.tables.items():
            vprint(f"🔄 Converting table: {table_name}")
            
            # Create new table with same name
            target_table = Table(table_name, target_metadata)
            
            # Convert each column
            for column_name, source_column in source_table.columns.items():
                converted_column = self._convert_column(source_column, target_table)
                target_table.append_column(converted_column)
            
            # Skip most constraints for DuckDB compatibility
            # DuckDB will handle primary keys automatically
            
            vprint(f"✅ Converted table {table_name} with {len(target_table.columns)} columns")
        
        vprint(f"🎯 Created DuckDB metadata with {len(target_metadata.tables)} tables")
        return target_metadata
    
    def show_conversion_summary(self, target_metadata: MetaData) -> None:
        """
        Show summary of the conversion process.
        
        Args:
            target_metadata: The converted metadata
        """
        vprint("\n" + "=" * 60)
        vprint("📊 METADATA CONVERSION SUMMARY")
        vprint("=" * 60)
        
        for table_name, target_table in target_metadata.tables.items():
            source_table = self.source_metadata.tables[table_name]
            
            vprint(f"\n📋 Table: {table_name}")
            vprint(f"   Columns: {len(target_table.columns)}")
            
            # Show type conversions
            type_changes = []
            for col_name in target_table.columns.keys():
                source_type = str(source_table.columns[col_name].type)
                target_type = str(target_table.columns[col_name].type)
                if source_type != target_type:
                    type_changes.append(f"{col_name}: {source_type} → {target_type}")
            
            if type_changes:
                vprint("   Type conversions:")
                for change in type_changes:
                    vprint(f"     • {change}")
            else:
                vprint("   No type conversions needed")


def adapt_metadata_for_duckdb(source_metadata: MetaData) -> MetaData:
    """
    Convenience function to adapt metadata for DuckDB compatibility.
    
    Args:
        source_metadata: SQLAlchemy metadata from Snowflake
        
    Returns:
        DuckDB-compatible metadata
    """
    adapter = MetadataAdapter(source_metadata)
    target_metadata = adapter.create_duckdb_metadata()
    adapter.show_conversion_summary(target_metadata)
    return target_metadata


def preview_table_ddl(table: Table) -> str:
    """
    Preview the DDL that would be generated for a table.
    
    Args:
        table: SQLAlchemy table
        
    Returns:
        DDL string
    """
    from sqlalchemy import create_engine
    from sqlalchemy.dialects import sqlite  # Close enough to DuckDB for preview
    
    # Create a mock engine for DDL generation
    engine = create_engine("sqlite:///:memory:")
    
    # Generate CREATE TABLE statement
    create_ddl = CreateTable(table).compile(engine)
    return str(create_ddl)


if __name__ == "__main__":
    print("Metadata Adapter - for converting Snowflake metadata to DuckDB")
    print("Use this module by importing adapt_metadata_for_duckdb function")