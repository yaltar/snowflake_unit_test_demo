"""
Querier classes for executing SQL against different database engines.

This module provides a unified interface for running SQL queries against
different database backends (DuckDB, Snowflake, etc.) with automatic
SQL dialect transpilation using SQLGlot.
"""

import os
import re
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import sqlglot
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class Querier(ABC):
    """Abstract base class for database queriers."""
    
    def __init__(self, engine: Engine, project_dialect: str = "snowflake"):
        """Initialize with a SQLAlchemy engine and project dialect.
        
        Args:
            engine: SQLAlchemy engine for database connection
            project_dialect: The SQL dialect used in the project's business logic
        """
        self.engine = engine
        self.project_dialect = project_dialect
    
    def _transpile_sql(self, sql: str) -> str:
        """Transpile SQL if project dialect differs from querier dialect.
        
        Args:
            sql: SQL query in project dialect
            
        Returns:
            SQL query transpiled to querier dialect (if needed)
        """
        querier_dialect = self.get_dialect()
        
        # Skip transpilation if dialects match
        if self.project_dialect == querier_dialect:
            return sql
        
        # Apply custom transpilation rules before SQLGlot
        sql_to_transpile = self._apply_custom_transpilation_rules(sql, querier_dialect)
        
        # Transpile SQL between dialects
        try:
            transpiled_sql = sqlglot.transpile(
                sql_to_transpile,
                read=self.project_dialect,
                write=querier_dialect
            )[0]
            return transpiled_sql
        except Exception as e:
            # If transpilation fails, try running the original SQL
            print(f"Warning: SQL transpilation failed: {e}")
            print(f"Attempting to run original SQL on {querier_dialect} engine...")
            return sql
    
    def _apply_custom_transpilation_rules(self, sql: str, target_dialect: str) -> str:
        """Apply custom transpilation rules for database-specific SQL patterns.
        
        This method handles SQL patterns that SQLGlot doesn't handle correctly
        or that need special conversion logic for specific database backends.
        
        Args:
            sql: Original SQL query
            target_dialect: Target database dialect
            
        Returns:
            SQL with custom transpilation rules applied
        """
        if target_dialect.lower() == "duckdb":
            return self._apply_duckdb_custom_rules(sql)
        
        return sql
    
    def _apply_duckdb_custom_rules(self, sql: str) -> str:
        """Apply DuckDB-specific custom transpilation rules.
        
        Args:
            sql: Original SQL query
            
        Returns:
            SQL with DuckDB-specific custom rules applied
        """
        # Handle LISTAGG(DISTINCT ...) WITHIN GROUP pattern
        # DuckDB doesn't support DISTINCT with WITHIN GROUP, but supports STRING_AGG with DISTINCT
        listagg_pattern = r'LISTAGG\s*\(\s*DISTINCT\s+([^,]+),\s*([^)]+)\s*\)\s*WITHIN\s+GROUP\s*\(\s*ORDER\s+BY\s+[^)]+\s*\)'
        
        def replace_listagg(match):
            column = match.group(1).strip()
            separator = match.group(2).strip()
            # Convert to DuckDB's STRING_AGG with DISTINCT
            return f"STRING_AGG(DISTINCT {column}, {separator})"
        
        # Apply the replacement
        sql = re.sub(listagg_pattern, replace_listagg, sql, flags=re.IGNORECASE)
        
        return sql
    
    def execute_sql(self, sql: str) -> pd.DataFrame:
        """Execute SQL and return results as a DataFrame.
        
        This method handles transpilation automatically and delegates
        to the concrete implementation.
        """
        transpiled_sql = self._transpile_sql(sql)
        return self._execute_sql_impl(transpiled_sql)
    
    @abstractmethod
    def _execute_sql_impl(self, sql: str) -> pd.DataFrame:
        """Execute SQL on the specific database engine.
        
        This method should be implemented by concrete querier classes
        and assumes the SQL is already in the correct dialect.
        """
        pass
    
    def execute_sql_file(self, file_path: str) -> pd.DataFrame:
        """Execute SQL from a file and return results as a DataFrame."""
        # Handle relative paths from the project root
        if not os.path.isabs(file_path):
            # Assuming we're running from project root or tests
            project_root = Path(__file__).parent.parent.parent
            full_path = project_root / "src" / file_path
        else:
            full_path = Path(file_path)
        
        if not full_path.exists():
            raise FileNotFoundError(f"SQL file not found: {full_path}")
        
        sql = full_path.read_text()
        return self.execute_sql(sql)
    
    @abstractmethod
    def get_dialect(self) -> str:
        """Return the SQL dialect name for this querier."""
        pass


class DuckDBQuerier(Querier):
    """Querier for DuckDB."""
    
    def get_dialect(self) -> str:
        return "duckdb"
    
    def _execute_sql_impl(self, sql: str) -> pd.DataFrame:
        """Execute SQL on DuckDB engine."""
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df


class SnowflakeQuerier(Querier):
    """Querier for Snowflake."""
    
    def get_dialect(self) -> str:
        return "snowflake"
    
    def _execute_sql_impl(self, sql: str) -> pd.DataFrame:
        """Execute SQL on Snowflake engine."""
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df


def create_querier(use_snowflake: bool = False, project_dialect: str = None, **kwargs) -> Querier:
    """
    Factory function to create the appropriate querier based on configuration.
    
    Args:
        use_snowflake: If True, creates a SnowflakeQuerier, otherwise DuckDBQuerier
        project_dialect: SQL dialect used in project's business logic. 
                        If None, reads from PROJECT_DIALECT env var, defaults to 'snowflake'
        **kwargs: Additional configuration parameters
    
    Returns:
        Querier instance
    """
    # Determine project dialect
    if project_dialect is None:
        project_dialect = os.getenv('PROJECT_DIALECT', 'snowflake')
    if use_snowflake:
        # Snowflake configuration
        account = kwargs.get('account') or os.getenv('SNOWFLAKE_ACCOUNT')
        user = kwargs.get('user') or os.getenv('SNOWFLAKE_USER')
        password = kwargs.get('password') or os.getenv('SNOWFLAKE_PASSWORD')
        database = kwargs.get('database') or os.getenv('SNOWFLAKE_DATABASE')
        schema = kwargs.get('schema') or os.getenv('SNOWFLAKE_SCHEMA')
        warehouse = kwargs.get('warehouse') or os.getenv('SNOWFLAKE_WAREHOUSE')
        role = kwargs.get('role') or os.getenv('SNOWFLAKE_ROLE')
        
        if not all([account, user, password, database, schema, warehouse]):
            raise ValueError(
                "Missing required Snowflake configuration. "
                "Please set SNOWFLAKE_* environment variables or pass them as arguments."
            )
        
        connection_string = (
            f"snowflake://{user}:{password}@{account}/"
            f"{database}/{schema}?warehouse={warehouse}"
        )
        if role:
            connection_string += f"&role={role}"
        
        engine = create_engine(connection_string)
        return SnowflakeQuerier(engine, project_dialect)
    
    else:
        # DuckDB configuration
        db_path = kwargs.get('db_path') or os.getenv('DUCKDB_PATH', ':memory:')
        engine = create_engine(f"duckdb:///{db_path}")
        return DuckDBQuerier(engine, project_dialect)