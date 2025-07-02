"""
Local database setup for DuckDB.

This module handles the initialization of the DuckDB database using SQLAlchemy
models with metadata adaptation for cross-database compatibility. Schema creation
uses adapted metadata from Snowflake models, and data loading uses direct SQL
INSERT statements.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

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
logger = get_logger("local_db")

try:
    from .models import Base
    from .metadata_adapter import adapt_metadata_for_duckdb
    MODELS_AVAILABLE = True
except ImportError:
    MODELS_AVAILABLE = False
    logger.warning("SQLAlchemy models not available. Run 'python scripts/generate_models.py' first.")


class LocalDBManager:
    """Manages the local DuckDB database setup using adapted SQLAlchemy metadata."""
    
    def __init__(self, db_path: str = ":memory:", schema_name: str = "main"):
        """
        Initialize the local database manager.
        
        Args:
            db_path: Path to the DuckDB database file. Defaults to in-memory database.
            schema_name: Database schema name. Defaults to main for DuckDB.
        """
        self.db_path = db_path
        self.schema_name = schema_name
        self.engine = create_engine(f"duckdb:///{db_path}")
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.adapted_metadata = None
    
    def execute_sql(self, sql: str, params: dict = None, use_schema: bool = True):
        """Execute SQL statement."""
        with self.engine.connect() as conn:
            # Ensure we're in the correct schema if requested and schema exists
            if use_schema and self.schema_name and self.schema_name != 'main':
                try:
                    conn.execute(text(f"USE {self.schema_name}"))
                except Exception:
                    # Schema might not exist yet, continue without it
                    pass
            
            # Execute the SQL
            result = conn.execute(text(sql), params or {})
            
            # COMMIT the transaction explicitly for INSERT/UPDATE/DELETE
            if sql.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                conn.commit()
            
            return result
    
    def get_session(self):
        """Get a new database session."""
        return self.SessionLocal()
    
    def create_tables_from_metadata(self) -> None:
        """Create tables using adapted SQLAlchemy metadata."""
        if not MODELS_AVAILABLE:
            raise RuntimeError("SQLAlchemy models not available. Run 'python scripts/generate_models.py' first.")
        
        vprint("🔄 Adapting Snowflake metadata for DuckDB compatibility...")
        
        # Adapt the metadata from Snowflake models to DuckDB
        self.adapted_metadata = adapt_metadata_for_duckdb(Base.metadata)
        
        vprint("🔨 Creating tables using adapted metadata...")
        
        # Create all tables using the adapted metadata
        self.adapted_metadata.create_all(bind=self.engine)
        
        logger.success("Tables created successfully using adapted metadata!")
    
    def drop_tables(self) -> None:
        """Drop all tables."""
        if self.adapted_metadata:
            self.adapted_metadata.drop_all(bind=self.engine)
        elif MODELS_AVAILABLE:
            # Fallback to original metadata
            Base.metadata.drop_all(bind=self.engine)
    
    def load_test_data(self, data_path: str = "tests/data") -> None:
        """
        Load test data from JSON files into the database.
        
        Args:
            data_path: Path to the directory containing JSON data files.
        """
        vprint(f"🔄 Loading test data from: {data_path}")
        
        # Handle relative paths
        if not os.path.isabs(data_path):
            project_root = Path(__file__).parent.parent.parent
            data_path = project_root / data_path
        else:
            data_path = Path(data_path)
        
        if not data_path.exists():
            raise FileNotFoundError(f"Test data directory not found: {data_path}")
        
        vprint(f"📁 Data directory exists: {data_path}")
        
        # Load data in order (respecting foreign key dependencies)
        self._load_clients(data_path / "clients.json")
        self._load_products(data_path / "products.json")
        self._load_orders(data_path / "orders.json")
        self._load_order_lines(data_path / "order_lines.json")
        
        logger.success("Test data loading completed")
    
    def _load_json_data(self, file_path: Path) -> List[Dict[str, Any]]:
        """Load data from a JSON file."""
        if not file_path.exists():
            logger.warning(f"Data file not found: {file_path}")
            return []
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Handle both list and dict formats
        if isinstance(data, dict):
            return [data]
        return data
    
    def _load_clients(self, file_path: Path) -> None:
        """Load client data from JSON file using pure SQL."""
        vprint(f"📥 Loading clients from: {file_path}")
        data = self._load_json_data(file_path)
        if not data:
            logger.warning("No client data found")
            return
        
        vprint(f"📊 Loading {len(data)} clients")
        for client_data in data:
            # Build SQL based on available fields
            columns = []
            values = []
            params = {}
            
            for field in ['id', 'name', 'email', 'created_at', 'updated_at']:
                if field in client_data:
                    columns.append(field)
                    values.append(f':{field}')
                    params[field] = client_data[field]
            
            if columns:
                sql = f"INSERT INTO clients ({', '.join(columns)}) VALUES ({', '.join(values)})"
                vprint(f"🔧 Executing SQL: {sql}")
                vprint(f"🔧 With params: {params}")
                try:
                    self.execute_sql(sql, params)
                    vprint(f"✅ Client inserted successfully")
                except Exception as e:
                    logger.error(f"Failed to insert client: {e}")
                    raise
    
    def _load_products(self, file_path: Path) -> None:
        """Load product data from JSON file using pure SQL."""
        data = self._load_json_data(file_path)
        if not data:
            return
        
        for product_data in data:
            # Build SQL based on available fields
            columns = []
            values = []
            params = {}
            
            for field in ['id', 'name', 'price', 'description', 'created_at', 'updated_at', 'category']:
                if field in product_data:
                    columns.append(field)
                    values.append(f':{field}')
                    params[field] = product_data[field]
            
            if columns:
                sql = f"INSERT INTO products ({', '.join(columns)}) VALUES ({', '.join(values)})"
                self.execute_sql(sql, params)
    
    def _load_orders(self, file_path: Path) -> None:
        """Load order data from JSON file using pure SQL."""
        data = self._load_json_data(file_path)
        if not data:
            return
        
        for order_data in data:
            # Handle date parsing if needed
            if 'date' in order_data and isinstance(order_data['date'], str):
                from datetime import datetime
                order_data['date'] = datetime.fromisoformat(order_data['date'])
            
            # Build SQL based on available fields
            columns = []
            values = []
            params = {}
            
            for field in ['id', 'client_id', 'date', 'status', 'created_at', 'updated_at']:
                if field in order_data:
                    columns.append(field)
                    values.append(f':{field}')
                    params[field] = order_data[field]
            
            if columns:
                sql = f'INSERT INTO orders ({", ".join(columns)}) VALUES ({", ".join(values)})'
                self.execute_sql(sql, params)
    
    def _load_order_lines(self, file_path: Path) -> None:
        """Load order line data from JSON file using pure SQL."""
        data = self._load_json_data(file_path)
        if not data:
            return
        
        for order_line_data in data:
            # Build SQL based on available fields
            columns = []
            values = []
            params = {}
            
            for field in ['id', 'order_id', 'product_id', 'quantity', 'unit_price', 'created_at', 'updated_at']:
                if field in order_line_data:
                    columns.append(field)
                    values.append(f':{field}')
                    params[field] = order_line_data[field]
            
            if columns:
                sql = f"INSERT INTO order_lines ({', '.join(columns)}) VALUES ({', '.join(values)})"
                self.execute_sql(sql, params)
    
    def setup_test_database(self, data_path: str = "tests/data") -> None:
        """
        Complete setup of the test database with schema and data.
        
        Args:
            data_path: Path to the directory containing JSON data files.
        """
        # Create tables using adapted metadata
        self.create_tables_from_metadata()
        
        # Load test data
        self.load_test_data(data_path)
        
        # Diagnostic: check that data was loaded
        self._diagnostic_check_data()
    
    def reset_database(self, data_path: str = "tests/data") -> None:
        """
        Reset the database by dropping and recreating tables and data.
        
        Args:
            data_path: Path to the directory containing JSON data files.
        """
        self.drop_tables()
        self.setup_test_database(data_path)
    
    def _diagnostic_check_data(self) -> None:
        """Diagnostic method to verify data was loaded correctly."""
        try:
            # Check if tables exist and have data
            tables = ['clients', 'products', 'orders', 'order_lines']  # "order" needs quotes
            
            with self.engine.connect() as conn:
                for table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
                    count = result.fetchone()[0]
                    vprint(f"📊 Table {table}: {count} rows")
                    
            logger.success("Database diagnostic completed")
            
        except Exception as e:
            logger.error(f"Database diagnostic failed: {e}")


def get_local_db_manager(db_path: str = None) -> LocalDBManager:
    """
    Factory function to create a LocalDBManager instance.
    
    Args:
        db_path: Optional path to the DuckDB database file.
                 If None, uses environment variable or defaults to in-memory.
    
    Returns:
        LocalDBManager instance
    """
    if db_path is None:
        db_path = os.getenv('DUCKDB_PATH', ':memory:')
    
    return LocalDBManager(db_path)