"""
Snowflake test database management.

This module handles the creation and cleanup of isolated test databases
in Snowflake for each test run, ensuring test isolation and data integrity.
Uses pure SQL operations without SQLAlchemy models.
"""

import os
import time
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

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
logger = get_logger("snowflake_test_manager")


class SnowflakeTestManager:
    """
    Manages Snowflake test database lifecycle.
    
    This class handles:
    - Creating isolated test databases by cloning from a reference database
    - Loading test data into the test database
    - Cleaning up test databases after tests complete
    """
    
    def __init__(self, 
                 test_id: str,
                 reference_database: str = None,
                 keep_test_data: bool = False,
                 **connection_kwargs):
        """
        Initialize the Snowflake test manager.
        
        Args:
            test_id: Unique identifier for this test run
            reference_database: Name of the reference database to clone from
            keep_test_data: If True, don't drop the test database after tests
            **connection_kwargs: Snowflake connection parameters
        """
        self.test_id = test_id
        self.test_database_name = f"CT_{test_id}"
        self.reference_database = reference_database or os.getenv('SNOWFLAKE_REFERENCE_DATABASE')
        self.keep_test_data = keep_test_data or os.getenv('KEEP_TEST_DATA', '0').lower() in ('1', 'true', 'yes', 'on')
        
        # Store connection parameters
        self.connection_kwargs = connection_kwargs
        
        # Extract connection details from kwargs or environment
        self.account = connection_kwargs.get('account') or os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = connection_kwargs.get('user') or os.getenv('SNOWFLAKE_USER')
        self.password = connection_kwargs.get('password') or os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = connection_kwargs.get('warehouse') or os.getenv('SNOWFLAKE_WAREHOUSE')
        self.role = connection_kwargs.get('role') or os.getenv('SNOWFLAKE_ROLE')
        
        # Original database and schema (for reference)
        self.original_database = connection_kwargs.get('database') or os.getenv('SNOWFLAKE_DATABASE')
        self.original_schema = connection_kwargs.get('schema') or os.getenv('SNOWFLAKE_SCHEMA')
        
        # Validate required parameters
        if not all([self.account, self.user, self.password, self.warehouse]):
            raise ValueError(
                "Missing required Snowflake configuration. "
                "Please set SNOWFLAKE_* environment variables or pass them as arguments."
            )
        
        if not self.reference_database:
            # If no reference database specified, use the original database
            self.reference_database = self.original_database
            if not self.reference_database:
                raise ValueError(
                    "No reference database specified. Set SNOWFLAKE_REFERENCE_DATABASE "
                    "or provide it via parameter."
                )
        
        # Create engines for different operations
        self._admin_engine = None
        self._test_engine = None
    
    def _get_admin_engine(self) -> Engine:
        """Get an engine connected to the account level for admin operations."""
        if self._admin_engine is None:
            connection_string = f"snowflake://{self.user}:{self.password}@{self.account}/"
            if self.role:
                connection_string += f"?role={self.role}"
            if self.warehouse:
                if '?' in connection_string:
                    connection_string += f"&warehouse={self.warehouse}"
                else:
                    connection_string += f"?warehouse={self.warehouse}"
            
            self._admin_engine = create_engine(connection_string)
        return self._admin_engine
    
    def _get_test_engine(self) -> Engine:
        """Get an engine connected to the test database."""
        if self._test_engine is None:
            connection_string = (
                f"snowflake://{self.user}:{self.password}@{self.account}/"
                f"{self.test_database_name}/{self.original_schema}"
            )
            if self.role:
                connection_string += f"?role={self.role}"
            if self.warehouse:
                if '?' in connection_string:
                    connection_string += f"&warehouse={self.warehouse}"
                else:
                    connection_string += f"?warehouse={self.warehouse}"
            
            self._test_engine = create_engine(connection_string)
        return self._test_engine
    
    def create_test_database(self) -> None:
        """
        Create a test database by cloning from the reference database.
        """
        try:
            admin_engine = self._get_admin_engine()
            
            with admin_engine.connect() as conn:
                # Set warehouse context
                if self.warehouse:
                    conn.execute(text(f"USE WAREHOUSE {self.warehouse}"))
                
                # Drop existing test database if it exists
                conn.execute(text(f"DROP DATABASE IF EXISTS {self.test_database_name}"))
                
                # Clone the reference database
                clone_sql = f"""
                CREATE DATABASE {self.test_database_name} 
                CLONE {self.reference_database}
                """
                conn.execute(text(clone_sql))
                
                logger.success(f"Created test database: {self.test_database_name}")
                
        except SQLAlchemyError as e:
            raise RuntimeError(f"Failed to create test database {self.test_database_name}: {e}")
    
    def setup_test_schema(self) -> None:
        """
        Set up the test database schema using DDL deployment.
        
        This method deploys the schema using SQL DDL migration files.
        """
        try:
            import subprocess
            import sys
            from pathlib import Path
            
            # Set environment for Snowflake deployment
            env = os.environ.copy()
            env['USE_DUCKDB'] = '0'  # Force Snowflake
            env['SNOWFLAKE_REF_DATABASE'] = self.test_database_name
            env['SNOWFLAKE_SCHEMA'] = self.original_schema
            
            # Run deployment script
            project_root = Path(__file__).parent.parent.parent
            deploy_script = project_root / "scripts" / "deploy_sql_ddl.py"
            
            result = subprocess.run([
                sys.executable, str(deploy_script)
            ], env=env, capture_output=True, text=True, check=True)
            
            logger.success(f"Schema setup complete for database: {self.test_database_name}")
            
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to setup schema in {self.test_database_name}: {e.stderr}")
        except Exception as e:
            raise RuntimeError(f"Failed to setup schema in {self.test_database_name}: {e}")
    
    def load_test_data(self, data_path: str) -> None:
        """
        Load test data into the test database.
        
        Args:
            data_path: Path to the directory containing test data files
        """
        try:
            # Import here to avoid circular imports
            from .local_db import LocalDBManager
            
            # Create a temporary LocalDBManager that uses our test engine
            # We'll use its data loading logic but with our Snowflake engine
            test_engine = self._get_test_engine()
            temp_manager = LocalDBManager(":memory:")  # Dummy path
            temp_manager.engine = test_engine  # Override with our Snowflake engine
            temp_manager.SessionLocal.configure(bind=test_engine)
            
            # Load the test data
            temp_manager.load_test_data(data_path)
            
            logger.success(f"Test data loaded into database: {self.test_database_name}")
            
        except Exception as e:
            raise RuntimeError(f"Failed to load test data into {self.test_database_name}: {e}")
    
    def cleanup_test_database(self) -> None:
        """
        Clean up the test database by dropping it.
        
        This method respects the keep_test_data setting.
        """
        vprint(f"🧹 Starting cleanup for test database: {self.test_database_name}")
        
        if self.keep_test_data:
            vprint(f"⏸️  Keeping test database: {self.test_database_name} (KEEP_TEST_DATA=1)")
            return
        
        try:
            vprint(f"🔗 Getting admin engine for cleanup...")
            admin_engine = self._get_admin_engine()
            
            with admin_engine.connect() as conn:
                vprint(f"✅ Connected to Snowflake for cleanup")
                
                # Set warehouse context
                if self.warehouse:
                    vprint(f"🏭 Using warehouse: {self.warehouse}")
                    conn.execute(text(f"USE WAREHOUSE {self.warehouse}"))
                
                # Check if database exists before dropping
                check_sql = f"SHOW DATABASES LIKE '{self.test_database_name}'"
                vprint(f"🔍 Checking if database exists: {check_sql}")
                result = conn.execute(text(check_sql))
                exists = len(list(result)) > 0
                
                if exists:
                    vprint(f"📦 Database {self.test_database_name} exists, dropping...")
                    # Drop the test database
                    drop_sql = f"DROP DATABASE IF EXISTS {self.test_database_name}"
                    vprint(f"🗑️  Executing: {drop_sql}")
                    conn.execute(text(drop_sql))
                    logger.success(f"Successfully dropped test database: {self.test_database_name}")
                else:
                    logger.warning(f"Database {self.test_database_name} does not exist (already cleaned up?)")
                
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error during cleanup of {self.test_database_name}: {e}")
            logger.error(f"Error details: {type(e).__name__}")
            raise  # Re-raise to ensure we see the error
        except Exception as e:
            logger.error(f"Unexpected error during cleanup of {self.test_database_name}: {e}")
            logger.error(f"Error details: {type(e).__name__}")
            raise  # Re-raise to ensure we see the error
    
    def get_engine(self) -> Engine:
        """Get the SQLAlchemy engine for the test database."""
        return self._get_test_engine()
    
    def setup_complete_test_environment(self, data_path: str) -> None:
        """
        Complete setup of the test environment.
        
        This method:
        1. Creates the test database by cloning
        2. Sets up the schema
        3. Loads test data
        
        Args:
            data_path: Path to test data directory
        """
        vprint(f"Setting up Snowflake test environment for test_id: {self.test_id}")
        
        self.create_test_database()
        self.setup_test_schema()
        self.load_test_data(data_path)
        
        logger.success(f"Snowflake test environment ready: {self.test_database_name}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup_test_database()
        
        # Close engines
        if self._admin_engine:
            self._admin_engine.dispose()
        if self._test_engine:
            self._test_engine.dispose()


def create_snowflake_test_manager(test_id: str, **kwargs) -> SnowflakeTestManager:
    """
    Factory function to create a SnowflakeTestManager.
    
    Args:
        test_id: Unique identifier for this test run
        **kwargs: Additional configuration parameters
    
    Returns:
        SnowflakeTestManager instance
    """
    return SnowflakeTestManager(test_id=test_id, **kwargs)