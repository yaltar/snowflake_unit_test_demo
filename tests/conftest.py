"""
Pytest configuration and fixtures for SQL testing demo.

This module provides the main testing fixtures including:
- ctcontext: Main testing context with querier and database setup
- Environment configuration for switching between DuckDB and Snowflake
"""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path so we can import our modules
import sys
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

# Import logging utilities
from utils.logging import get_logger, vprint

# Create logger for conftest
logger = get_logger("conftest")

from db.querier import create_querier
from db.local_db import get_local_db_manager
from db.snowflake_test_manager import create_snowflake_test_manager


class CTContext:
    """
    Test context class that provides unified access to querier and database setup.
    
    This class encapsulates the test environment and provides a consistent
    interface for running SQL queries and managing test data across different
    database backends.
    """
    
    def __init__(self, use_snowflake: bool = False, test_id: str = None):
        """
        Initialize the test context.
        
        Args:
            use_snowflake: If True, use Snowflake backend, otherwise use DuckDB
            test_id: Unique identifier for this test run (used for Snowflake database naming)
        """
        self.use_snowflake = use_snowflake
        self.test_id = test_id or "default"
        
        # Set up database managers and queriers
        if use_snowflake:
            self._setup_snowflake_context()
        else:
            self._setup_duckdb_context()
    
    def _setup_duckdb_context(self):
        """Set up DuckDB context with database manager and querier."""
        self.db_manager = get_local_db_manager()
        self._setup_local_database()
        
        # Create querier using the SAME engine as the database manager
        # This ensures both use the same database instance
        from db.querier import DuckDBQuerier
        project_dialect = os.getenv('PROJECT_DIALECT', 'snowflake')
        self.querier = DuckDBQuerier(self.db_manager.engine, project_dialect)
        
        self.snowflake_manager = None
    
    def _setup_snowflake_context(self):
        """Set up Snowflake context with test database manager and querier."""
        # Create Snowflake test manager
        self.snowflake_manager = create_snowflake_test_manager(test_id=self.test_id)
        
        # Setup the test database environment
        test_data_path = Path(__file__).parent / "data"
        self.snowflake_manager.setup_complete_test_environment(str(test_data_path))
        
        # Create querier using the test database engine
        test_engine = self.snowflake_manager.get_engine()
        from db.querier import SnowflakeQuerier
        project_dialect = os.getenv('PROJECT_DIALECT', 'snowflake')
        self.querier = SnowflakeQuerier(test_engine, project_dialect)
        
        self.db_manager = None
    
    def _setup_local_database(self):
        """Set up the local DuckDB database with test data."""
        # Get the test data path
        test_data_path = Path(__file__).parent / "data"
        
        # Setup the database with tables and test data
        self.db_manager.setup_test_database(str(test_data_path))
    
    def reset_database(self):
        """Reset the database."""
        if self.use_snowflake:
            # For Snowflake, we don't reset during tests - each test gets a fresh database
            pass
        elif self.db_manager:
            test_data_path = Path(__file__).parent / "data"
            self.db_manager.reset_database(str(test_data_path))
    
    def cleanup(self):
        """Clean up database resources."""
        vprint(f"🧹 CTContext cleanup called (use_snowflake={self.use_snowflake})")
        if self.use_snowflake and self.snowflake_manager:
            vprint("🏗️  Calling Snowflake manager cleanup...")
            self.snowflake_manager.cleanup_test_database()
            vprint("✅ Snowflake manager cleanup completed")
        elif self.db_manager:
            vprint("🏗️  DuckDB cleanup is handled by reset_database")
            # DuckDB cleanup is handled by reset_database
            pass
        else:
            vprint("⚠️  No manager available for cleanup")
    
    def execute_sql(self, sql: str):
        """Execute SQL using the configured querier."""
        return self.querier.execute_sql(sql)
    
    def execute_sql_file(self, file_path: str):
        """Execute SQL from a file using the configured querier."""
        return self.querier.execute_sql_file(file_path)


@pytest.fixture(scope="session")
def use_snowflake():
    """
    Determine whether to use Snowflake based on environment configuration.
    
    Returns:
        bool: True if USE_SNOWFLAKE environment variable is set to '1' or 'true'
    """
    use_sf = os.getenv('USE_SNOWFLAKE', '0').lower()
    return use_sf in ('1', 'true', 'yes', 'on')


@pytest.fixture(scope="function")
def ctcontext(use_snowflake, request):
    """
    Main test fixture providing the CT (Cross-Testing) context.
    
    This fixture creates a CTContext instance configured for either
    DuckDB (local) or Snowflake (remote) testing based on environment
    variables.
    
    Args:
        use_snowflake: Boolean fixture indicating which backend to use
        request: Pytest request object for extracting test information
    
    Yields:
        CTContext: Configured test context with querier and database setup
    """
    # Extract test ID from pytest request
    test_id = _extract_test_id(request)
    
    context = CTContext(use_snowflake=use_snowflake, test_id=test_id)
    
    yield context
    
    # Cleanup
    try:
        vprint(f"🧹 Starting pytest fixture cleanup (use_snowflake={use_snowflake})")
        if use_snowflake:
            vprint("🏗️  Calling Snowflake cleanup...")
            context.cleanup()
            vprint("✅ Snowflake cleanup completed")
        else:
            vprint("🏗️  Calling DuckDB reset...")
            context.reset_database()
            vprint("✅ DuckDB reset completed")
    except Exception as e:
        # Log warning but don't fail the test
        logger.error(f"Failed to cleanup database: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")


@pytest.fixture(scope="function")
def sample_data():
    """
    Fixture providing sample data for tests that need to verify specific values.
    
    This fixture returns expected values that should match the test data
    loaded into the database.
    
    Returns:
        dict: Dictionary containing expected test values
    """
    return {
        "alice_revenue": 125.00,  # Expected revenue for Alice (1 Laptop + 1 Mouse = 100 + 25)
        "bob_revenue": 25.00,     # Expected revenue for Bob (1 Mouse = 25)
        "total_clients": 2,       # Total number of clients in test data
        "total_products": 3,      # Total number of products in test data
        "total_orders": 2,        # Total number of orders in test data
    }


# Additional utility fixtures

@pytest.fixture(scope="session")
def project_root():
    """Get the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def test_data_dir():
    """Get the test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture(scope="function")
def duckdb_querier():
    """Create a DuckDB querier for tests that specifically need DuckDB."""
    querier = create_querier(use_snowflake=False)
    db_manager = get_local_db_manager()
    
    # Setup database
    test_data_path = Path(__file__).parent / "data"
    db_manager.setup_test_database(str(test_data_path))
    
    yield querier
    
    # Cleanup
    try:
        db_manager.reset_database(str(test_data_path))
    except Exception as e:
        print(f"Warning: Failed to reset database: {e}")


# Configure pytest markers
pytest_plugins = []

def _extract_test_id(request) -> str:
    """
    Extract a test ID from the pytest request object.
    
    This creates a unique identifier for each test that can be used
    for database naming in Snowflake.
    
    Args:
        request: Pytest request object
        
    Returns:
        str: Sanitized test ID suitable for database naming
    """
    # Get the test node ID (e.g., "tests/test_revenue.py::test_client_revenue")
    node_id = request.node.nodeid
    
    # Extract test module and function name
    if "::" in node_id:
        # Split on :: to get file path and test name
        parts = node_id.split("::")
        test_file = parts[0].replace("/", "_").replace(".py", "")
        test_name = parts[-1]  # Get the last part (actual test function)
    else:
        # Fallback if format is different
        test_file = "unknown"
        test_name = "test"
    
    # Create a sanitized test ID
    test_id = f"{test_file}_{test_name}"
    
    # Sanitize for Snowflake database naming (alphanumeric + underscore only)
    import re
    test_id = re.sub(r'[^a-zA-Z0-9_]', '_', test_id)
    
    # Ensure it starts with a letter (Snowflake requirement)
    if test_id and not test_id[0].isalpha():
        test_id = f"test_{test_id}"
    
    # Limit length (Snowflake database names have limits)
    if len(test_id) > 50:
        test_id = test_id[:50]
    
    return test_id.upper()  # Snowflake prefers uppercase


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "snowflake: mark test as requiring Snowflake connection"
    )
    config.addinivalue_line(
        "markers", "duckdb: mark test as DuckDB-specific"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


def pytest_runtest_setup(item):
    """
    Setup hook to automatically skip tests based on environment and markers.
    
    This function runs before each test and automatically skips tests
    that are not compatible with the current database backend.
    """
    # Check if we're using Snowflake
    use_sf = os.getenv('USE_SNOWFLAKE', '0').lower() in ('1', 'true', 'yes', 'on')
    
    # Skip DuckDB-specific tests when using Snowflake
    if use_sf and item.get_closest_marker("duckdb"):
        pytest.skip("DuckDB-specific test skipped when using Snowflake backend")
    
    # Skip Snowflake-specific tests when using DuckDB
    if not use_sf and item.get_closest_marker("snowflake"):
        pytest.skip("Snowflake-specific test skipped when using DuckDB backend")