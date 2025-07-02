"""
Example tests demonstrating the SQL testing framework.

This module shows how to use the ctcontext fixture to test
business logic SQL across different database engines.
"""

import pytest
import pandas as pd


def test_revenue_by_client(ctcontext, sample_data):
    """
    Test the revenue calculation per client using the business logic SQL.
    
    This test demonstrates:
    - Loading and executing SQL from a file
    - Verifying specific business logic results
    - Using the same test across DuckDB and Snowflake
    """
    # Execute the first query from our business logic SQL
    # For this test, we'll use a simple inline SQL based on the business logic
    sql = """
    SELECT
        c.name AS client,
        SUM(p.price * ol.quantity) AS revenue
    FROM clients c
    JOIN orders o ON o.client_id = c.id
    JOIN order_lines ol ON ol.order_id = o.id
    JOIN products p ON p.id = ol.product_id
    GROUP BY c.name
    ORDER BY c.name
    """
    
    result_df = ctcontext.execute_sql(sql)
    
    # Verify the results
    assert len(result_df) == 2, "Should have 2 clients"
    
    # Check Alice's revenue
    alice_row = result_df[result_df['client'] == 'Alice']
    assert len(alice_row) == 1, "Should have exactly one row for Alice"
    assert float(alice_row['revenue'].iloc[0]) == sample_data['alice_revenue'], \
        f"Alice's revenue should be {sample_data['alice_revenue']}"
    
    # Check Bob's revenue  
    bob_row = result_df[result_df['client'] == 'Bob']
    assert len(bob_row) == 1, "Should have exactly one row for Bob"
    assert float(bob_row['revenue'].iloc[0]) == sample_data['bob_revenue'], \
        f"Bob's revenue should be {sample_data['bob_revenue']}"


def test_client_order_summary(ctcontext):
    """
    Test a more complex query that joins multiple tables.
    
    This test shows how to verify aggregate calculations
    and demonstrates working with multiple columns.
    """
    sql = """
    SELECT
        c.name AS client,
        COUNT(DISTINCT o.id) AS total_orders,
        SUM(ol.quantity) AS total_items,
        SUM(p.price * ol.quantity) AS total_revenue
    FROM clients c
    JOIN orders o ON o.client_id = c.id  
    JOIN order_lines ol ON ol.order_id = o.id
    JOIN products p ON p.id = ol.product_id
    GROUP BY c.name
    ORDER BY total_revenue DESC
    """
    
    result_df = ctcontext.execute_sql(sql)
    
    # Verify structure
    expected_columns = ['client', 'total_orders', 'total_items', 'total_revenue']
    assert list(result_df.columns) == expected_columns, \
        f"Expected columns {expected_columns}, got {list(result_df.columns)}"
    
    # Verify Alice (should be first due to higher revenue)
    alice_row = result_df.iloc[0]
    assert alice_row['client'] == 'Alice'
    assert alice_row['total_orders'] == 1
    assert alice_row['total_items'] == 2  # 1 laptop + 1 mouse
    assert float(alice_row['total_revenue']) == 125.00
    
    # Verify Bob  
    bob_row = result_df.iloc[1]
    assert bob_row['client'] == 'Bob'
    assert bob_row['total_orders'] == 1
    assert bob_row['total_items'] == 1  # 1 mouse
    assert float(bob_row['total_revenue']) == 25.00


def test_product_sales(ctcontext):
    """
    Test product-focused analytics.
    
    This test demonstrates querying from a product perspective
    and shows how to handle different aggregation levels.
    """
    sql = """
    SELECT
        p.name AS product_name,
        SUM(ol.quantity) AS total_quantity_sold,
        COUNT(DISTINCT ol.order_id) AS orders_containing_product,
        SUM(p.price * ol.quantity) AS total_revenue
    FROM products p
    JOIN order_lines ol ON ol.product_id = p.id
    GROUP BY p.id, p.name
    ORDER BY total_quantity_sold DESC
    """
    
    result_df = ctcontext.execute_sql(sql)
    
    # Mouse should be the top-selling product (appears in 2 orders)
    mouse_row = result_df[result_df['product_name'] == 'Mouse']
    assert len(mouse_row) == 1
    assert mouse_row['total_quantity_sold'].iloc[0] == 2
    assert mouse_row['orders_containing_product'].iloc[0] == 2
    assert float(mouse_row['total_revenue'].iloc[0]) == 50.00  # 2 * 25
    
    # Laptop should appear once
    laptop_row = result_df[result_df['product_name'] == 'Laptop']
    assert len(laptop_row) == 1
    assert laptop_row['total_quantity_sold'].iloc[0] == 1
    assert laptop_row['orders_containing_product'].iloc[0] == 1
    assert float(laptop_row['total_revenue'].iloc[0]) == 100.00


@pytest.mark.parametrize("expected_count,table_name", [
    (2, "clients"),
    (3, "products"), 
    (2, "orders"),
    (3, "order_lines")
])
def test_data_integrity(ctcontext, expected_count, table_name):
    """
    Test that the test data was loaded correctly.
    
    This parameterized test verifies that all test data
    is present in the database.
    """
    sql = f"SELECT COUNT(*) as count FROM {table_name}"
    result_df = ctcontext.execute_sql(sql)
    
    actual_count = result_df['count'].iloc[0]
    assert actual_count == expected_count, \
        f"Expected {expected_count} rows in {table_name}, got {actual_count}"


def test_sql_transpilation_works(ctcontext):
    """
    Test that demonstrates SQL transpilation is working.
    
    This test uses some Snowflake-specific syntax to verify
    that SQLGlot is properly transpiling to DuckDB when needed.
    """
    # Use a query with some Snowflake-style functions
    sql = """
    SELECT 
        c.name,
        COUNT(*) as order_count
    FROM clients c
    JOIN orders o ON c.id = o.client_id
    GROUP BY c.name
    HAVING COUNT(*) > 0
    ORDER BY c.name
    """
    
    result_df = ctcontext.execute_sql(sql)
    
    # Should work regardless of backend
    assert len(result_df) == 2
    assert 'Alice' in result_df['name'].values
    assert 'Bob' in result_df['name'].values


# Test specifically for DuckDB (will be skipped on Snowflake)
@pytest.mark.duckdb
def test_duckdb_specific_features(duckdb_querier):
    """
    Test DuckDB-specific functionality.
    
    This test will only run when using DuckDB backend.
    """
    sql = "SELECT 'DuckDB test' as message"
    result_df = duckdb_querier.execute_sql(sql)
    
    assert result_df['message'].iloc[0] == 'DuckDB test'


# Test that requires Snowflake (will be skipped on DuckDB)
@pytest.mark.snowflake
def test_snowflake_specific_features(ctcontext):
    """
    Test Snowflake-specific functionality.
    
    This test will only run when using Snowflake backend.
    """
    # This would only run if USE_SNOWFLAKE=1 is set
    if not ctcontext.use_snowflake:
        pytest.skip("Test requires Snowflake backend")
    
    sql = "SELECT CURRENT_WAREHOUSE() as warehouse"
    result_df = ctcontext.execute_sql(sql)
    
    # Just verify we got a result (warehouse name will vary)
    assert len(result_df) == 1
    assert 'warehouse' in result_df.columns