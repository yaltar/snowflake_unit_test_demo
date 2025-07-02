"""
Legacy Test Module - BEFORE MIGRATION

This test module demonstrates the "old way" of testing analytics with direct Snowflake connections.
This testing approach has several problems:
- Requires live Snowflake connection for every test
- No local testing capability
- Manual test data setup/teardown
- Tightly coupled to Snowflake environment
- Slow test execution
- Difficult to run in CI/CD without Snowflake access

This will be migrated to the modern ctcontext-based testing framework.
"""

import os
import pytest
import snowflake.connector
from decimal import Decimal
from src.legacy.legacy_analytics import LegacySnowflakeAnalytics, LegacyCustomerInsight

@pytest.mark.skip(reason="Legacy tests are being ignored for now")
class TestLegacyAnalytics:
    """
    Legacy test class using direct Snowflake connections.
    
    Problems with this approach:
    1. Requires live Snowflake for every test - slow and expensive
    2. Manual connection management - error prone
    3. No test isolation - tests can interfere with each other
    4. Manual test data setup - complex and brittle
    5. Cannot run locally without Snowflake access
    6. Difficult to run in CI/CD pipelines
    """
    
    @classmethod
    def setup_class(cls):
        """
        Manual test setup - creates Snowflake connection and test data.
        
        Problems:
        - Manual connection management
        - Hardcoded test data insertion
        - No cleanup if setup fails
        - Requires Snowflake credentials in test environment
        """
        # Skip tests if Snowflake not configured
        required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
        if not all(os.getenv(var) for var in required_vars):
            pytest.skip("Snowflake credentials not configured for legacy tests")
        
        # Manual connection creation
        cls.connection = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_REF_DATABASE', 'TEST_REF'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'MY_SHOP'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        
        # Manual test data setup - brittle and hardcoded
        cls._setup_test_data()
    
    @classmethod
    def _setup_test_data(cls):
        """
        Manual test data insertion - brittle and error-prone.
        
        Problems:
        - Hardcoded test data
        - No data isolation between tests
        - Manual SQL execution
        - No rollback mechanism
        - Data might conflict with other tests
        """
        cursor = cls.connection.cursor()
        
        try:
            # Clear existing data (dangerous!)
            cursor.execute("DELETE FROM order_lines")
            cursor.execute("DELETE FROM orders") 
            cursor.execute("DELETE FROM products")
            cursor.execute("DELETE FROM clients")
            
            # Insert hardcoded test data
            cursor.execute("""
                INSERT INTO clients (id, name, email) VALUES
                (1, 'Legacy Customer A', 'customerA@legacy.com'),
                (2, 'Legacy Customer B', 'customerB@legacy.com')
            """)
            
            cursor.execute("""
                INSERT INTO products (id, name, price) VALUES
                (1, 'Legacy Product X', 100.00),
                (2, 'Legacy Product Y', 50.00),
                (3, 'Legacy Product Z', 75.00)
            """)
            
            cursor.execute("""
                INSERT INTO orders (id, client_id, date) VALUES
                (1, 1, '2024-01-15'),
                (2, 2, '2024-01-20'),
                (3, 1, '2024-02-10')
            """)
            
            cursor.execute("""
                INSERT INTO order_lines (id, order_id, product_id, quantity) VALUES
                (1, 1, 1, 2),
                (2, 1, 2, 1),
                (3, 2, 2, 3),
                (4, 3, 3, 1)
            """)
            
        finally:
            cursor.close()
    
    @classmethod
    def teardown_class(cls):
        """
        Manual cleanup - often forgotten or fails.
        
        Problems:
        - Manual resource management
        - Cleanup might fail silently
        - Test data might remain in database
        """
        if hasattr(cls, 'connection') and cls.connection:
            cls.connection.close()
    
    def test_customer_insights_structure(self):
        """
        Test customer insights return correct structure.
        
        Problems:
        - Requires live Snowflake connection
        - Slow execution (network calls)
        - Brittle - depends on exact test data
        - No isolation from other tests
        """
        analytics = LegacySnowflakeAnalytics()
        
        insights = analytics.get_customer_insights()
        
        # Basic structure validation
        assert isinstance(insights, list)
        assert len(insights) > 0
        
        # Validate first insight structure
        first_insight = insights[0]
        assert isinstance(first_insight, LegacyCustomerInsight)
        assert isinstance(first_insight.customer_name, str)
        assert isinstance(first_insight.total_revenue, Decimal)
        assert isinstance(first_insight.order_count, int)
        assert isinstance(first_insight.avg_order_value, Decimal)
        assert isinstance(first_insight.favorite_products, str)
        
        # Brittle assertions based on hardcoded test data
        assert first_insight.total_revenue > Decimal('0')
        assert first_insight.order_count > 0
        assert len(first_insight.favorite_products) > 0
    
    def test_customer_insights_business_logic(self):
        """
        Test customer insights business logic calculations.
        
        Problems:
        - Hardcoded expectations based on manual test data
        - Fragile - breaks if test data changes
        - No way to control or isolate test scenarios
        - Difficult to test edge cases
        """
        analytics = LegacySnowflakeAnalytics()
        
        insights = analytics.get_customer_insights()
        
        # Find specific customers (brittle - depends on exact test data)
        customer_a = next((i for i in insights if 'Customer A' in i.customer_name), None)
        assert customer_a is not None, "Legacy Customer A should exist"
        
        # Hardcoded business logic validation (brittle)
        # Customer A: 2 orders (Order 1: 2×$100 + 1×$50 = $250, Order 3: 1×$75 = $75)
        # Total: $325, Average: $162.50
        assert customer_a.total_revenue == Decimal('325.00')
        assert customer_a.order_count == 2
        assert customer_a.avg_order_value == Decimal('162.50')
        
        # Product list validation (fragile string matching)
        assert 'Legacy Product X' in customer_a.favorite_products
        assert 'Legacy Product Z' in customer_a.favorite_products
    
    def test_monthly_sales_trend(self):
        """
        Test monthly sales trend analysis.
        
        Problems:
        - Requires live Snowflake for date functions
        - Hardcoded date expectations
        - No control over time-based test scenarios
        - Difficult to test different time periods
        """
        analytics = LegacySnowflakeAnalytics()
        
        trends = analytics.get_monthly_sales_trend()
        
        # Basic structure validation
        assert isinstance(trends, list)
        assert len(trends) > 0
        
        # Validate structure of trend data
        first_trend = trends[0]
        assert 'SALES_MONTH' in first_trend
        assert 'TOTAL_ORDERS' in first_trend
        assert 'UNIQUE_CUSTOMERS' in first_trend
        assert 'MONTHLY_REVENUE' in first_trend
        assert 'AVG_ORDER_VALUE' in first_trend
        
        # Brittle business logic validation
        # Based on hardcoded test data: January 2024 should have 2 orders
        january_trend = next((t for t in trends if '2024-01' in str(t['SALES_MONTH'])), None)
        if january_trend:
            assert january_trend['TOTAL_ORDERS'] == 2
            assert january_trend['UNIQUE_CUSTOMERS'] == 2
            assert january_trend['MONTHLY_REVENUE'] == Decimal('400.00')  # $250 + $150
    
    def test_product_performance_analysis(self):
        """
        Test product performance analysis.
        
        Problems:
        - Returns DataFrame instead of business objects
        - Hardcoded product expectations
        - Complex validation logic
        - No abstraction for test assertions
        """
        analytics = LegacySnowflakeAnalytics()
        
        products_df = analytics.analyze_product_performance()
        
        # Basic DataFrame validation
        assert not products_df.empty
        assert len(products_df) > 0
        
        # Validate column structure
        expected_columns = ['NAME', 'PRICE', 'TOTAL_SOLD', 'ORDERS_COUNT', 
                          'UNIQUE_CUSTOMERS', 'TOTAL_REVENUE', 'SALES_RANK', 
                          'REVENUE_RANK', 'CUSTOMER_LIST', 'PRODUCT_CATEGORY']
        for col in expected_columns:
            assert col in products_df.columns
        
        # Brittle product-specific validation
        product_x = products_df[products_df['NAME'] == 'Legacy Product X']
        assert not product_x.empty
        assert product_x['TOTAL_SOLD'].iloc[0] == 2  # Hardcoded expectation
        assert product_x['TOTAL_REVENUE'].iloc[0] == Decimal('200.00')  # 2 × $100
    
    @pytest.mark.slow
    def test_full_business_report_integration(self):
        """
        Integration test for full business report.
        
        Problems:
        - Very slow (multiple Snowflake calls)
        - Marked as @pytest.mark.slow (indicates problem)
        - No way to speed up or mock
        - Expensive to run frequently
        - Blocks CI/CD pipelines
        """
        from src.legacy.legacy_analytics import generate_legacy_business_report
        
        # This is slow because it makes multiple Snowflake calls
        report = generate_legacy_business_report()
        
        # Basic integration validation
        assert 'customer_insights' in report
        assert 'sales_trends' in report
        assert 'product_performance' in report
        
        # Ensure all components have data
        assert len(report['customer_insights']) > 0
        assert len(report['sales_trends']) > 0
        assert not report['product_performance'].empty


# Pytest configuration for legacy tests
def pytest_configure(config):
    """
    Configure pytest for legacy tests.
    
    Problems demonstrated:
    - Need special configuration for legacy tests
    - Slow tests require special handling
    - Complex setup for Snowflake-dependent tests
    """
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (require live Snowflake connection)"
    )