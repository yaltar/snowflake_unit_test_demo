"""
Tests for analytics services.

This module contains comprehensive tests for the business logic services,
including unit tests, integration tests, and cross-database validation.
"""

import pytest
from decimal import Decimal
from unittest.mock import Mock, MagicMock
import pandas as pd
from src.services.analytics_service import (
    RevenueAnalytics, OrderAnalytics, ProductAnalytics, AdvancedAnalytics,
    ClientRevenue, ProductSales, OrderSummary
)
from db.querier import Querier


class TestRevenueAnalytics:
    """Tests for RevenueAnalytics service."""
    
    def test_get_revenue_by_client_with_real_data(self, ctcontext, sample_data):
        """Test revenue calculation with real test data."""
        service = RevenueAnalytics(ctcontext.querier)
        
        revenues = service.get_revenue_by_client()
        
        # Should have 2 clients
        assert len(revenues) == 2
        
        # Check specific values based on our test data
        alice_revenue = next((r for r in revenues if r.client_name == 'Alice'), None)
        bob_revenue = next((r for r in revenues if r.client_name == 'Bob'), None)
        
        assert alice_revenue is not None
        assert bob_revenue is not None
        
        # Verify expected revenue amounts (from sample_data fixture)
        assert alice_revenue.total_revenue == Decimal(str(sample_data['alice_revenue']))
        assert bob_revenue.total_revenue == Decimal(str(sample_data['bob_revenue']))
        
        # Verify data types
        assert isinstance(alice_revenue, ClientRevenue)
        assert isinstance(alice_revenue.total_revenue, Decimal)
    
    def test_get_total_revenue_with_real_data(self, ctcontext, sample_data):
        """Test total revenue calculation."""
        service = RevenueAnalytics(ctcontext.querier)
        
        total_revenue = service.get_total_revenue()
        
        # Should equal sum of Alice and Bob revenues
        expected_total = Decimal(str(sample_data['alice_revenue'] + sample_data['bob_revenue']))
        assert total_revenue == expected_total
        assert isinstance(total_revenue, Decimal)
    
    def test_get_revenue_by_client_with_mock(self):
        """Test revenue calculation with mocked querier."""
        # Mock querier
        mock_querier = Mock(spec=Querier)
        mock_df = pd.DataFrame([
            {'client_name': 'Test Client', 'total_revenue': 100.50}
        ])
        mock_querier.execute_sql.return_value = mock_df
        
        service = RevenueAnalytics(mock_querier)
        revenues = service.get_revenue_by_client()
        
        # Verify service called querier correctly
        mock_querier.execute_sql.assert_called_once()
        sql_call = mock_querier.execute_sql.call_args[0][0]
        assert 'SELECT' in sql_call
        assert 'SUM(p.price * ol.quantity)' in sql_call
        
        # Verify results
        assert len(revenues) == 1
        assert revenues[0].client_name == 'Test Client'
        assert revenues[0].total_revenue == Decimal('100.50')
    
    def test_get_revenue_by_client_sql_error(self):
        """Test error handling when SQL fails."""
        mock_querier = Mock(spec=Querier)
        mock_querier.execute_sql.side_effect = Exception("Database connection failed")
        
        service = RevenueAnalytics(mock_querier)
        
        with pytest.raises(RuntimeError, match="Failed to get revenue by client"):
            service.get_revenue_by_client()
    
    def test_get_total_revenue_empty_data(self):
        """Test total revenue with no data."""
        mock_querier = Mock(spec=Querier)
        mock_df = pd.DataFrame([{'total_revenue': None}])
        mock_querier.execute_sql.return_value = mock_df
        
        service = RevenueAnalytics(mock_querier)
        total_revenue = service.get_total_revenue()
        
        assert total_revenue == Decimal('0')


class TestOrderAnalytics:
    """Tests for OrderAnalytics service."""
    
    def test_get_client_order_summary_with_real_data(self, ctcontext):
        """Test order summary with real test data."""
        service = OrderAnalytics(ctcontext.querier)
        
        summaries = service.get_client_order_summary()
        
        # Should have 2 clients
        assert len(summaries) == 2
        
        # Results should be sorted by revenue (desc), so Alice should be first
        assert summaries[0].client_name == 'Alice'
        assert summaries[1].client_name == 'Bob'
        
        # Check Alice's summary
        alice_summary = summaries[0]
        assert alice_summary.total_orders == 1
        assert alice_summary.total_items == 2  # 1 laptop + 1 mouse
        assert alice_summary.total_revenue == Decimal('125.00')
        assert alice_summary.avg_order_value == Decimal('125.00')
        
        # Check Bob's summary
        bob_summary = summaries[1]
        assert bob_summary.total_orders == 1
        assert bob_summary.total_items == 1  # 1 mouse
        assert bob_summary.total_revenue == Decimal('25.00')
        assert bob_summary.avg_order_value == Decimal('25.00')
        
        # Verify data types
        assert isinstance(alice_summary, OrderSummary)
        assert isinstance(alice_summary.total_revenue, Decimal)
    
    def test_get_order_count_with_real_data(self, ctcontext, sample_data):
        """Test order count with real test data."""
        service = OrderAnalytics(ctcontext.querier)
        
        order_count = service.get_order_count()
        
        assert order_count == sample_data['total_orders']
        assert isinstance(order_count, int)
    
    def test_get_client_order_summary_with_mock(self):
        """Test order summary with mocked data."""
        mock_querier = Mock(spec=Querier)
        mock_df = pd.DataFrame([
            {
                'client_name': 'Test Client',
                'total_orders': 2,
                'total_items': 5,
                'total_revenue': 200.00
            }
        ])
        mock_querier.execute_sql.return_value = mock_df
        
        service = OrderAnalytics(mock_querier)
        summaries = service.get_client_order_summary()
        
        # Verify results
        assert len(summaries) == 1
        summary = summaries[0]
        assert summary.client_name == 'Test Client'
        assert summary.total_orders == 2
        assert summary.total_items == 5
        assert summary.total_revenue == Decimal('200.00')
        assert summary.avg_order_value == Decimal('100.00')  # 200 / 2
    
    def test_avg_order_value_calculation_with_zero_orders(self):
        """Test average order value calculation when no orders exist."""
        mock_querier = Mock(spec=Querier)
        mock_df = pd.DataFrame([
            {
                'client_name': 'No Orders Client',
                'total_orders': 0,
                'total_items': 0,
                'total_revenue': 0.00
            }
        ])
        mock_querier.execute_sql.return_value = mock_df
        
        service = OrderAnalytics(mock_querier)
        summaries = service.get_client_order_summary()
        
        summary = summaries[0]
        assert summary.avg_order_value == Decimal('0')


class TestProductAnalytics:
    """Tests for ProductAnalytics service."""
    
    def test_get_top_selling_products_with_real_data(self, ctcontext):
        """Test top selling products with real test data."""
        service = ProductAnalytics(ctcontext.querier)
        
        top_products = service.get_top_selling_products(limit=2)
        
        # Should have products sorted by quantity sold
        assert len(top_products) <= 2  # Might have fewer than 2 products
        
        # Mouse should be the top seller (2 units sold)
        mouse_product = next((p for p in top_products if p.product_name == 'Mouse'), None)
        assert mouse_product is not None
        assert mouse_product.total_quantity_sold == 2
        assert mouse_product.total_revenue == Decimal('50.00')  # 2 * 25
        assert mouse_product.orders_count == 2
        
        # Verify data types
        assert isinstance(mouse_product, ProductSales)
        assert isinstance(mouse_product.total_revenue, Decimal)
    
    def test_get_product_performance_with_real_data(self, ctcontext):
        """Test product performance with real test data."""
        service = ProductAnalytics(ctcontext.querier)
        
        performance = service.get_product_performance()
        
        # Should have only products that have sales (Keyboard has no sales)
        assert len(performance) == 2  # Only Laptop and Mouse have sales
        
        # Laptop should be highest revenue (100.00)
        laptop_product = next((p for p in performance if p.product_name == 'Laptop'), None)
        assert laptop_product is not None
        assert laptop_product.total_revenue == Decimal('100.00')
        assert laptop_product.total_quantity_sold == 1
        assert laptop_product.orders_count == 1
    
    def test_get_top_selling_products_with_mock(self):
        """Test top selling products with mocked data."""
        mock_querier = Mock(spec=Querier)
        mock_df = pd.DataFrame([
            {
                'product_name': 'Top Product',
                'total_quantity_sold': 10,
                'orders_count': 5,
                'total_revenue': 500.00
            }
        ])
        mock_querier.execute_sql.return_value = mock_df
        
        service = ProductAnalytics(mock_querier)
        products = service.get_top_selling_products(limit=1)
        
        # Verify service called querier correctly
        mock_querier.execute_sql.assert_called_once()
        sql_call = mock_querier.execute_sql.call_args[0][0]
        assert 'LIMIT 1' in sql_call
        
        # Verify results
        assert len(products) == 1
        product = products[0]
        assert product.product_name == 'Top Product'
        assert product.total_quantity_sold == 10
        assert product.orders_count == 5
        assert product.total_revenue == Decimal('500.00')
    
    def test_get_product_performance_sql_error(self):
        """Test error handling when SQL fails."""
        mock_querier = Mock(spec=Querier)
        mock_querier.execute_sql.side_effect = Exception("SQL execution failed")
        
        service = ProductAnalytics(mock_querier)
        
        with pytest.raises(RuntimeError, match="Failed to get product performance"):
            service.get_product_performance()


class TestCrossDatabaseConsistency:
    """Tests to verify consistent results across DuckDB and Snowflake."""
    
    @pytest.mark.parametrize("use_duckdb", [True, False])
    def test_revenue_consistency_across_databases(self, use_duckdb):
        """Test that revenue calculations are consistent between DuckDB and Snowflake."""
        # This test would require both databases to be available
        # For now, we'll skip if Snowflake is not configured
        pytest.skip("Cross-database testing requires both DuckDB and Snowflake setup")
    
    def test_data_type_consistency(self, ctcontext):
        """Test that all services return expected data types."""
        revenue_service = RevenueAnalytics(ctcontext.querier)
        order_service = OrderAnalytics(ctcontext.querier)
        product_service = ProductAnalytics(ctcontext.querier)
        
        # Test revenue service types
        revenues = revenue_service.get_revenue_by_client()
        for revenue in revenues:
            assert isinstance(revenue.client_name, str)
            assert isinstance(revenue.total_revenue, Decimal)
        
        # Test order service types
        summaries = order_service.get_client_order_summary()
        for summary in summaries:
            assert isinstance(summary.client_name, str)
            assert isinstance(summary.total_orders, int)
            assert isinstance(summary.total_items, int)
            assert isinstance(summary.total_revenue, Decimal)
            assert isinstance(summary.avg_order_value, Decimal)
        
        # Test product service types
        products = product_service.get_top_selling_products()
        for product in products:
            assert isinstance(product.product_name, str)
            assert isinstance(product.total_quantity_sold, int)
            assert isinstance(product.total_revenue, Decimal)
            assert isinstance(product.orders_count, int)


# Integration test that demonstrates the complete workflow
class TestBusinessLogicIntegration:
    """Integration tests demonstrating complete business logic workflows."""
    
    def test_complete_analytics_workflow(self, ctcontext, sample_data):
        """Test a complete analytics workflow using all services."""
        # Initialize all services
        revenue_service = RevenueAnalytics(ctcontext.querier)
        order_service = OrderAnalytics(ctcontext.querier)
        product_service = ProductAnalytics(ctcontext.querier)
        
        # Get comprehensive analytics
        client_revenues = revenue_service.get_revenue_by_client()
        total_revenue = revenue_service.get_total_revenue()
        order_summaries = order_service.get_client_order_summary()
        order_count = order_service.get_order_count()
        top_products = product_service.get_top_selling_products()
        
        # Verify data consistency across services
        # Total revenue should match sum of client revenues
        calculated_total = sum(r.total_revenue for r in client_revenues)
        assert total_revenue == calculated_total
        
        # Order summaries should match revenue data
        for revenue in client_revenues:
            matching_summary = next(
                (s for s in order_summaries if s.client_name == revenue.client_name),
                None
            )
            assert matching_summary is not None
            assert matching_summary.total_revenue == revenue.total_revenue
        
        # Verify expected counts match sample data
        assert len(client_revenues) == sample_data['total_clients']
        assert order_count == sample_data['total_orders']
        
        # Print analytics summary (for demonstration)
        print(f"\n=== Business Analytics Summary ===")
        print(f"Total Revenue: ${total_revenue}")
        print(f"Total Orders: {order_count}")
        print(f"Client Count: {len(client_revenues)}")
        print(f"Top Product: {top_products[0].product_name if top_products else 'None'}")
        
        # Demonstrate the power of the service layer
        assert total_revenue > Decimal('0')
        assert order_count > 0
        assert len(top_products) > 0