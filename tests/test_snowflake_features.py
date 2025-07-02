"""
Tests for advanced SQL features in analytics services.

This module tests the advanced SQL features in the analytics services,
including window functions, date functions, and string aggregation.
"""

import pytest
from decimal import Decimal
from src.services.analytics_service import (
    RevenueAnalytics, OrderAnalytics, ProductAnalytics, AdvancedAnalytics
)


class TestAdvancedSQLFeatures:
    """Tests for advanced SQL features in analytics services."""
    
    def test_top_revenue_clients_by_month(self, ctcontext):
        """
        Test monthly top revenue client analysis.
        
        Tests the get_top_revenue_clients_by_month method which uses
        window functions to find the top revenue client for each month.
        """
        service = RevenueAnalytics(ctcontext.querier)
        results = service.get_top_revenue_clients_by_month()
        
        # Verify structure
        assert isinstance(results, list)
        
        if results:  # Only check if we have data
            result = results[0]
            assert 'month' in result
            assert 'client_name' in result
            assert 'monthly_revenue' in result
            assert isinstance(result['monthly_revenue'], Decimal)
            assert result['monthly_revenue'] > Decimal('0')
            assert isinstance(result['client_name'], str)
            assert len(result['client_name']) > 0
    
    def test_weekly_order_patterns(self, ctcontext):
        """
        Test weekly order pattern analysis.
        
        Tests the get_weekly_order_patterns method which analyzes
        order patterns by week and day of week using date functions.
        """
        service = OrderAnalytics(ctcontext.querier)
        results = service.get_weekly_order_patterns()
        
        # Verify structure
        assert isinstance(results, list)
        
        if results:  # Only check if we have data
            result = results[0]
            assert 'week_ending' in result
            assert 'day_of_week' in result
            assert 'orders_count' in result
            assert 'total_revenue' in result
            assert 'week_number' in result
            
            assert isinstance(result['day_of_week'], int)
            assert isinstance(result['orders_count'], int)
            assert isinstance(result['total_revenue'], Decimal)
            assert isinstance(result['week_number'], int)
            
            # Validate ranges
            assert 1 <= result['day_of_week'] <= 7  # Day of week should be 1-7
            assert result['orders_count'] >= 0
            assert result['total_revenue'] >= Decimal('0')
            assert result['week_number'] >= 1
    
    def test_customer_product_preferences(self, ctcontext):
        """
        Test customer product preference analysis.
        
        Tests the get_customer_product_preferences method which aggregates
        products purchased by each customer using string aggregation.
        """
        service = ProductAnalytics(ctcontext.querier)
        results = service.get_customer_product_preferences()
        
        # Verify structure
        assert isinstance(results, list)
        
        if results:  # Only check if we have data
            result = results[0]
            assert 'customer' in result
            assert 'purchased_products' in result
            assert 'unique_products' in result
            assert 'total_items_purchased' in result
            
            assert isinstance(result['customer'], str)
            assert isinstance(result['purchased_products'], str)
            assert isinstance(result['unique_products'], int)
            assert isinstance(result['total_items_purchased'], int)
            
            # Validate data ranges
            assert len(result['customer']) > 0
            assert len(result['purchased_products']) > 0
            assert result['unique_products'] > 0
            assert result['total_items_purchased'] > 0
            
            # If multiple products, should be comma-separated
            if result['unique_products'] > 1:
                assert ', ' in result['purchased_products']
    
    def test_cohort_analysis(self, ctcontext):
        """
        Test cohort analysis functionality.
        
        Tests the get_cohort_analysis method which performs customer
        cohort analysis using advanced window functions and CTEs.
        """
        service = AdvancedAnalytics(ctcontext.querier)
        results = service.get_cohort_analysis()
        
        # Verify structure
        assert isinstance(results, list)
        
        if results:  # Only check if we have data
            result = results[0]
            assert 'cohort_month' in result
            assert 'months_since_first_order' in result
            assert 'customers_in_period' in result
            assert 'total_revenue' in result
            assert 'retention_rate' in result
            
            assert isinstance(result['months_since_first_order'], int)
            assert isinstance(result['customers_in_period'], int)
            assert isinstance(result['total_revenue'], Decimal)
            
            # Validate ranges
            assert result['months_since_first_order'] >= 0
            assert result['customers_in_period'] > 0
            assert result['total_revenue'] > Decimal('0')
            
            if result['retention_rate'] is not None:
                assert isinstance(result['retention_rate'], float)
                assert 0 <= result['retention_rate'] <= 1.0


class TestAdvancedAnalyticsService:
    """Tests for the AdvancedAnalytics service class."""
    
    def test_advanced_analytics_service_creation(self, ctcontext):
        """Test that AdvancedAnalytics service can be created and used."""
        service = AdvancedAnalytics(ctcontext.querier)
        assert service is not None
        assert service.querier is not None
    
    def test_all_advanced_methods_return_lists(self, ctcontext):
        """Test that all advanced analytics methods return list structures."""
        service = AdvancedAnalytics(ctcontext.querier)
        
        # Test cohort analysis returns a list
        cohort_results = service.get_cohort_analysis()
        assert isinstance(cohort_results, list)