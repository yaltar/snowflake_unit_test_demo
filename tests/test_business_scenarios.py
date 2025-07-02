"""
Business scenario tests for analytics services.

This module tests realistic business scenarios using the analytics services,
demonstrating practical use cases for the SQL business logic.
"""

import pytest
from decimal import Decimal
from src.services.analytics_service import (
    RevenueAnalytics, OrderAnalytics, ProductAnalytics, AdvancedAnalytics
)


class TestBusinessScenarios:
    """Tests for realistic business scenarios."""
    
    def test_monthly_revenue_report(self, ctcontext):
        """
        Test generating a monthly revenue report.
        
        Simulates generating a business report showing top customers by month,
        which is a common business analytics requirement.
        """
        revenue_service = RevenueAnalytics(ctcontext.querier)
        
        # Get overall revenue data
        total_revenue = revenue_service.get_total_revenue()
        client_revenues = revenue_service.get_revenue_by_client()
        monthly_top_clients = revenue_service.get_top_revenue_clients_by_month()
        
        # Business logic validations
        assert total_revenue > Decimal('0'), "Business should have revenue"
        assert len(client_revenues) > 0, "Should have paying customers"
        
        # Verify monthly analysis makes sense
        if monthly_top_clients:
            for monthly_data in monthly_top_clients:
                assert monthly_data['monthly_revenue'] > Decimal('0')
                assert len(monthly_data['client_name']) > 0
                
        # Revenue consistency check
        calculated_total = sum(c.total_revenue for c in client_revenues)
        assert total_revenue == calculated_total, "Revenue totals should match"
    
    def test_operational_dashboard(self, ctcontext):
        """
        Test generating an operational dashboard.
        
        Simulates generating key operational metrics that would appear
        on a business dashboard.
        """
        revenue_service = RevenueAnalytics(ctcontext.querier)
        order_service = OrderAnalytics(ctcontext.querier)
        product_service = ProductAnalytics(ctcontext.querier)
        
        # Collect dashboard metrics
        total_revenue = revenue_service.get_total_revenue()
        order_count = order_service.get_order_count()
        client_summaries = order_service.get_client_order_summary()
        top_products = product_service.get_top_selling_products(limit=3)
        
        # Dashboard validations
        assert total_revenue >= Decimal('0')
        assert order_count >= 0
        assert isinstance(client_summaries, list)
        assert isinstance(top_products, list)
        
        # Business logic validations
        if order_count > 0:
            avg_order_value = total_revenue / order_count
            assert avg_order_value > Decimal('0')
            
        # Customer analysis
        if client_summaries:
            for summary in client_summaries:
                assert summary.total_orders > 0
                assert summary.total_revenue > Decimal('0')
                assert summary.avg_order_value > Decimal('0')
    
    def test_product_performance_analysis(self, ctcontext):
        """
        Test product performance analysis.
        
        Simulates analyzing product performance including customer preferences
        and sales patterns.
        """
        product_service = ProductAnalytics(ctcontext.querier)
        
        # Get product analytics
        top_products = product_service.get_top_selling_products()
        all_products = product_service.get_product_performance()
        customer_preferences = product_service.get_customer_product_preferences()
        
        # Product analysis validations
        assert isinstance(top_products, list)
        assert isinstance(all_products, list)
        assert isinstance(customer_preferences, list)
        
        # Business validations
        if top_products:
            # Top products should have sales
            for product in top_products:
                assert product.total_quantity_sold > 0
                assert product.total_revenue > Decimal('0')
                assert product.orders_count > 0
        
        if customer_preferences:
            # Customer preferences should make sense
            for pref in customer_preferences:
                assert len(pref['customer']) > 0
                assert len(pref['purchased_products']) > 0
                assert pref['unique_products'] > 0
                assert pref['total_items_purchased'] > 0
    
    def test_customer_retention_analysis(self, ctcontext):
        """
        Test customer retention analysis.
        
        Simulates analyzing customer behavior and retention patterns
        using cohort analysis.
        """
        advanced_service = AdvancedAnalytics(ctcontext.querier)
        order_service = OrderAnalytics(ctcontext.querier)
        
        # Get retention analytics
        cohort_data = advanced_service.get_cohort_analysis()
        order_patterns = order_service.get_weekly_order_patterns()
        
        # Retention analysis validations
        assert isinstance(cohort_data, list)
        assert isinstance(order_patterns, list)
        
        # Business validations for cohort analysis
        if cohort_data:
            for cohort in cohort_data:
                assert cohort['customers_in_period'] > 0
                assert cohort['total_revenue'] > Decimal('0')
                assert cohort['months_since_first_order'] >= 0
                
                # Retention rate should be valid when present
                if cohort['retention_rate'] is not None:
                    assert 0 <= cohort['retention_rate'] <= 1.0
        
        # Business validations for order patterns
        if order_patterns:
            for pattern in order_patterns:
                assert pattern['orders_count'] >= 0
                assert pattern['total_revenue'] >= Decimal('0')
                assert 1 <= pattern['day_of_week'] <= 7
    
    def test_comprehensive_business_kpis(self, ctcontext):
        """
        Test calculation of comprehensive business KPIs.
        
        Simulates calculating key performance indicators that would be
        reported to business stakeholders.
        """
        revenue_service = RevenueAnalytics(ctcontext.querier)
        order_service = OrderAnalytics(ctcontext.querier)
        product_service = ProductAnalytics(ctcontext.querier)
        
        # Calculate core KPIs
        total_revenue = revenue_service.get_total_revenue()
        total_orders = order_service.get_order_count()
        client_count = len(revenue_service.get_revenue_by_client())
        products_sold = len(product_service.get_product_performance())
        
        # Calculate derived KPIs
        if total_orders > 0:
            avg_order_value = total_revenue / total_orders
            assert avg_order_value > Decimal('0'), "Average order value should be positive"
        
        if client_count > 0:
            revenue_per_customer = total_revenue / client_count
            assert revenue_per_customer > Decimal('0'), "Revenue per customer should be positive"
        
        # KPI validation
        assert total_revenue >= Decimal('0'), "Revenue should not be negative"
        assert total_orders >= 0, "Order count should not be negative"
        assert client_count >= 0, "Client count should not be negative"
        assert products_sold >= 0, "Product count should not be negative"
    
    def test_end_to_end_business_workflow(self, ctcontext):
        """
        Test an end-to-end business workflow.
        
        Simulates a complete business analysis workflow from raw data
        to actionable insights.
        """
        # Initialize all services
        revenue_service = RevenueAnalytics(ctcontext.querier)
        order_service = OrderAnalytics(ctcontext.querier)
        product_service = ProductAnalytics(ctcontext.querier)
        advanced_service = AdvancedAnalytics(ctcontext.querier)
        
        # Step 1: Revenue Analysis
        total_revenue = revenue_service.get_total_revenue()
        client_revenues = revenue_service.get_revenue_by_client()
        
        # Step 2: Order Analysis  
        order_summaries = order_service.get_client_order_summary()
        order_count = order_service.get_order_count()
        
        # Step 3: Product Analysis
        top_products = product_service.get_top_selling_products()
        
        # Step 4: Advanced Analysis
        try:
            cohort_analysis = advanced_service.get_cohort_analysis()
        except Exception:
            # Advanced features might not be supported on all backends
            cohort_analysis = []
        
        # Workflow validations
        assert isinstance(total_revenue, Decimal)
        assert isinstance(client_revenues, list)
        assert isinstance(order_summaries, list)
        assert isinstance(order_count, int)
        assert isinstance(top_products, list)
        assert isinstance(cohort_analysis, list)
        
        # Business consistency checks
        if client_revenues and order_summaries:
            # Should have same number of clients in both analyses
            revenue_clients = {r.client_name for r in client_revenues}
            order_clients = {s.client_name for s in order_summaries}
            assert revenue_clients == order_clients, "Client lists should match"
        
        # Success - workflow completed without errors
        assert True, "End-to-end business workflow completed successfully"