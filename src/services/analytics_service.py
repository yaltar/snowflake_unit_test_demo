"""
Analytics services for e-commerce business logic.

This module provides service classes that encapsulate business analytics
using SQL queries through the Querier interface. Each service focuses on
a specific domain of business analytics.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from decimal import Decimal

import pandas as pd

# Handle imports for both package and standalone usage
try:
    from ..db.querier import Querier
except ImportError:
    # Fallback for direct execution or when not run as package
    import sys
    from pathlib import Path
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))
    from db.querier import Querier


@dataclass
class ClientRevenue:
    """Data class for client revenue information."""
    client_name: str
    total_revenue: Decimal
    
    def __post_init__(self):
        """Ensure total_revenue is a Decimal."""
        if not isinstance(self.total_revenue, Decimal):
            self.total_revenue = Decimal(str(self.total_revenue))


@dataclass
class ProductSales:
    """Data class for product sales information."""
    product_name: str
    total_quantity_sold: int
    total_revenue: Decimal
    orders_count: int
    
    def __post_init__(self):
        """Ensure numeric types are correct."""
        if not isinstance(self.total_revenue, Decimal):
            self.total_revenue = Decimal(str(self.total_revenue))
        self.total_quantity_sold = int(self.total_quantity_sold)
        self.orders_count = int(self.orders_count)


@dataclass
class OrderSummary:
    """Data class for order summary information."""
    client_name: str
    total_orders: int
    total_items: int
    total_revenue: Decimal
    avg_order_value: Decimal
    
    def __post_init__(self):
        """Ensure numeric types are correct."""
        if not isinstance(self.total_revenue, Decimal):
            self.total_revenue = Decimal(str(self.total_revenue))
        if not isinstance(self.avg_order_value, Decimal):
            self.avg_order_value = Decimal(str(self.avg_order_value))
        self.total_orders = int(self.total_orders)
        self.total_items = int(self.total_items)


class RevenueAnalytics:
    """Service for revenue-related analytics."""
    
    def __init__(self, querier: Querier):
        """Initialize with a querier for database operations.
        
        Args:
            querier: Database querier instance (DuckDB, Snowflake, etc.)
        """
        self.querier = querier
    
    def get_revenue_by_client(self) -> List[ClientRevenue]:
        """
        Get total revenue for each client.
        
        Returns:
            List of ClientRevenue objects sorted by client name
            
        Raises:
            RuntimeError: If SQL query fails
        """
        sql = """
        SELECT
            c.name AS client_name,
            SUM(p.price * ol.quantity) AS total_revenue
        FROM clients c
        JOIN orders o ON o.client_id = c.id
        JOIN order_lines ol ON ol.order_id = o.id
        JOIN products p ON p.id = ol.product_id
        GROUP BY c.name
        ORDER BY c.name
        """
        
        try:
            result_df = self.querier.execute_sql(sql)
            
            return [
                ClientRevenue(
                    client_name=row['client_name'],
                    total_revenue=Decimal(str(row['total_revenue']))
                )
                for _, row in result_df.iterrows()
            ]
        except Exception as e:
            raise RuntimeError(f"Failed to get revenue by client: {e}")
    
    def get_total_revenue(self) -> Decimal:
        """
        Get total revenue across all clients.
        
        Returns:
            Total revenue as Decimal
            
        Raises:
            RuntimeError: If SQL query fails
        """
        sql = """
        SELECT SUM(p.price * ol.quantity) AS total_revenue
        FROM clients c
        JOIN orders o ON o.client_id = c.id
        JOIN order_lines ol ON ol.order_id = o.id
        JOIN products p ON p.id = ol.product_id
        """
        
        try:
            result_df = self.querier.execute_sql(sql)
            total = result_df['total_revenue'].iloc[0]
            return Decimal(str(total)) if total is not None else Decimal('0')
        except Exception as e:
            raise RuntimeError(f"Failed to get total revenue: {e}")
    
    def get_top_revenue_clients_by_month(self) -> List[Dict[str, Any]]:
        """
        Get the top revenue client for each month using Snowflake's QUALIFY clause.
        
        This method demonstrates Snowflake-specific SQL features:
        - QUALIFY clause for filtering window function results
        - Advanced window functions with PARTITION BY
        - DATE_TRUNC for month aggregation
        
        Returns:
            List of dictionaries with month, client_name, and monthly_revenue
            
        Raises:
            RuntimeError: If SQL query fails
        """
        # Snowflake-specific SQL using QUALIFY clause
        sql = """
        SELECT 
            DATE_TRUNC('month', o.date) AS month,
            c.name AS client_name,
            SUM(p.price * ol.quantity) AS monthly_revenue
        FROM clients c
        JOIN orders o ON c.id = o.client_id  
        JOIN order_lines ol ON o.id = ol.order_id
        JOIN products p ON ol.product_id = p.id
        GROUP BY DATE_TRUNC('month', o.date), c.name
        QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', o.date) ORDER BY SUM(p.price * ol.quantity) DESC) = 1
        ORDER BY month DESC
        """
        
        try:
            result_df = self.querier.execute_sql(sql)
            
            return [
                {
                    'month': row['month'],
                    'client_name': row['client_name'],
                    'monthly_revenue': Decimal(str(row['monthly_revenue']))
                }
                for _, row in result_df.iterrows()
            ]
        except Exception as e:
            raise RuntimeError(f"Failed to get top revenue clients by month: {e}")


class OrderAnalytics:
    """Service for order-related analytics."""
    
    def __init__(self, querier: Querier):
        """Initialize with a querier for database operations.
        
        Args:
            querier: Database querier instance (DuckDB, Snowflake, etc.)
        """
        self.querier = querier
    
    def get_client_order_summary(self) -> List[OrderSummary]:
        """
        Get comprehensive order summary for each client.
        
        Returns:
            List of OrderSummary objects sorted by total revenue (desc)
            
        Raises:
            RuntimeError: If SQL query fails
        """
        sql = """
        SELECT
            c.name AS client_name,
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
        
        try:
            result_df = self.querier.execute_sql(sql)
            
            summaries = []
            for _, row in result_df.iterrows():
                total_revenue = Decimal(str(row['total_revenue']))
                total_orders = int(row['total_orders'])
                avg_order_value = total_revenue / total_orders if total_orders > 0 else Decimal('0')
                
                summaries.append(OrderSummary(
                    client_name=row['client_name'],
                    total_orders=total_orders,
                    total_items=int(row['total_items']),
                    total_revenue=total_revenue,
                    avg_order_value=avg_order_value
                ))
            
            return summaries
        except Exception as e:
            raise RuntimeError(f"Failed to get client order summary: {e}")
    
    def get_order_count(self) -> int:
        """
        Get total number of orders in the system.
        
        Returns:
            Total number of orders
            
        Raises:
            RuntimeError: If SQL query fails
        """
        sql = "SELECT COUNT(*) AS order_count FROM orders"
        
        try:
            result_df = self.querier.execute_sql(sql)
            return int(result_df['order_count'].iloc[0])
        except Exception as e:
            raise RuntimeError(f"Failed to get order count: {e}")
    
    def get_weekly_order_patterns(self) -> List[Dict[str, Any]]:
        """
        Get weekly order patterns using Snowflake-specific date functions.
        
        This method demonstrates Snowflake-specific SQL features:
        - LAST_DAY() function with 'week' parameter
        - DAYOFWEEK() function for day-of-week analysis  
        - WEEK() function for week numbering
        - Advanced date manipulation specific to Snowflake
        
        Returns:
            List of dictionaries with week_ending, day_of_week, orders_count, revenue
            
        Raises:
            RuntimeError: If SQL query fails
        """
        # Snowflake-specific SQL using advanced date functions
        sql = """
        SELECT
            LAST_DAY(o.date, 'week') AS week_ending,
            DAYOFWEEK(o.date) AS day_of_week,
            COUNT(DISTINCT o.id) AS orders_count,
            SUM(p.price * ol.quantity) AS total_revenue,
            WEEK(o.date) AS week_number
        FROM orders o
        JOIN order_lines ol ON o.id = ol.order_id
        JOIN products p ON ol.product_id = p.id
        GROUP BY LAST_DAY(o.date, 'week'), DAYOFWEEK(o.date), WEEK(o.date)
        ORDER BY week_ending DESC, day_of_week
        """
        
        try:
            result_df = self.querier.execute_sql(sql)
            
            return [
                {
                    'week_ending': row['week_ending'],
                    'day_of_week': int(row['day_of_week']),
                    'orders_count': int(row['orders_count']),
                    'total_revenue': Decimal(str(row['total_revenue'])),
                    'week_number': int(row['week_number'])
                }
                for _, row in result_df.iterrows()
            ]
        except Exception as e:
            raise RuntimeError(f"Failed to get weekly order patterns: {e}")


class ProductAnalytics:
    """Service for product-related analytics."""
    
    def __init__(self, querier: Querier):
        """Initialize with a querier for database operations.
        
        Args:
            querier: Database querier instance (DuckDB, Snowflake, etc.)
        """
        self.querier = querier
    
    def get_top_selling_products(self, limit: int = 3) -> List[ProductSales]:
        """
        Get top-selling products by quantity sold.
        
        Args:
            limit: Maximum number of products to return (default: 3)
            
        Returns:
            List of ProductSales objects sorted by quantity sold (desc)
            
        Raises:
            RuntimeError: If SQL query fails
        """
        sql = f"""
        SELECT
            p.name AS product_name,
            SUM(ol.quantity) AS total_quantity_sold,
            COUNT(DISTINCT ol.order_id) AS orders_count,
            SUM(p.price * ol.quantity) AS total_revenue
        FROM products p
        JOIN order_lines ol ON ol.product_id = p.id
        GROUP BY p.id, p.name
        ORDER BY total_quantity_sold DESC
        LIMIT {limit}
        """
        
        try:
            result_df = self.querier.execute_sql(sql)
            
            return [
                ProductSales(
                    product_name=row['product_name'],
                    total_quantity_sold=int(row['total_quantity_sold']),
                    total_revenue=Decimal(str(row['total_revenue'])),
                    orders_count=int(row['orders_count'])
                )
                for _, row in result_df.iterrows()
            ]
        except Exception as e:
            raise RuntimeError(f"Failed to get top selling products: {e}")
    
    def get_product_performance(self) -> List[ProductSales]:
        """
        Get performance metrics for all products.
        
        Returns:
            List of ProductSales objects sorted by total revenue (desc)
            
        Raises:
            RuntimeError: If SQL query fails
        """
        sql = """
        SELECT
            p.name AS product_name,
            SUM(ol.quantity) AS total_quantity_sold,
            COUNT(DISTINCT ol.order_id) AS orders_count,
            SUM(p.price * ol.quantity) AS total_revenue
        FROM products p
        JOIN order_lines ol ON ol.product_id = p.id
        GROUP BY p.id, p.name
        ORDER BY total_revenue DESC
        """
        
        try:
            result_df = self.querier.execute_sql(sql)
            
            return [
                ProductSales(
                    product_name=row['product_name'],
                    total_quantity_sold=int(row['total_quantity_sold']),
                    total_revenue=Decimal(str(row['total_revenue'])),
                    orders_count=int(row['orders_count'])
                )
                for _, row in result_df.iterrows()
            ]
        except Exception as e:
            raise RuntimeError(f"Failed to get product performance: {e}")
    
    def get_customer_product_preferences(self) -> List[Dict[str, Any]]:
        """
        Get customer product preferences using Snowflake's LISTAGG function.
        
        This method demonstrates Snowflake-specific SQL features:
        - LISTAGG() function with WITHIN GROUP clause
        - String aggregation functionality specific to Snowflake
        - Advanced grouping with comma-separated concatenation
        
        Returns:
            List of dictionaries with customer, purchased_products, unique_products
            
        Raises:
            RuntimeError: If SQL query fails
        """
        # Snowflake-specific SQL using LISTAGG
        sql = """
        SELECT
            c.name AS customer,
            LISTAGG(DISTINCT p.name, ', ') WITHIN GROUP (ORDER BY p.name) AS purchased_products,
            COUNT(DISTINCT p.id) AS unique_products,
            SUM(ol.quantity) AS total_items_purchased
        FROM clients c
        JOIN orders o ON c.id = o.client_id
        JOIN order_lines ol ON o.id = ol.order_id  
        JOIN products p ON ol.product_id = p.id
        GROUP BY c.name
        ORDER BY unique_products DESC, customer
        """
        
        try:
            result_df = self.querier.execute_sql(sql)
            
            return [
                {
                    'customer': row['customer'],
                    'purchased_products': row['purchased_products'],
                    'unique_products': int(row['unique_products']),
                    'total_items_purchased': int(row['total_items_purchased'])
                }
                for _, row in result_df.iterrows()
            ]
        except Exception as e:
            raise RuntimeError(f"Failed to get customer product preferences: {e}")


class AdvancedAnalytics:
    """Service for advanced analytics using cutting-edge Snowflake features."""
    
    def __init__(self, querier: Querier):
        """Initialize with a querier for database operations.
        
        Args:
            querier: Database querier instance (DuckDB, Snowflake, etc.)
        """
        self.querier = querier
    
    def get_cohort_analysis(self) -> List[Dict[str, Any]]:
        """
        Perform cohort analysis using advanced Snowflake window functions.
        
        This method demonstrates advanced Snowflake-specific SQL features:
        - Complex window functions with multiple OVER clauses
        - Advanced date calculations and cohort grouping
        - Sophisticated analytical SQL patterns
        
        Returns:
            List of dictionaries with cohort analysis data
            
        Raises:
            RuntimeError: If SQL query fails
        """
        # Advanced Snowflake SQL with complex window functions
        sql = """
        WITH customer_orders AS (
            SELECT
                c.id AS customer_id,
                c.name AS customer_name,
                o.date AS order_date,
                DATE_TRUNC('month', o.date) AS order_month,
                ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY o.date) AS order_sequence,
                FIRST_VALUE(DATE_TRUNC('month', o.date)) OVER (PARTITION BY c.id ORDER BY o.date) AS first_order_month,
                SUM(p.price * ol.quantity) AS order_value
            FROM clients c
            JOIN orders o ON c.id = o.client_id
            JOIN order_lines ol ON o.id = ol.order_id
            JOIN products p ON ol.product_id = p.id
            GROUP BY c.id, c.name, o.id, o.date
        ),
        cohort_data AS (
            SELECT
                first_order_month AS cohort_month,
                order_month,
                DATEDIFF('month', first_order_month, order_month) AS months_since_first_order,
                COUNT(DISTINCT customer_id) AS customers_in_period,
                SUM(order_value) AS total_revenue
            FROM customer_orders
            GROUP BY first_order_month, order_month
        )
        SELECT
            cohort_month,
            months_since_first_order,
            customers_in_period,
            total_revenue,
            LAG(customers_in_period) OVER (PARTITION BY cohort_month ORDER BY months_since_first_order) AS prev_period_customers,
            CASE 
                WHEN LAG(customers_in_period) OVER (PARTITION BY cohort_month ORDER BY months_since_first_order) > 0
                THEN customers_in_period::FLOAT / LAG(customers_in_period) OVER (PARTITION BY cohort_month ORDER BY months_since_first_order)
                ELSE 1.0
            END AS retention_rate
        FROM cohort_data
        ORDER BY cohort_month, months_since_first_order
        """
        
        try:
            result_df = self.querier.execute_sql(sql)
            
            return [
                {
                    'cohort_month': row['cohort_month'],
                    'months_since_first_order': int(row['months_since_first_order']),
                    'customers_in_period': int(row['customers_in_period']),
                    'total_revenue': Decimal(str(row['total_revenue'])),
                    'prev_period_customers': int(row['prev_period_customers']) if row['prev_period_customers'] is not None else None,
                    'retention_rate': float(row['retention_rate']) if row['retention_rate'] is not None else None
                }
                for _, row in result_df.iterrows()
            ]
        except Exception as e:
            raise RuntimeError(f"Failed to get cohort analysis: {e}")


# Factory functions for easy service creation
def create_revenue_analytics(querier: Querier) -> RevenueAnalytics:
    """Create a RevenueAnalytics service instance."""
    return RevenueAnalytics(querier)


def create_order_analytics(querier: Querier) -> OrderAnalytics:
    """Create an OrderAnalytics service instance."""
    return OrderAnalytics(querier)


def create_product_analytics(querier: Querier) -> ProductAnalytics:
    """Create a ProductAnalytics service instance."""
    return ProductAnalytics(querier)


def create_advanced_analytics(querier: Querier) -> AdvancedAnalytics:
    """Create an AdvancedAnalytics service instance."""
    return AdvancedAnalytics(querier)