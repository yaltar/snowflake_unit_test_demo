"""
Legacy Analytics Module - BEFORE MIGRATION

This module demonstrates the "old way" of doing analytics with direct Snowflake connections.
This code has several problems:
- Tightly coupled to Snowflake
- No cross-database testing capability  
- Manual connection management
- Embedded SQL strings
- No transpilation support
- Difficult to test locally

This will be migrated to the modern querier-based framework.
"""

import os
import snowflake.connector
from typing import List, Dict, Any, Optional
from decimal import Decimal
import pandas as pd
from dataclasses import dataclass


@dataclass
class LegacyCustomerInsight:
    """Legacy data class for customer insights."""
    customer_name: str
    total_revenue: Decimal
    order_count: int
    avg_order_value: Decimal
    favorite_products: str


class LegacySnowflakeAnalytics:
    """
    Legacy analytics class that uses Snowflake connector directly.
    
    Problems with this approach:
    1. Tightly coupled to Snowflake - cannot test locally
    2. Manual connection management - error prone
    3. Embedded SQL - hard to maintain
    4. No transpilation - locked into Snowflake syntax
    5. Difficult to unit test - requires live Snowflake connection
    """
    
    def __init__(self):
        """Initialize with direct Snowflake connection."""
        self.connection = None
        self._connect_to_snowflake()
    
    def _connect_to_snowflake(self):
        """Create direct Snowflake connection - manual and error-prone."""
        try:
            self.connection = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                database=os.getenv('SNOWFLAKE_REF_DATABASE', 'TEST_REF'),
                schema=os.getenv('SNOWFLAKE_SCHEMA', 'MY_SHOP'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                role=os.getenv('SNOWFLAKE_ROLE')
            )
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Snowflake: {e}")
    
    def get_customer_insights(self) -> List[LegacyCustomerInsight]:
        """
        Get comprehensive customer insights using embedded Snowflake SQL.
        
        Problems with this method:
        - Long embedded SQL string
        - Snowflake-specific syntax (LISTAGG, WITHIN GROUP)
        - Manual cursor management
        - No transpilation support
        - Error-prone data conversion
        """
        # Embedded SQL - hard to maintain and test
        sql = """
        WITH customer_orders AS (
            SELECT
                c.name AS customer_name,
                o.id AS order_id,
                o.date AS order_date,
                SUM(p.price * ol.quantity) AS order_value
            FROM clients c
            JOIN orders o ON c.id = o.client_id
            JOIN order_lines ol ON o.id = ol.order_id
            JOIN products p ON ol.product_id = p.id
            GROUP BY c.name, o.id, o.date
        ),
        customer_products AS (
            SELECT
                c.name AS customer_name,
                LISTAGG(DISTINCT p.name, ', ') WITHIN GROUP (ORDER BY p.name) AS favorite_products
            FROM clients c
            JOIN orders o ON c.id = o.client_id
            JOIN order_lines ol ON o.id = ol.order_id
            JOIN products p ON ol.product_id = p.id
            GROUP BY c.name
        )
        SELECT
            co.customer_name,
            SUM(co.order_value) AS total_revenue,
            COUNT(co.order_id) AS order_count,
            AVG(co.order_value) AS avg_order_value,
            cp.favorite_products
        FROM customer_orders co
        JOIN customer_products cp ON co.customer_name = cp.customer_name
        GROUP BY co.customer_name, cp.favorite_products
        ORDER BY total_revenue DESC
        """
        
        # Manual cursor management - error prone
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # Manual data conversion - tedious and error-prone
            insights = []
            for row in results:
                data = dict(zip(columns, row))
                insight = LegacyCustomerInsight(
                    customer_name=data['CUSTOMER_NAME'],
                    total_revenue=Decimal(str(data['TOTAL_REVENUE'])),
                    order_count=int(data['ORDER_COUNT']),
                    avg_order_value=Decimal(str(data['AVG_ORDER_VALUE'])),
                    favorite_products=data['FAVORITE_PRODUCTS']
                )
                insights.append(insight)
            
            return insights
            
        finally:
            cursor.close()
    
    def get_monthly_sales_trend(self) -> List[Dict[str, Any]]:
        """
        Get monthly sales trends using Snowflake-specific date functions.
        
        Problems:
        - Uses Snowflake-specific DATE_TRUNC
        - Manual pandas DataFrame creation
        - No error handling for SQL execution
        - Tightly coupled to Snowflake date syntax
        """
        # More embedded SQL with Snowflake-specific functions
        sql = """
        SELECT
            DATE_TRUNC('month', o.date) AS sales_month,
            COUNT(DISTINCT o.id) AS total_orders,
            COUNT(DISTINCT o.client_id) AS unique_customers,
            SUM(p.price * ol.quantity) AS monthly_revenue,
            AVG(p.price * ol.quantity) AS avg_order_value
        FROM orders o
        JOIN order_lines ol ON o.id = ol.order_id
        JOIN products p ON ol.product_id = p.id
        GROUP BY DATE_TRUNC('month', o.date)
        ORDER BY sales_month DESC
        """
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # Manual DataFrame construction
            data = []
            for row in results:
                row_data = dict(zip(columns, row))
                # Manual type conversion
                row_data['MONTHLY_REVENUE'] = Decimal(str(row_data['MONTHLY_REVENUE']))
                row_data['AVG_ORDER_VALUE'] = Decimal(str(row_data['AVG_ORDER_VALUE']))
                data.append(row_data)
            
            return data
            
        finally:
            cursor.close()
    
    def analyze_product_performance(self) -> pd.DataFrame:
        """
        Analyze product performance with complex Snowflake SQL.
        
        Problems:
        - Very complex embedded SQL
        - Uses multiple Snowflake-specific features
        - Manual pandas construction
        - No abstraction or reusability
        """
        # Complex embedded SQL with Snowflake features
        sql = """
        WITH product_stats AS (
            SELECT
                p.id,
                p.name,
                p.price,
                SUM(ol.quantity) AS total_sold,
                COUNT(DISTINCT ol.order_id) AS orders_count,
                COUNT(DISTINCT o.client_id) AS unique_customers,
                SUM(p.price * ol.quantity) AS total_revenue,
                RANK() OVER (ORDER BY SUM(ol.quantity) DESC) AS sales_rank,
                RANK() OVER (ORDER BY SUM(p.price * ol.quantity) DESC) AS revenue_rank
            FROM products p
            JOIN order_lines ol ON p.id = ol.product_id
            JOIN orders o ON ol.order_id = o.id
            GROUP BY p.id, p.name, p.price
        ),
        customer_favorites AS (
            SELECT
                p.id,
                COUNT(DISTINCT o.client_id) AS customer_count,
                LISTAGG(DISTINCT c.name, ', ') WITHIN GROUP (ORDER BY c.name) AS customer_list
            FROM products p
            JOIN order_lines ol ON p.id = ol.product_id
            JOIN orders o ON ol.order_id = o.id
            JOIN clients c ON o.client_id = c.id
            GROUP BY p.id
        )
        SELECT
            ps.name,
            ps.price,
            ps.total_sold,
            ps.orders_count,
            ps.unique_customers,
            ps.total_revenue,
            ps.sales_rank,
            ps.revenue_rank,
            cf.customer_list,
            CASE 
                WHEN ps.sales_rank <= 3 THEN 'Top Seller'
                WHEN ps.revenue_rank <= 3 THEN 'High Value'
                ELSE 'Standard'
            END AS product_category
        FROM product_stats ps
        JOIN customer_favorites cf ON ps.id = cf.id
        ORDER BY ps.total_revenue DESC
        """
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            
            # Convert to pandas manually
            df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
            
            # Manual type conversions
            df['TOTAL_REVENUE'] = df['TOTAL_REVENUE'].apply(lambda x: Decimal(str(x)))
            df['PRICE'] = df['PRICE'].apply(lambda x: Decimal(str(x)))
            
            return df
            
        finally:
            cursor.close()
    
    def __del__(self):
        """Cleanup connection - manual resource management."""
        if self.connection:
            self.connection.close()


# Example usage function showing the legacy pattern
def generate_legacy_business_report():
    """
    Generate a comprehensive business report using legacy approach.
    
    Problems demonstrated:
    - Manual connection management
    - Multiple service instantiations
    - No dependency injection
    - Tightly coupled to Snowflake
    - Difficult to test or mock
    """
    analytics = LegacySnowflakeAnalytics()
    
    print("=== LEGACY BUSINESS REPORT ===")
    
    # Customer insights
    insights = analytics.get_customer_insights()
    print(f"Customer Insights: {len(insights)} customers analyzed")
    for insight in insights:
        print(f"  {insight.customer_name}: ${insight.total_revenue} revenue, {insight.order_count} orders")
    
    # Sales trends
    trends = analytics.get_monthly_sales_trend()
    print(f"Sales Trends: {len(trends)} months analyzed")
    
    # Product performance
    products_df = analytics.analyze_product_performance()
    print(f"Product Performance: {len(products_df)} products analyzed")
    
    print("=== END LEGACY REPORT ===")
    
    return {
        'customer_insights': insights,
        'sales_trends': trends,
        'product_performance': products_df
    }