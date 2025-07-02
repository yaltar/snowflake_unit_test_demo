# Migration Guide: From Legacy Snowflake Code to Unified Testing Framework

## 🎯 Overview

This guide demonstrates how to migrate from legacy Snowflake-specific analytics code to our modern unified testing framework. We'll transform tightly-coupled, hard-to-test code into clean, testable, cross-database business logic.

## 📋 Migration Checklist

- [ ] **Step 1**: Analyze Legacy Code Structure
- [ ] **Step 2**: Extract Business Logic to Service Classes  
- [ ] **Step 3**: Replace Direct Snowflake Connections with Querier
- [ ] **Step 4**: Migrate Tests to Framework Fixtures
- [ ] **Step 5**: Add Cross-Database Testing Support
- [ ] **Step 6**: Verify Transpilation and Edge Cases

---

## 🔍 Step 1: Analyze Legacy Code Structure

### **Before Migration**: Legacy Code Problems

Let's examine our legacy code (`src/legacy/legacy_analytics.py`):

```python
class LegacySnowflakeAnalytics:
    def __init__(self):
        # ❌ PROBLEM: Direct Snowflake connection
        self.connection = snowflake.connector.connect(...)
    
    def get_customer_insights(self):
        # ❌ PROBLEM: Embedded SQL strings
        sql = """
        WITH customer_orders AS (
            SELECT c.name, SUM(p.price * ol.quantity) AS order_value
            FROM clients c JOIN orders o ON c.id = o.client_id
            -- ... complex embedded SQL
        )
        SELECT co.customer_name, SUM(co.order_value) AS total_revenue
        FROM customer_orders co
        """
        
        # ❌ PROBLEM: Manual cursor management
        cursor = self.connection.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
```

### **Problems Identified**:
1. **Tightly Coupled**: Direct Snowflake connection prevents local testing
2. **Embedded SQL**: Hard to maintain and test SQL strings
3. **Manual Resource Management**: Error-prone cursor handling
4. **No Abstraction**: Business logic mixed with database code
5. **Testing Challenges**: Requires live Snowflake for every test

---

## 🔧 Step 2: Extract Business Logic to Service Classes

### **Migration Action**: Create Modern Service Class

Create `src/services/customer_analytics.py`:

```python
from src.services.analytics_service import RevenueAnalytics
from db.querier import Querier

class CustomerAnalytics:
    """
    ✅ SOLUTION: Clean service class with dependency injection
    """
    
    def __init__(self, querier: Querier):
        # ✅ SOLUTION: Dependency injection instead of direct connection
        self.querier = querier
    
    def get_customer_insights(self) -> List[CustomerInsight]:
        """
        ✅ SOLUTION: Clean method with documented SQL
        """
        # ✅ SOLUTION: Well-structured SQL with comments
        sql = """
        -- Customer insights with order aggregation
        WITH customer_orders AS (
            SELECT
                c.name AS customer_name,
                o.id AS order_id,
                SUM(p.price * ol.quantity) AS order_value
            FROM clients c
            JOIN orders o ON c.id = o.client_id
            JOIN order_lines ol ON o.id = ol.order_id
            JOIN products p ON ol.product_id = p.id
            GROUP BY c.name, o.id
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
        
        # ✅ SOLUTION: Framework handles execution and transpilation
        result_df = self.querier.execute_sql(sql)
        
        # ✅ SOLUTION: Clean data conversion to business objects
        return [
            CustomerInsight(
                customer_name=row['customer_name'],
                total_revenue=Decimal(str(row['total_revenue'])),
                order_count=int(row['order_count']),
                avg_order_value=Decimal(str(row['avg_order_value'])),
                favorite_products=row['favorite_products']
            )
            for _, row in result_df.iterrows()
        ]
```

### **Benefits Achieved**:
- ✅ **Dependency Injection**: Testable with any database backend
- ✅ **Clean SQL**: Well-documented, maintainable queries
- ✅ **Automatic Resource Management**: Framework handles connections
- ✅ **Type Safety**: Strong typing with data classes
- ✅ **Transpilation Support**: Same SQL works on DuckDB and Snowflake

---

## 🔌 Step 3: Replace Direct Snowflake Connections with Querier

### **Migration Action**: Update Service Instantiation

**Before** (Legacy Pattern):
```python
# ❌ PROBLEM: Direct instantiation with hidden dependencies
analytics = LegacySnowflakeAnalytics()  # Creates Snowflake connection internally
insights = analytics.get_customer_insights()
```

**After** (Framework Pattern):
```python
# ✅ SOLUTION: Explicit dependency injection
from src.services.customer_analytics import CustomerAnalytics

def create_customer_analytics(querier: Querier) -> CustomerAnalytics:
    """Factory function for dependency injection."""
    return CustomerAnalytics(querier)

# Usage with framework
customer_service = create_customer_analytics(querier)
insights = customer_service.get_customer_insights()
```

### **Benefits Achieved**:
- ✅ **Testability**: Can inject mock queriers for unit testing
- ✅ **Flexibility**: Works with any database backend
- ✅ **Clarity**: Dependencies are explicit and visible
- ✅ **Maintainability**: Easy to change database connections

---

## 🧪 Step 4: Migrate Tests to Framework Fixtures

### **Migration Action**: Replace Manual Test Setup

**Before** (Legacy Test Pattern):
```python
class TestLegacyAnalytics:
    @classmethod
    def setup_class(cls):
        # ❌ PROBLEM: Manual Snowflake connection
        cls.connection = snowflake.connector.connect(...)
        cls._setup_test_data()  # Manual test data insertion
    
    def test_customer_insights(self):
        # ❌ PROBLEM: Requires live Snowflake connection
        analytics = LegacySnowflakeAnalytics()
        insights = analytics.get_customer_insights()
        
        # ❌ PROBLEM: Hardcoded assertions based on manual test data
        assert insights[0].total_revenue == Decimal('325.00')
```

**After** (Framework Test Pattern):
```python
class TestCustomerAnalytics:
    """
    ✅ SOLUTION: Clean tests using framework fixtures
    """
    
    def test_customer_insights(self, ctcontext, sample_data):
        # ✅ SOLUTION: Framework provides setup automatically
        service = CustomerAnalytics(ctcontext.querier)
        insights = service.get_customer_insights()
        
        # ✅ SOLUTION: Flexible assertions using sample_data fixture
        assert len(insights) == sample_data['total_clients']
        
        # ✅ SOLUTION: Business logic validation
        for insight in insights:
            assert insight.total_revenue > Decimal('0')
            assert insight.order_count > 0
            assert len(insight.favorite_products) > 0
    
    def test_customer_insights_business_logic(self, ctcontext):
        # ✅ SOLUTION: Test business logic, not hardcoded data
        service = CustomerAnalytics(ctcontext.querier)
        insights = service.get_customer_insights()
        
        # ✅ SOLUTION: Test business rules
        total_revenue = sum(i.total_revenue for i in insights)
        assert total_revenue > Decimal('0')
        
        # ✅ SOLUTION: Test data consistency
        for insight in insights:
            expected_avg = insight.total_revenue / insight.order_count
            assert abs(insight.avg_order_value - expected_avg) < Decimal('0.01')
```

### **Benefits Achieved**:
- ✅ **Fast Tests**: Runs on DuckDB locally (milliseconds vs seconds)
- ✅ **Automatic Setup**: Framework handles database setup/teardown
- ✅ **Test Isolation**: Each test gets clean database state
- ✅ **Flexible Assertions**: Tests business logic, not hardcoded data
- ✅ **CI/CD Friendly**: No external dependencies required

---

## 🔄 Step 5: Add Cross-Database Testing Support

### **Migration Action**: Enable Multi-Database Testing

**Test Execution Options**:

```bash
# ✅ SOLUTION: Local development (fast)
poetry run pytest tests/test_customer_analytics.py

# ✅ SOLUTION: Production validation (comprehensive)  
USE_SNOWFLAKE=1 poetry run pytest tests/test_customer_analytics.py

# ✅ SOLUTION: Both databases in CI/CD
poetry run pytest tests/test_customer_analytics.py  # DuckDB
if [ "$SNOWFLAKE_AVAILABLE" = "1" ]; then
    USE_SNOWFLAKE=1 poetry run pytest tests/test_customer_analytics.py  # Snowflake
fi
```

**Framework Benefits**:
```python
def test_cross_database_compatibility(self, ctcontext):
    """
    ✅ SOLUTION: Same test runs on multiple databases
    """
    service = CustomerAnalytics(ctcontext.querier)
    
    # This test runs identically on DuckDB and Snowflake
    insights = service.get_customer_insights()
    
    # Business logic validation works regardless of backend
    assert len(insights) > 0
    total_revenue = sum(i.total_revenue for i in insights)
    assert total_revenue > Decimal('0')
    
    # Framework handles SQL transpilation automatically
    # LISTAGG(DISTINCT ...) WITHIN GROUP → STRING_AGG(DISTINCT ...)
```

### **Benefits Achieved**:
- ✅ **Development Speed**: Local DuckDB testing (10x+ faster)
- ✅ **Production Confidence**: Same logic tested on Snowflake
- ✅ **Automatic Transpilation**: Framework handles SQL differences
- ✅ **Flexible Deployment**: Choose database per environment

---

## 🔍 Step 6: Verify Transpilation and Edge Cases

### **Migration Action**: Test Advanced SQL Features

**Advanced Feature Testing**:
```python
def test_listagg_transpilation(self, ctcontext):
    """
    ✅ SOLUTION: Framework handles Snowflake-specific SQL
    """
    service = CustomerAnalytics(ctcontext.querier)
    insights = service.get_customer_insights()
    
    # Verify LISTAGG functionality works on both databases
    for insight in insights:
        # Should have comma-separated product list
        if ',' in insight.favorite_products:
            products = insight.favorite_products.split(', ')
            assert len(products) > 1
            assert all(len(p.strip()) > 0 for p in products)

def test_date_functions_transpilation(self, ctcontext):
    """
    ✅ SOLUTION: Framework handles date function differences
    """
    service = CustomerAnalytics(ctcontext.querier)
    trends = service.get_monthly_sales_trend()
    
    # DATE_TRUNC functionality works on both databases
    for trend in trends:
        assert trend['sales_month'] is not None
        assert trend['monthly_revenue'] > Decimal('0')
```

### **Benefits Achieved**:
- ✅ **Automatic Handling**: Framework transpiles complex SQL
- ✅ **Edge Case Coverage**: Tests work with advanced features
- ✅ **Consistent Results**: Same business logic, different SQL dialects
- ✅ **Future-Proof**: Easy to add more transpilation rules

---

## 🎯 Live Demo Script

### **Demo Flow for Presentation**:

1. **Show Legacy Problems** (5 minutes):
   ```bash
   # Show legacy code structure
   cat src/legacy/legacy_analytics.py | head -50
   
   # Highlight problems: embedded SQL, direct connections, manual management
   ```

2. **Step-by-Step Migration** (15 minutes):
   ```bash
   # Step 1: Create service class
   cp src/legacy/legacy_analytics.py src/services/customer_analytics.py
   
   # Step 2: Replace connection with querier injection
   # [Live editing to show transformation]
   
   # Step 3: Extract and clean SQL
   # [Live editing to show SQL improvements]
   
   # Step 4: Create modern tests
   cp tests/legacy/test_legacy_analytics.py tests/test_customer_analytics.py
   
   # Step 5: Replace manual setup with fixtures
   # [Live editing to show test transformation]
   ```

3. **Demonstrate Benefits** (10 minutes):
   ```bash
   # Fast local testing
   time poetry run pytest tests/test_customer_analytics.py
   
   # Same tests on Snowflake
   time USE_SNOWFLAKE=1 poetry run pytest tests/test_customer_analytics.py
   
   # Show transpilation working
   # [Show SQL differences between databases]
   ```

### **Key Points to Emphasize**:
- 🚀 **10x+ faster development** with local DuckDB testing
- 🔄 **Same business logic** works on multiple databases
- 🧪 **Better test coverage** with framework fixtures
- 🔧 **Easier maintenance** with clean service architecture
- 🎯 **Production confidence** with cross-database validation

---

## 📊 Migration Benefits Summary

| Aspect | Legacy Approach | Framework Approach | Improvement |
|--------|----------------|-------------------|-------------|
| **Test Speed** | 5-10 seconds/test | 50-100ms/test | **50-100x faster** |
| **Local Development** | Requires Snowflake | Works offline | **No external deps** |
| **Test Maintenance** | Manual setup/teardown | Automatic fixtures | **90% less boilerplate** |
| **Cross-Database** | Snowflake only | DuckDB + Snowflake | **Multi-backend support** |
| **SQL Portability** | Locked to Snowflake | Automatic transpilation | **Database agnostic** |
| **Code Quality** | Tightly coupled | Clean architecture | **SOLID principles** |
| **CI/CD Integration** | Requires Snowflake access | Runs anywhere | **Infrastructure independent** |

---

## 🚀 Next Steps After Migration

1. **Extend Test Coverage**: Add more edge cases and business scenarios
2. **Add More Backends**: PostgreSQL, BigQuery, etc.
3. **Performance Testing**: Benchmark queries across databases  
4. **Advanced Features**: Add more transpilation rules as needed
5. **Team Training**: Onboard team to new development workflow

---

## 💡 Migration Tips

### **Common Pitfalls to Avoid**:
- ❌ Don't migrate all code at once - do it incrementally
- ❌ Don't forget to test edge cases after migration
- ❌ Don't ignore SQL differences between databases
- ❌ Don't skip updating documentation and team processes

### **Success Strategies**:
- ✅ Start with simplest analytics functions first
- ✅ Keep legacy code running during migration
- ✅ Test thoroughly on both DuckDB and Snowflake
- ✅ Document any custom transpilation rules needed
- ✅ Train team on new development workflow

---

**🎉 Congratulations! You've successfully migrated to the unified testing framework!**

Your code is now faster to develop, easier to test, more maintainable, and works across multiple database backends.