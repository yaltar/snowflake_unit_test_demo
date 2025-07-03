# SQL Testing Demo Project

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

A demonstration project showing how to test SQL business logic across multiple database engines using a unified approach.

## 🎯 Overview

This project demonstrates how to:
- 🧪 Test SQL business logic locally using **DuckDB** for fast, offline development
- ❄️ Run the same tests against **Snowflake** for production parity
- 🔄 Use **SQLGlot** for automatic SQL dialect transpilation
- 📊 Maintain a single test suite that works across different database engines

## 🏗️ Architecture

The project uses a layered architecture:

- **Models Layer** (`src/db/models.py`): SQLAlchemy models defining the database schema
- **Querier Layer** (`src/db/querier.py`): Unified interface for executing SQL across different engines
- **Services Layer** (`src/services/`): Business logic services with SQL analytics
- **Legacy Layer** (`src/legacy/`): Legacy code examples for migration demos
- **Utils Layer** (`src/utils/`): Logging and utility functions
- **Test Layer** (`tests/`): Pytest-based test suite with fixtures and test data
- **Scripts** (`scripts/`): Deployment, model generation, and demo scripts

## 📦 Business Domain

Simple e-commerce schema with:

### Tables
- `clients(id, name, email)` - Customer information
- `products(id, name, price)` - Product catalog  
- `orders(id, client_id, date)` - Customer orders
- `order_lines(id, order_id, product_id, quantity)` - Order line items

### Business Logic Examples
- **Revenue Analytics**: Revenue calculation per client, total revenue, top clients by month
- **Product Analytics**: Top-selling products, performance metrics, customer preferences
- **Order Analytics**: Order summaries, weekly patterns, client order analysis
- **Advanced Analytics**: Cohort analysis, retention rates, complex window functions
- **Legacy Migration**: Examples of migrating from legacy Snowflake-coupled code

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Poetry (for dependency management)

### Installation

1. **Clone and setup the project:**
   ```bash
   git clone <repository-url>
   cd sql-testing-demo
   ```

2. **Install dependencies:**
   ```bash
   poetry install --extras=full
   
   ```

3. **Configure environment (optional):**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

### Running Tests

#### Local Testing (DuckDB)
```bash
# Run all tests with DuckDB (default)
poetry run pytest tests/

# Run with verbose output
poetry run pytest tests/ -v

# Run specific test file
poetry run pytest tests/test_revenue_by_client.py
```

#### Production Testing (Snowflake)
```bash
# Configure Snowflake credentials in .env file
export USE_SNOWFLAKE=1
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_DATABASE=your_database
export SNOWFLAKE_SCHEMA=your_schema
export SNOWFLAKE_WAREHOUSE=your_warehouse

# Run tests against Snowflake
USE_SNOWFLAKE=1 poetry run pytest tests/
```

## 📁 Project Structure

```
sql-testing-demo/
├── src/
│   ├── db/
│   │   ├── models.py                    # SQLAlchemy models
│   │   ├── querier.py                   # Database querier classes
│   │   ├── local_db.py                  # DuckDB setup and data loading
│   │   ├── metadata_adapter.py          # Database metadata adaptation
│   │   └── snowflake_test_manager.py    # Snowflake test environment manager
│   ├── services/
│   │   └── analytics_service.py         # Business analytics services
│   ├── legacy/
│   │   └── legacy_analytics.py          # Legacy code examples
│   ├── utils/
│   │   └── logging.py                   # Logging utilities
│   └── logic/
│       └── business_logic.sql           # Business SQL (Snowflake dialect)
├── tests/
│   ├── conftest.py                      # Pytest fixtures and configuration
│   ├── test_revenue_by_client.py        # Basic revenue tests
│   ├── test_business_scenarios.py       # Complex business scenario tests
│   ├── test_snowflake_features.py       # Snowflake-specific feature tests
│   ├── services/
│   │   └── test_analytics_service.py    # Service layer tests
│   ├── legacy/
│   │   └── test_legacy_analytics.py     # Legacy code tests
│   └── data/                            # Test data in JSON format
│       ├── clients.json
│       ├── products.json
│       ├── orders.json
│       └── order_lines.json
├── scripts/
│   ├── generate_models.py               # SQLAlchemy model generation
│   ├── deploy_sql_ddl.py               # SQL DDL deployment
│   └── demo_workflow.py                # Demo workflow script
├── sql_migrations/
│   ├── 000_create_schema.sql           # Schema creation
│   ├── 001_initial_schema.sql          # Initial schema
│   ├── 002_add_product_category.sql    # Schema evolution
│   └── rollback/                       # Rollback scripts
├── presentation/                       # Presentation materials
│   ├── slides_v2.md                   # Main presentation slides
│   ├── SPEAKER_NOTES_V2.md            # Speaker notes
│   └── FRAMEWORK_FLOW_DIAGRAM.md      # Technical diagrams
├── pyproject.toml                      # Poetry configuration
├── MIGRATION_GUIDE.md                  # Migration guide
└── README.md
```

## 🧪 Writing Tests

### Service-Based Test Structure

```python
def test_revenue_analytics(ctcontext):
    # Use business service
    service = RevenueAnalytics(ctcontext.querier)
    revenues = service.get_revenue_by_client()
    
    # Verify business logic
    assert len(revenues) > 0
    assert all(r.total_revenue > 0 for r in revenues)
    assert isinstance(revenues[0], ClientRevenue)
```

### Direct SQL Testing

```python
def test_my_business_logic(ctcontext, sample_data):
    # Execute SQL query
    sql = """
    SELECT 
        c.name as client_name,
        SUM(p.price * ol.quantity) as total_revenue
    FROM clients c
    JOIN orders o ON c.id = o.client_id
    JOIN order_lines ol ON o.id = ol.order_id
    JOIN products p ON ol.product_id = p.id
    GROUP BY c.name
    """
    result_df = ctcontext.querier.execute_sql(sql)
    
    # Verify results
    assert len(result_df) > 0
    assert result_df['total_revenue'].sum() > 0
```

### Snowflake-Specific Features

```python
def test_snowflake_listagg(ctcontext):
    # Test LISTAGG function (transpiled to STRING_AGG in DuckDB)
    service = ProductAnalytics(ctcontext.querier)
    preferences = service.get_customer_product_preferences()
    
    assert len(preferences) > 0
    assert 'purchased_products' in preferences[0]
```

### Engine-Specific Tests

```python
@pytest.mark.duckdb
def test_duckdb_only_feature(duckdb_querier):
    # This test only runs with DuckDB
    pass

@pytest.mark.snowflake  
def test_snowflake_only_feature(ctcontext):
    # This test only runs with Snowflake
    if not ctcontext.use_snowflake:
        pytest.skip("Requires Snowflake")
    pass
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `USE_SNOWFLAKE` | Use Snowflake backend if "1" | "0" |
| `DUCKDB_PATH` | DuckDB database file path | ":memory:" |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | - |
| `SNOWFLAKE_USER` | Snowflake username | - |
| `SNOWFLAKE_PASSWORD` | Snowflake password | - |
| `SNOWFLAKE_DATABASE` | Snowflake database name | - |
| `SNOWFLAKE_SCHEMA` | Snowflake schema name | - |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | - |

### Test Data

Test data is stored in JSON files under `tests/data/`:
- Easy to read and modify
- Version controlled with code
- Supports complex relationships
- Allows for predictable test assertions

Example `clients.json`:
```json
[
    {
        "id": 1,
        "name": "Alice",
        "email": "alice@example.com"
    }
]
```

## 🔄 SQL Transpilation

The project uses [SQLGlot](https://github.com/tobymao/sqlglot) to automatically transpile SQL between dialects:

```python
# Business logic written in Snowflake dialect
sql_snowflake = """
SELECT 
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as orders
FROM orders 
GROUP BY DATE_TRUNC('month', order_date)
"""

# Automatically transpiled to DuckDB dialect when needed
sql_duckdb = transpile(sql_snowflake, read='snowflake', write='duckdb')[0]
```

## 🧩 Key Components

### CTContext Fixture

The `ctcontext` fixture provides a unified testing interface:

```python
@pytest.fixture
def ctcontext(use_snowflake):
    """Main test fixture providing database querier and setup."""
    context = CTContext(use_snowflake=use_snowflake)
    yield context
    context.reset_database()  # Cleanup
```

### Querier Classes

- **`Querier`**: Abstract base class defining the interface
- **`DuckDBQuerier`**: Local testing with SQLGlot transpilation and custom rules
- **`SnowflakeQuerier`**: Production testing with native SQL

### Key Components

**LocalDBManager**: Handles DuckDB setup and test data loading
- Creates tables from SQLAlchemy models
- Loads JSON test data
- Manages database lifecycle

**MetadataAdapter**: Adapts database metadata between engines
- Handles type conversions
- Manages dialect-specific differences

**SnowflakeTestManager**: Manages Snowflake test environments
- Database creation and cleanup
- Test data loading
- Connection management

**Analytics Services**: Business logic encapsulation
- `RevenueAnalytics`: Revenue calculations and client analysis
- `OrderAnalytics`: Order summaries and patterns
- `ProductAnalytics`: Product performance and preferences
- `AdvancedAnalytics`: Complex cohort analysis and retention

## 🎨 Best Practices

### SQL Development
1. Write business logic in **Snowflake dialect** (production target)
2. Use **meaningful table aliases** for readability
3. **Comment complex logic** in SQL files
4. **Group related queries** in the same file

### Test Development  
1. Use **descriptive test names** that explain the business rule
2. **Verify specific business outcomes**, not just "query runs"
3. Use **sample_data fixture** for expected values
4. **Test edge cases** and boundary conditions

### Data Management
1. Keep test data **minimal but realistic**
2. Use **consistent IDs** across related tables
3. **Document expected results** in test comments
4. **Version control** test data changes

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure tests pass on both engines
5. Submit a pull request

## 📚 Additional Resources

- [SQLGlot Documentation](https://github.com/tobymao/sqlglot)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Pytest Documentation](https://docs.pytest.org/)

## 🐛 Troubleshooting

### Common Issues

**SQL Transpilation Errors:**
- Check SQL syntax compatibility between dialects
- Use SQLGlot's online transpiler to debug
- Simplify complex expressions if needed

**Test Data Issues:**
- Verify JSON file formatting
- Check foreign key relationships
- Ensure data consistency

**Connection Issues:**
- Verify environment variables
- Check network connectivity for Snowflake
- Validate credentials

### Debug Mode

Run tests with debug output:
```bash
poetry run pytest tests/ -v -s --tb=short
```

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 🔧 Advanced Features

### Model Generation
The project includes scripts for automatic SQLAlchemy model generation:
```bash
# Generate models from existing Snowflake schema
python scripts/generate_models.py
```

### SQL Migrations
Manage schema evolution with SQL migrations:
```bash
# Deploy schema changes
python scripts/deploy_sql_ddl.py
```

### Legacy Migration
Examples of migrating from legacy Snowflake-coupled code:
- `src/legacy/legacy_analytics.py`: Legacy implementation
- `src/services/analytics_service.py`: Modern service-based approach
- `tests/legacy/`: Tests demonstrating migration

## 🚀 Next Steps

This demo can be extended with:
- **CI/CD integration**: Automated testing pipeline
- **Performance benchmarking**: Compare DuckDB vs Snowflake performance
- **Data quality tests**: Validate data integrity
- **Schema evolution**: Automated schema synchronization
- **More analytics**: Advanced business intelligence features

Happy testing! 🎉