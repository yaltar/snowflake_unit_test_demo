#!/usr/bin/env python3
"""
Deploy database schema to Snowflake using SQL DDL files.

This script executes SQL DDL migration files in order to deploy schema changes
to Snowflake databases. It tracks applied migrations in a schema_versions table
and supports rollback functionality.

This simulates a production workflow where teams deploy schema changes
directly to Snowflake using versioned DDL migrations.
"""

import os
import sys
import re
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_snowflake_engine():
    """Create Snowflake engine from environment variables."""
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    database = os.getenv('SNOWFLAKE_REF_DATABASE', 'TEST_REF')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'MY_SHOP')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    role = os.getenv('SNOWFLAKE_ROLE')
    
    if not all([account, user, password, warehouse]):
        raise ValueError(
            "Missing required Snowflake configuration. "
            "Please set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, "
            "and SNOWFLAKE_WAREHOUSE environment variables."
        )
    
    url = f"snowflake://{user}:{password}@{account}/{database}/{schema}"
    
    params = []
    if warehouse:
        params.append(f"warehouse={warehouse}")
    if role:
        params.append(f"role={role}")
    
    if params:
        url += "?" + "&".join(params)
    
    return create_engine(url)


def ensure_version_table(engine, schema_name):
    """Create schema_versions table if it doesn't exist."""
    with engine.connect() as conn:
        try:
            # Use the specified schema
            conn.execute(text(f"USE SCHEMA {schema_name}"))
            
            # Create version table with Snowflake-specific syntax
            version_table_sql = """
                CREATE TABLE IF NOT EXISTS schema_versions (
                    version_number VARCHAR(50) PRIMARY KEY,
                    migration_name VARCHAR(255) NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                    applied_by VARCHAR(100) DEFAULT CURRENT_USER()
                )
            """
            
            conn.execute(text(version_table_sql))
            conn.commit()
            return True
        except Exception:
            # Schema doesn't exist yet - this is fine, migrations will create it
            return False


def get_applied_versions(engine, schema_name):
    """Get list of already applied migration versions."""
    with engine.connect() as conn:
        try:
            # Ensure we're in the correct schema
            conn.execute(text(f"USE SCHEMA {schema_name}"))
            result = conn.execute(text("SELECT version_number FROM schema_versions ORDER BY version_number"))
            return [row[0] for row in result.fetchall()]
        except Exception:
            # Schema or table might not exist yet
            return []


def get_migration_files(migrations_dir):
    """Get list of migration files in order."""
    migration_files = []
    migrations_path = Path(migrations_dir)
    
    if not migrations_path.exists():
        print(f"❌ Migrations directory not found: {migrations_path}")
        return []
    
    # Find all .sql files and sort them by number
    for file_path in migrations_path.glob("*.sql"):
        # Extract version number from filename (e.g., "001" from "001_initial_schema.sql")
        match = re.match(r'^(\d+)_(.+)\.sql$', file_path.name)
        if match:
            version = match.group(1)
            name = match.group(2)
            migration_files.append({
                'version': version,
                'name': name,
                'filename': file_path.name,
                'path': file_path
            })
    
    # Sort by version number
    migration_files.sort(key=lambda x: int(x['version']))
    return migration_files


def execute_sql_file(engine, file_path, migration_name):
    """Execute a SQL migration file on Snowflake."""
    print(f"📄 Executing migration: {file_path.name}")
    
    try:
        with open(file_path, 'r') as f:
            sql_content = f.read()
        
        # Process SQL content to extract executable statements
        statements = []
        
        # Split by semicolons and process each potential statement
        for raw_stmt in sql_content.split(';'):
            # Remove comments and normalize whitespace
            lines = []
            for line in raw_stmt.split('\n'):
                line = line.strip()
                # Skip comment lines and empty lines
                if line and not line.startswith('--'):
                    lines.append(line)
            
            if lines:
                stmt = ' '.join(lines).strip()
                if stmt and stmt.upper() != 'COMMIT':
                    statements.append(stmt)
        
        with engine.connect() as conn:
            for statement in statements:
                print(f"   Executing: {statement[:60]}...")
                conn.execute(text(statement))
            
            # Snowflake auto-commits, but explicit commit for consistency
            conn.commit()
            
        print(f"✅ Migration {file_path.name} executed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error executing migration {file_path.name}: {e}")
        return False


def record_migration(engine, schema_name, version, migration_name):
    """Record a successful migration in the schema_versions table."""
    with engine.connect() as conn:
        # Ensure we're in the correct schema
        conn.execute(text(f"USE SCHEMA {schema_name}"))
        conn.execute(text("""
            INSERT INTO schema_versions (version_number, migration_name)
            VALUES (:version, :name)
        """), {'version': version, 'name': migration_name})
        conn.commit()


def deploy_migrations():
    """Deploy all pending SQL DDL migrations to Snowflake."""
    print("🚀 Starting Snowflake DDL Schema Deployment")
    print("=" * 70)
    
    # Get project paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    migrations_dir = project_root / "sql_migrations"
    
    print(f"📁 Project root: {project_root}")
    print(f"📁 Migrations directory: {migrations_dir}")
    
    try:
        # Connect to Snowflake
        engine = get_snowflake_engine()
        schema_name = os.getenv('SNOWFLAKE_SCHEMA', 'MY_SHOP')
        database_name = os.getenv('SNOWFLAKE_REF_DATABASE', 'TEST_REF')
        
        print(f"❄️  Snowflake connection established")
        print(f"📊 Target database: {database_name}")
        print(f"📋 Target schema: {schema_name}")
        
        # Try to ensure version tracking table exists
        version_table_ready = ensure_version_table(engine, schema_name)
        if version_table_ready:
            print("✅ Schema versions table ready")
        else:
            print("📋 Schema doesn't exist yet - will be created by migrations")
        
        # Get applied and available migrations
        applied_versions = get_applied_versions(engine, schema_name)
        migration_files = get_migration_files(migrations_dir)
        
        if not migration_files:
            print("⚠️  No migration files found")
            return True
        
        print(f"📊 Found {len(migration_files)} migration files")
        print(f"📊 Already applied: {len(applied_versions)} migrations")
        
        # Filter pending migrations
        pending_migrations = [m for m in migration_files if m['version'] not in applied_versions]
        
        if not pending_migrations:
            print("✅ No pending migrations. Snowflake schema is up to date!")
            return True
        
        print(f"🔄 Pending migrations: {len(pending_migrations)}")
        for migration in pending_migrations:
            print(f"   - {migration['filename']}")
        
        print("\n" + "=" * 70)
        print("⬆️  Deploying to Snowflake")
        print("=" * 70)
        
        # Apply pending migrations
        for migration in pending_migrations:
            print(f"\n🔄 Processing migration {migration['version']}: {migration['name']}")
            
            if execute_sql_file(engine, migration['path'], migration['name']):
                # If this was the schema creation migration and version table wasn't ready before
                if not version_table_ready and migration['version'] == '000':
                    print("📋 Setting up version tracking table after schema creation...")
                    ensure_version_table(engine, schema_name)
                    version_table_ready = True
                
                record_migration(engine, schema_name, migration['version'], migration['name'])
                print(f"✅ Migration {migration['version']} completed and recorded")
            else:
                print(f"❌ Migration {migration['version']} failed")
                return False
        
        print("\n" + "=" * 70)
        print("✅ Snowflake DDL deployment completed successfully!")
        print("=" * 70)
        
        print(f"❄️  Snowflake database ready: {database_name}.{schema_name}")
        print("📋 Schema deployed with:")
        print("   - Snowflake-native data types (DECIMAL, TIMESTAMP_NTZ, etc.)")
        print("   - Identity columns for auto-incrementing IDs")
        print("   - Version tracking in schema_versions table")
        print("   - Production-ready constraints and indexes")
        
        print("\n💡 Next steps:")
        print("   - Generate SQLAlchemy models: python scripts/generate_models.py")
        print("   - Run tests against Snowflake: USE_SNOWFLAKE=1 pytest")
        
        return True
        
    except Exception as e:
        print(f"❌ Snowflake deployment failed: {e}")
        return False


def rollback_migration(target_version=None):
    """Rollback migrations to a target version."""
    print(f"🔙 Starting Snowflake rollback to version {target_version}")
    print("=" * 70)
    
    # Get project paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    rollback_dir = project_root / "sql_migrations" / "rollback"
    
    try:
        engine = get_snowflake_engine()
        schema_name = os.getenv('SNOWFLAKE_SCHEMA', 'MY_SHOP')
        applied_versions = get_applied_versions(engine, schema_name)
        
        if not applied_versions:
            print("⚠️  No migrations to rollback")
            return True
        
        # Determine rollback target
        if target_version is None:
            # Rollback latest migration
            versions_to_rollback = [applied_versions[-1]]
        else:
            # Rollback to target version
            versions_to_rollback = [v for v in applied_versions if int(v) > int(target_version)]
            versions_to_rollback.reverse()  # Rollback in reverse order
        
        if not versions_to_rollback:
            print(f"⚠️  No migrations to rollback to version {target_version}")
            return True
        
        print(f"🔄 Rolling back {len(versions_to_rollback)} migrations from Snowflake:")
        for version in versions_to_rollback:
            print(f"   - Version {version}")
        
        # Execute rollback scripts
        for version in versions_to_rollback:
            rollback_files = list(rollback_dir.glob(f"{version}_*_rollback.sql"))
            
            if not rollback_files:
                print(f"❌ Rollback script not found for version {version}")
                return False
            
            rollback_path = rollback_files[0]
            print(f"\n🔄 Rolling back version {version}")
            
            if execute_sql_file(engine, rollback_path, f"rollback_{version}"):
                # Remove from version table
                with engine.connect() as conn:
                    conn.execute(text(f"USE SCHEMA {schema_name}"))
                    conn.execute(text(
                        "DELETE FROM schema_versions WHERE version_number = :version"
                    ), {'version': version})
                    conn.commit()
                print(f"✅ Version {version} rolled back successfully")
            else:
                print(f"❌ Rollback failed for version {version}")
                return False
        
        print("\n✅ Snowflake rollback completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Snowflake rollback failed: {e}")
        return False


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Deploy database schema to Snowflake using SQL DDL files',
        epilog="""
Examples:
  python scripts/deploy_sql_ddl.py                    # Deploy all pending migrations
  python scripts/deploy_sql_ddl.py --rollback         # Rollback latest migration
  python scripts/deploy_sql_ddl.py --rollback 001     # Rollback to version 001
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--rollback', type=str, metavar='VERSION', nargs='?', const='',
                       help='Rollback to specified version (or latest if no version specified)')
    
    args = parser.parse_args()
    
    try:
        if args.rollback is not None:
            # If --rollback with no value, rollback latest
            target_version = args.rollback if args.rollback else None
            success = rollback_migration(target_version)
        else:
            success = deploy_migrations()
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n🛑 Snowflake deployment interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error during Snowflake deployment: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())