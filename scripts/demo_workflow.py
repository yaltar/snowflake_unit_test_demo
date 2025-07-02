#!/usr/bin/env python3
"""
SQL DDL Schema Deployment and SQLAlchemy Model Generation Demo.

This script demonstrates the core schema management workflow:
1. Deploy SQL DDL migration files to Snowflake database
2. Generate SQLAlchemy models from the deployed database schema using sqlacodegen

This simulates a production workflow where schema changes are deployed via DDL
and SQLAlchemy models are automatically generated from the actual database structure.
"""

import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def print_banner(title, char="="):
    """Print a banner with title."""
    width = 70
    print(f"\n{char * width}")
    print(f"{title:^{width}}")
    print(f"{char * width}")

def print_step(step_num, title):
    """Print a step header."""
    print(f"\n🔸 Step {step_num}: {title}")
    print("-" * 50)

def run_script(script_name, description):
    """Run a Python script and return success status."""
    script_path = Path(__file__).parent / script_name
    
    if not script_path.exists():
        print(f"❌ Script not found: {script_path}")
        return False
    
    print(f"🚀 {description}")
    print(f"📝 Running: python {script_name}")
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            print("📋 Output:")
            print(result.stdout)
        
        if result.stderr:
            print("⚠️  Errors/Warnings:")
            print(result.stderr)
        
        if result.returncode == 0:
            print("✅ Script completed successfully!")
            return True
        else:
            print(f"❌ Script failed with return code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error running script: {e}")
        return False

def show_current_models():
    """Show the current content of models.py."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    models_path = project_root / "src" / "db" / "models.py"
    
    if models_path.exists():
        print(f"📄 Current models.py content:")
        print("-" * 40)
        
        with open(models_path, 'r') as f:
            content = f.read()
            # Show first 30 lines to keep output manageable
            lines = content.split('\n')
            for i, line in enumerate(lines[:30], 1):
                print(f"{i:3d}: {line}")
            
            if len(lines) > 30:
                print(f"... ({len(lines) - 30} more lines)")
        
        print("-" * 40)
    else:
        print("📄 No models.py file found")


def check_prerequisites():
    """Check if all prerequisites are met."""
    print("🔍 Checking prerequisites...")
    
    # Check environment variables
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_WAREHOUSE'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("💡 Please set these in your .env file or environment")
        return False
    
    # Check if SQL migrations directory exists
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    migrations_dir = project_root / "sql_migrations"
    
    if migrations_dir.exists():
        migration_files = list(migrations_dir.glob("*.sql"))
        print(f"✅ SQL migrations found ({len(migration_files)} files)")
    else:
        print("❌ SQL migrations directory not found")
        return False
    
    # Check if sqlacodegen is available
    try:
        subprocess.run(['sqlacodegen', '--help'], capture_output=True, check=True)
        print("✅ sqlacodegen found")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ sqlacodegen not found. Install with: pip install sqlacodegen")
        return False
    
    print("✅ All prerequisites met!")
    return True

def demo_workflow():
    """Run the complete demo workflow."""
    print_banner("🎭 SQL DDL Schema Deployment + Model Generation Demo", "🎯")
    
    print("""
This demo shows the core schema management workflow:

🔄 Workflow Overview:
   1. Show current SQLAlchemy models
   2. Deploy SQL DDL migration files to Snowflake database
   3. Generate updated SQLAlchemy models from deployed schema
   4. Show the updated models

🎯 This demonstrates a production workflow where:
   - Schema changes are defined in SQL DDL migration files
   - DDL migrations are deployed directly to Snowflake database
   - SQLAlchemy models are auto-generated from actual database schema
   - Models stay synchronized with the deployed database structure
    """)
    
    input("Press Enter to continue...")
    
    # Check prerequisites
    print_step(0, "Prerequisites Check")
    if not check_prerequisites():
        print("❌ Prerequisites not met. Please resolve the issues above.")
        return False
    
    # Step 1: Show current models
    print_step(1, "Current SQLAlchemy Models")
    show_current_models()
    
    input("\nPress Enter to deploy SQL DDL schema...")
    
    # Step 2: Deploy DDL schema
    print_step(2, "Deploy SQL DDL Schema to Snowflake")
    if not run_script("deploy_sql_ddl.py", "Deploying DDL schema migrations to Snowflake"):
        print("❌ DDL schema deployment failed")
        return False
    
    input("\nPress Enter to generate SQLAlchemy models...")
    
    # Step 3: Generate models from deployed schema
    print_step(3, "Generate SQLAlchemy Models from Database Schema")
    if not run_script("generate_models.py", "Generating SQLAlchemy models from Snowflake schema"):
        print("❌ Model generation failed")
        return False
    
    # Step 4: Show updated models
    print_step(4, "Updated SQLAlchemy Models")
    show_current_models()
    
    print_banner("✅ Demo Workflow Completed!", "🎉")
    
    print("""
🎯 Demo Summary:
   ✅ SQL DDL migrations deployed to Snowflake database
   ✅ SQLAlchemy models generated from actual database schema
   ✅ Models synchronized with deployed database structure

💡 Key Benefits Demonstrated:
   🔄 Automated model generation from database schema
   🎯 Single source of truth (deployed database schema)
   📝 Version-controlled schema management via SQL DDL migrations
   👨‍💻 Schema-first approach with automatic model synchronization
   ❄️ Direct Snowflake deployment workflow

🚀 Next Steps:
   - Make schema changes by adding new DDL migration files
   - Run this workflow again to update models
   - Use generated models in your application code
   - Integrate this workflow into your deployment pipeline
    """)
    
    return True

def main():
    """Main function."""
    try:
        success = demo_workflow()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error during demo: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())