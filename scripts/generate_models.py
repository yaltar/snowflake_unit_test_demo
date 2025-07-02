#!/usr/bin/env python3
"""
Generate SQLAlchemy models from Snowflake TEST_REF database using sqlacodegen.

This script connects to the TEST_REF Snowflake database and generates
SQLAlchemy models based on the actual database schema, ensuring consistency
between the database structure and the Python models used for testing.
"""

import os
import sys
import tempfile
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_snowflake_connection_string():
    """Build Snowflake connection string from environment variables."""
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
    
    connection_string = f"snowflake://{user}:{password}@{account}/{database}/{schema}"
    
    params = []
    if warehouse:
        params.append(f"warehouse={warehouse}")
    if role:
        params.append(f"role={role}")
    
    if params:
        connection_string += "?" + "&".join(params)
    
    return connection_string

def backup_current_models(models_path):
    """Create a backup of the current models.py file."""
    if models_path.exists():
        backup_path = models_path.with_suffix('.py.backup')
        print(f"🔄 Backing up current models.py to {backup_path}")
        
        with open(models_path, 'r') as src, open(backup_path, 'w') as dst:
            dst.write(src.read())
        
        return backup_path
    return None

def generate_models_with_sqlacodegen(connection_string, output_path):
    """Use sqlacodegen to generate models from the database schema."""
    print(f"🔗 Connecting to Snowflake TEST_REF database...")
    print(f"📝 Generating models to {output_path}")
    
    # Prepare sqlacodegen command
    cmd = [
        'sqlacodegen',
        '--options', 'noconstraints,noindexes,nocomments',
        '--generator', 'declarative',
        '--outfile', str(output_path),
        connection_string
    ]
    
    try:
        # Run sqlacodegen
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        print("✅ Models generated successfully!")
        
        if result.stdout:
            print("📋 sqlacodegen output:")
            print(result.stdout)
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to generate models: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except FileNotFoundError:
        print("❌ sqlacodegen not found. Please install it with: pip install sqlacodegen")
        return False

def post_process_generated_models(models_path):
    """Post-process the generated models for better usability."""
    if not models_path.exists():
        print(f"❌ Generated models file not found: {models_path}")
        return False
    
    print("🔧 Post-processing generated models...")
    
    # Read the generated content
    with open(models_path, 'r') as f:
        content = f.read()
    
    # Add our custom header
    header = '''"""
SQLAlchemy models for the e-commerce demo schema.

This module defines the database models for:
- clients: Customer information
- products: Product catalog
- orders: Customer orders
- order_lines: Individual items within orders

NOTE: This file is auto-generated from the Snowflake TEST_REF database
using sqlacodegen. Manual changes may be overwritten.

For DuckDB compatibility, use the metadata_adapter module to convert
these Snowflake-specific models to DuckDB-compatible metadata.
"""

'''
    
    # Remove any existing module docstring if present
    lines = content.split('\n')
    if lines and (lines[0].startswith('"""') or lines[0].startswith("'''")):
        # Find the end of the existing docstring
        quote_type = lines[0][:3]
        end_idx = 1
        for i, line in enumerate(lines[1:], 1):
            if quote_type in line:
                end_idx = i + 1
                break
        content = '\n'.join(lines[end_idx:])
    
    final_content = header + content
    
    # Write the processed content
    with open(models_path, 'w') as f:
        f.write(final_content)
    
    print("✅ Post-processing completed!")
    return True

def show_differences(original_backup, new_models_path):
    """Show differences between the original and new models."""
    if not original_backup or not original_backup.exists():
        print("📄 No backup found - this is a new models.py file")
        return
    
    print("🔍 Comparing changes...")
    
    try:
        # Use diff command to show differences
        result = subprocess.run(
            ['diff', '-u', str(original_backup), str(new_models_path)],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print("✅ No differences found - models are identical")
        else:
            print("📋 Changes detected:")
            # Show first 20 lines of diff
            diff_lines = result.stdout.split('\n')[:20]
            print('\n'.join(diff_lines))
            if len(result.stdout.split('\n')) > 20:
                print("... (truncated)")
            
    except FileNotFoundError:
        print("⚠️  diff command not available - cannot show differences")

def main():
    """Main function to generate models from TEST_REF database."""
    print("🚀 Starting SQLAlchemy model generation from Snowflake TEST_REF database")
    print("=" * 70)
    
    # Get project paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    models_path = project_root / "src" / "db" / "models.py"
    
    print(f"📁 Project root: {project_root}")
    print(f"📁 Models file: {models_path}")
    
    try:
        # Get connection string
        connection_string = get_snowflake_connection_string()
        
        # Backup current models
        backup_path = backup_current_models(models_path)
        
        # Generate new models
        success = generate_models_with_sqlacodegen(connection_string, models_path)
        
        if success:
            # Post-process the generated models
            post_process_generated_models(models_path)
            
            # Show differences
            show_differences(backup_path, models_path)
            
            print("=" * 70)
            print("✅ Model generation completed successfully!")
            print(f"📄 New models saved to: {models_path}")
            if backup_path:
                print(f"💾 Backup saved to: {backup_path}")
            print("\n💡 For DuckDB testing, use metadata_adapter to convert these models")
        else:
            print("❌ Model generation failed!")
            return 1
            
    except Exception as e:
        print(f"❌ Error during model generation: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())