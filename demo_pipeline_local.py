"""
Automated pipeline runner for Orderly Data Platform.
This script runs the complete pipeline without human intervention,
assuming source files and seed CSV are already prepared.
"""

import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path='.env.example')

def run_command(cmd, desc=None, cwd=None):
    """Execute a command with error handling"""
    print(f"\n{'='*60}")
    if desc:
        print(f"Step: {desc}")
    print(f"Running: {cmd}")
    
    result = subprocess.run(cmd, shell=True, cwd=cwd)
    if result.returncode != 0:
        print(f"ERROR: Command failed: {cmd}")
        sys.exit(result.returncode)
    
    print(f"✅ Success: {desc or cmd}")

def check_prerequisites():
    """Check if required files and directories exist"""
    print("🔍 Checking prerequisites...")
    
    # Check if dbt directory exists
    dbt_dir = Path("dbt")
    if not dbt_dir.exists():
        print("❌ Error: dbt directory not found")
        print("Please ensure you're running this script from the project root directory.")
        sys.exit(1)
    
    # Check if dbt_project.yml exists
    dbt_project = Path("dbt/dbt_project.yml")
    if not dbt_project.exists():
        print("❌ Error: dbt_project.yml not found in dbt directory")
        sys.exit(1)
    
    # Check if source files exist
    source_dir = Path("data/source")
    if not source_dir.exists() or not any(source_dir.iterdir()):
        print("❌ Error: No source files found in data/source/")
        print("Please ensure your source data files are in place before running the automated pipeline.")
        sys.exit(1)
    
    # Check if seed file exists (assuming it's already curated)
    seed_file = Path("dbt/seeds/ref_sku_names.csv")
    if not seed_file.exists():
        print("❌ Error: SKU seed file not found at dbt/seeds/ref_sku_names.csv")
        print("Please ensure the curated SKU seed file is in place before running the automated pipeline.")
        sys.exit(1)
    
    # Check if warehouse directory exists and clean it
    warehouse_dir = Path("warehouse")
    if not warehouse_dir.exists():
        warehouse_dir.mkdir(parents=True, exist_ok=True)
        print("✅ Created warehouse directory")
    else:
        # Delete any existing .duckdb files
        duckdb_files = list(warehouse_dir.glob("*.duckdb"))
        if duckdb_files:
            for db_file in duckdb_files:
                try:
                    db_file.unlink()
                    print(f"🗑️  Deleted existing database: {db_file}")
                except Exception as e:
                    print(f"⚠️  Warning: Could not delete {db_file}: {e}")
        print("✅ Warehouse directory ready")
    
    print("✅ All prerequisites satisfied")

def main():
    """Run the complete automated pipeline"""
    print("🚀 Starting Orderly Automated Pipeline")
    print("=" * 60)
    
    # Check prerequisites
    check_prerequisites()
    
    try:
        # Step 1: Generate bronze models
        run_command(
            "dbt build --select bronze --project-dir ./dbt", 
            desc="Generate bronze models"
        )
        
        # Step 2: Generate stg_orders staging table
        run_command(
            "dbt build --select stg_orders --project-dir ./dbt", 
            desc="Generate stg_orders staging table"
        )
        
        # Step 3: Generate seed tables (skip SKU curation - assume ready)
        run_command(
            "dbt seed --project-dir ./dbt", 
            desc="Load seed tables"
        )
        
        # Step 4: Build remaining staging models
        run_command(
            "dbt build --select staging --exclude stg_orders --project-dir ./dbt", 
            desc="Build remaining staging models"
        )
        
        # Step 5: Generate silver models
        run_command(
            "dbt build --select silver --project-dir ./dbt", 
            desc="Generate silver models"
        )
        
        # Step 6: Generate gold models
        run_command(
            "dbt build --select gold --project-dir ./dbt", 
            desc="Generate gold models"
        )
        
        # Step 7: Run tests
        run_command(
            "dbt test --project-dir ./dbt", 
            desc="Run data quality tests"
        )
        
        print("\n" + "=" * 60)
        print("🎉 AUTOMATED PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("Your data warehouse is ready at: warehouse/orderly.duckdb")
        print("You can now run analytics using the notebook: notebooks/orderly_business_analytics.ipynb")
        print("To export data: make export-csv")
        print("To view documentation: make docs")
        
    except KeyboardInterrupt:
        print("\n⏸️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
