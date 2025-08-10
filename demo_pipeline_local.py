"""
Automated pipeline runner for Orderly Data Platform.
This script runs the complete pipeline without human intervention,
assuming source files and seed CSV are already prepared.
"""

import subprocess
import sys
import shutil
from pathlib import Path
from dotenv import load_dotenv
import datetime

load_dotenv(dotenv_path='.env.example')

def run_command(cmd, desc=None, cwd=None, capture_output=False):
    """Execute a command with error handling"""
    print(f"\n{'='*60}")
    if desc:
        print(f"Step: {desc}")
    print(f"Running: {cmd}")
    
    if capture_output:
        result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"ERROR: Command failed: {cmd}")
            print(f"STDERR: {result.stderr}")
            sys.exit(result.returncode)
        print(f"✅ Success: {desc or cmd}")
        return result.stdout
    else:
        result = subprocess.run(cmd, shell=True, cwd=cwd)
        if result.returncode != 0:
            print(f"ERROR: Command failed: {cmd}")
            sys.exit(result.returncode)
        print(f"✅ Success: {desc or cmd}")
        return None

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
    
    # Initialize log collection
    pipeline_log = []
    start_time = datetime.datetime.now()
    pipeline_log.append(f"Pipeline started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    pipeline_log.append("=" * 60)
    
    # Check prerequisites
    check_prerequisites()
    pipeline_log.append("✅ Prerequisites check passed")
    
    try:
        # Step 1: Generate bronze models
        pipeline_log.append("\n📊 Step 1: Generate bronze models")
        run_command(
            "dbt build --select bronze --project-dir ./dbt", 
            desc="Generate bronze models"
        )
        pipeline_log.append("✅ Bronze models generated successfully")
        
        # Step 2: Generate stg_orders staging table
        pipeline_log.append("\n📊 Step 2: Generate stg_orders staging table")
        run_command(
            "dbt build --select stg_orders --project-dir ./dbt", 
            desc="Generate stg_orders staging table"
        )
        pipeline_log.append("✅ stg_orders staging table generated successfully")
        
        # Step 3: Generate seed tables (skip SKU curation - assume ready)
        pipeline_log.append("\n📊 Step 3: Load seed tables")
        run_command(
            "dbt seed --project-dir ./dbt", 
            desc="Load seed tables"
        )
        pipeline_log.append("✅ Seed tables loaded successfully")
        
        # Step 4: Build remaining staging models
        pipeline_log.append("\n📊 Step 4: Build remaining staging models")
        run_command(
            "dbt build --select staging --exclude stg_orders --project-dir ./dbt", 
            desc="Build remaining staging models"
        )
        pipeline_log.append("✅ Remaining staging models built successfully")
        
        # Step 5: Generate silver models
        pipeline_log.append("\n📊 Step 5: Generate silver models")
        run_command(
            "dbt build --select silver --project-dir ./dbt", 
            desc="Generate silver models"
        )
        pipeline_log.append("✅ Silver models generated successfully")
        
        # Step 6: Generate gold models
        pipeline_log.append("\n📊 Step 6: Generate gold models")
        run_command(
            "dbt build --select gold --project-dir ./dbt", 
            desc="Generate gold models"
        )
        pipeline_log.append("✅ Gold models generated successfully")
        
        # Step 7: Run tests
        pipeline_log.append("\n📊 Step 7: Run data quality tests")
        run_command(
            "dbt test --project-dir ./dbt", 
            desc="Run data quality tests"
        )
        pipeline_log.append("✅ Data quality tests passed")
        
        # Step 8: Generate documentation
        pipeline_log.append("\n📚 Step 8: Generate documentation")
        docs_dir = Path("docs")
        docs_dir.mkdir(exist_ok=True)
        
        run_command(
            "dbt docs generate --static", 
            desc="Generate dbt documentation"
        )
        
        # Copy generated docs to docs directory
        dbt_target_dir = Path("dbt/target")
        if dbt_target_dir.exists():
            for doc_file in ["static_index.html"]:
                src = dbt_target_dir / doc_file
                if src.exists():
                    shutil.copy2(src, docs_dir / doc_file)
            pipeline_log.append("✅ Documentation generated and saved to docs/ as HTML")
        else:
            pipeline_log.append("⚠️  Warning: Could not find dbt target directory for docs")
        
        # Step 9: Save pipeline log
        pipeline_log.append("\n📝 Step 9: Save pipeline log")
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        pipeline_log.append(f"Pipeline completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        pipeline_log.append(f"Total duration: {duration}")
        pipeline_log.append("=" * 60)
        pipeline_log.append("🎉 AUTOMATED PIPELINE COMPLETED SUCCESSFULLY!")
        
        # Save log to file
        log_content = "\n".join(pipeline_log)
        log_file = Path("demo_pipeline.log")
        with open(log_file, "w", encoding="utf-8") as f:
            f.write(log_content)
        
        print("\n" + "=" * 60)
        print("🎉 AUTOMATED PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("Your data warehouse is ready at: warehouse/orderly.duckdb")
        print("Documentation generated in: docs/ (HTML format)")
        print(f"Pipeline log saved to: {log_file}")
        print("You can now run analytics using the notebook: notebooks/orderly_business_analytics.ipynb")
        print("To export data: make export-csv")
        
    except KeyboardInterrupt:
        print("\n⏸️  Pipeline interrupted by user")
        pipeline_log.append("❌ Pipeline interrupted by user")
        # Save partial log
        log_content = "\n".join(pipeline_log)
        with open("demo_pipeline_interrupted.log", "w", encoding="utf-8") as f:
            f.write(log_content)
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        pipeline_log.append(f"❌ Pipeline failed with error: {e}")
        # Save error log
        log_content = "\n".join(pipeline_log)
        with open("demo_pipeline_error.log", "w", encoding="utf-8") as f:
            f.write(log_content)
        sys.exit(1)

if __name__ == "__main__":
    main()
