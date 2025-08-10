"""
Main orchestration script for running the complete Medallion architecture pipeline.
This script executes each pipeline step in sequence, with error handling and logging.
"""

import subprocess
import sys

def run_command(cmd, desc=None):
	print(f"\n{'='*60}")
	if desc:
		print(f"Step: {desc}")
	print(f"Running: {cmd}")
	result = subprocess.run(cmd, shell=True)
	if result.returncode != 0:
		print(f"ERROR: Command failed: {cmd}")
		sys.exit(result.returncode)

if __name__ == "__main__":
	# Generate bronze
	run_command("dbt build --select bronze", desc="Generate bronze models")

	# Generate first staging table
	run_command("dbt build --select stg_orders", desc="Generate stg_orders staging table")

	# Generate local .csv to curate SKU seed file
	run_command("python src/transform/sku_curation_cli.py export", desc="Export SKU curation candidates")

	# Update seed file with proper curation, then run this to update seed csv
	run_command("python src/transform/generate_sku_seed.py data/intermediate/curation_exports/sku_name_curation_20250809_approved.csv", desc="Generate SKU seed from curated file")

	# Generate seed
	run_command("dbt seed", desc="Generating seed table")

	# Run the rest of the staging
	run_command("dbt build --select staging --exclude stg_orders", desc="Build remaining staging models")

	# Run silver
	run_command("dbt build --select silver", desc="Generate silver models")