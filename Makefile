.PHONY: help init pipeline pipeline-auto dbt-build-bronze dbt-run dbt-test docs clean export-csv export-bronze export-silver export-gold stage-bronze stage-stg-orders export-sku-candidates generate-sku-seed seed-update stage-remaining silver gold full-rebuild

# Load environment variables if .env exists
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Default target
help:
	@echo "==================================================================="
	@echo "                    ORDERLY DATA PIPELINE"
	@echo "==================================================================="
	@echo ""
	@echo "🚀 MAIN TARGETS:"
	@echo "  pipeline           - Full HITL pipeline (Human-in-the-Loop)"
	@echo "  pipeline-auto      - Automated pipeline (skips HITL steps)"
	@echo "  full-rebuild       - Complete rebuild from scratch"
	@echo ""
	@echo "🔧 SETUP:"
	@echo "  init               - Initialize project (create .env and directories)"
	@echo ""
	@echo "🏗️  PIPELINE STAGES:"
	@echo "  stage-bronze       - Generate bronze models"
	@echo "  stage-stg-orders   - Generate stg_orders staging table"
	@echo "  export-sku-candidates - Export SKU curation candidates (HITL)"
	@echo "  generate-sku-seed  - Generate SKU seed from curated file (HITL)"
	@echo "  seed-update        - Update seed tables"
	@echo "  stage-remaining    - Build remaining staging models"
	@echo "  silver             - Generate silver models"
	@echo "  gold               - Generate gold models"
	@echo ""
	@echo "🧪 TESTING & DOCS:"
	@echo "  dbt-test           - Run all dbt tests"
	@echo "  docs               - Generate dbt documentation"
	@echo ""
	@echo "📊 EXPORTS:"
	@echo "  export-csv         - Export all layers to CSV"
	@echo "  export-bronze      - Export bronze layer to CSV"
	@echo "  export-silver      - Export silver layer to CSV"
	@echo "  export-gold        - Export gold layer to CSV"
	@echo ""
	@echo "🧹 CLEANUP:"
	@echo "  clean              - Clean dbt artifacts"
	@echo ""
	@echo "==================================================================="

# Initialize project
init:
	@echo "🚀 Initializing Orderly project..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ Created .env from .env.example"; \
	else \
		echo "ℹ️  .env already exists"; \
	fi
	@mkdir -p warehouse
	@mkdir -p data/intermediate
	@mkdir -p data/intermediate/curation_exports
	@mkdir -p data/exports
	@echo "✅ Created warehouse and data directories"
	@echo "🎉 Initialization complete!"

# =================================================================
# FULL PIPELINE WITH HUMAN-IN-THE-LOOP (HITL)
# =================================================================
pipeline: init stage-bronze stage-stg-orders export-sku-candidates
	@echo ""
	@echo "⏸️  PIPELINE PAUSED FOR HUMAN INTERVENTION"
	@echo "==================================================================="
	@echo "📋 NEXT STEPS (Manual):"
	@echo "1. Review SKU candidates in: data/intermediate/curation_exports/"
	@echo "2. Update the curation file with proper mappings"
	@echo "3. Run: make generate-sku-seed CURATION_FILE=<your_curated_file.csv>"
	@echo "4. Run: make pipeline-continue"
	@echo "==================================================================="

# Continue pipeline after HITL step
pipeline-continue: seed-update stage-remaining silver gold
	@echo "🎉 Pipeline completed successfully!"

# =================================================================
# AUTOMATED PIPELINE (NO HITL - assumes seed file is ready)
# =================================================================
pipeline-auto: init stage-bronze stage-stg-orders seed-update stage-remaining silver gold
	@echo "🎉 Automated pipeline completed successfully!"

# =================================================================
# INDIVIDUAL PIPELINE STAGES
# =================================================================

# Stage 1: Generate bronze models
stage-bronze:
	@echo "🔄 Step 1: Generate bronze models"
	@echo "Running: dbt build --select bronze"
	cd dbt && dbt build --select bronze
	@echo "✅ Bronze models completed"

# Stage 2: Generate first staging table
stage-stg-orders:
	@echo "🔄 Step 2: Generate stg_orders staging table"
	@echo "Running: dbt build --select stg_orders"
	cd dbt && dbt build --select stg_orders
	@echo "✅ stg_orders completed"

# Stage 3: Export SKU curation candidates (HITL step)
export-sku-candidates:
	@echo "🔄 Step 3: Export SKU curation candidates"
	@echo "Running: python hitl/sku_curation_cli.py export"
	python hitl/sku_curation_cli.py export
	@echo "✅ SKU candidates exported to data/intermediate/curation_exports/"

# Stage 4: Generate SKU seed from curated file (HITL step)
generate-sku-seed:
	@if [ -z "$(CURATION_FILE)" ]; then \
		echo "❌ Error: CURATION_FILE not specified"; \
		echo "Usage: make generate-sku-seed CURATION_FILE=path/to/curated_file.csv"; \
		exit 1; \
	fi
	@echo "🔄 Step 4: Generate SKU seed from curated file"
	@echo "Running: python hitl/generate_sku_seed.py $(CURATION_FILE)"
	python hitl/generate_sku_seed.py $(CURATION_FILE)
	@echo "✅ SKU seed generated"

# Stage 5: Update seed tables
seed-update:
	@echo "🔄 Step 5: Generating seed table"
	@echo "Running: dbt seed"
	cd dbt && dbt seed
	@echo "✅ Seed tables updated"

# Stage 6: Build remaining staging models
stage-remaining:
	@echo "🔄 Step 6: Build remaining staging models"
	@echo "Running: dbt build --select staging --exclude stg_orders"
	cd dbt && dbt build --select staging --exclude stg_orders
	@echo "✅ Remaining staging models completed"

# Stage 7: Generate silver models
silver:
	@echo "🔄 Step 7: Generate silver models"
	@echo "Running: dbt build --select silver"
	cd dbt && dbt build --select silver
	@echo "✅ Silver models completed"

# Stage 8: Generate gold models
gold:
	@echo "🔄 Step 8: Generate gold models"
	@echo "Running: dbt build --select gold"
	cd dbt && dbt build --select gold
	@echo "✅ Gold models completed"

# =================================================================
# UTILITY TARGETS
# =================================================================

# Complete rebuild from scratch
full-rebuild: clean init pipeline-auto
	@echo "🎉 Full rebuild completed!"

# Run all dbt models
dbt-run:
	@echo "🔄 Running all dbt models..."
	cd dbt && dbt run

# Run all dbt tests
dbt-test:
	@echo "🧪 Running all dbt tests..."
	cd dbt && dbt test

# Generate documentation
docs:
	@echo "📚 Generating dbt documentation..."
	cd dbt && dbt docs generate
	cd dbt && dbt docs serve

# Export all layers to CSV
export-csv: export-bronze export-silver export-gold
	@echo "✅ All layers exported to CSV"

# Export bronze layer to CSV
export-bronze:
	@echo "📊 Exporting bronze layer to CSV..."
	@mkdir -p data/exports
	python -c "import duckdb; conn = duckdb.connect('warehouse/orderly.duckdb'); tables = conn.execute(\"SHOW TABLES FROM dev_bronze\").fetchall(); [conn.execute(f\"COPY dev_bronze.{table[0]} TO 'data/exports/bronze_{table[0]}.csv' (HEADER, DELIMITER ',')\") for table in tables]; print('Bronze layer exported')"

# Export silver layer to CSV
export-silver:
	@echo "📊 Exporting silver layer to CSV..."
	@mkdir -p data/exports
	python -c "import duckdb; conn = duckdb.connect('warehouse/orderly.duckdb'); tables = conn.execute(\"SHOW TABLES FROM dev_silver\").fetchall(); [conn.execute(f\"COPY dev_silver.{table[0]} TO 'data/exports/silver_{table[0]}.csv' (HEADER, DELIMITER ',')\") for table in tables]; print('Silver layer exported')"

# Export gold layer to CSV
export-gold:
	@echo "📊 Exporting gold layer to CSV..."
	@mkdir -p data/exports
	python -c "import duckdb; conn = duckdb.connect('warehouse/orderly.duckdb'); tables = conn.execute(\"SHOW TABLES FROM dev_gold\").fetchall(); [conn.execute(f\"COPY dev_gold.{table[0]} TO 'data/exports/gold_{table[0]}.csv' (HEADER, DELIMITER ',')\") for table in tables]; print('Gold layer exported')"

# Clean dbt artifacts
clean:
	@echo "🧹 Cleaning dbt artifacts..."
	rm -rf dbt/target/
	rm -rf dbt/dbt_packages/
	rm -rf dbt/logs/
	@echo "✅ Clean complete!"

# Quick status check
status:
	@echo "📋 Pipeline Status Check"
	@echo "========================"
	@echo "Source files:"
	@ls -la data/source/ 2>/dev/null || echo "❌ No source files found"
	@echo ""
	@echo "Warehouse:"
	@ls -la warehouse/ 2>/dev/null || echo "❌ No warehouse found"
	@echo ""
	@echo "Exports:"
	@ls -la data/exports/ 2>/dev/null || echo "ℹ️  No exports found"