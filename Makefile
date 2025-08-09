.PHONY: init dbt-build-bronze dbt-run dbt-test docs clean export-csv export-bronze export-silver export-gold pipeline help

# Load environment variables if .env exists
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Default target
help:
	@echo "Available targets:"
	@echo "  init              - Initialize project (create .env and warehouse directory)"
	@echo "  dbt-build-bronze  - Build Bronze layer models"
	@echo "  dbt-run           - Run all dbt models"
	@echo "  dbt-test          - Run all dbt tests"
	@echo "  docs              - Generate dbt documentation"
	@echo "  export-csv        - Export all layers to CSV (data/intermediate)"
	@echo "  export-bronze     - Export bronze layer to CSV"
	@echo "  export-silver     - Export silver layer to CSV"
	@echo "  export-gold       - Export gold layer to CSV"
	@echo "  pipeline          - Run complete pipeline with CSV export"
	@echo "  clean             - Clean dbt artifacts"

# Initialize project
init:
	@echo "Initializing Orderly project..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env from .env.example"; \
	else \
		echo ".env already exists"; \
	fi
	@mkdir -p warehouse
	@mkdir -p data/intermediate
	@echo "Created warehouse and data/intermediate directories"
	@echo "Initialization complete!"

# Build Bronze layer
dbt-build-bronze:
	@echo "Building Bronze layer..."
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Run 'make init' first."; \
		exit 1; \
	fi
	cd dbt && dbt build --select path:models/bronze

# Run all dbt models
dbt-run:
	@echo "Running all dbt models..."
	cd dbt && dbt run

# Run all dbt tests
dbt-test:
	@echo "Running all dbt tests..."
	cd dbt && dbt test

# Generate documentation
docs:
	@echo "Generating dbt documentation..."
	cd dbt && dbt docs generate

# Export all layers to CSV
export-csv:
	@echo "Exporting all layers to CSV..."
	@mkdir -p data/intermediate
	python export_csv.py --layers bronze silver gold

# Export bronze layer to CSV
export-bronze:
	@echo "Exporting bronze layer to CSV..."
	@mkdir -p data/intermediate
	python export_csv.py --layers bronze

# Export silver layer to CSV
export-silver:
	@echo "Exporting silver layer to CSV..."
	@mkdir -p data/intermediate
	python export_csv.py --layers silver

# Export gold layer to CSV
export-gold:
	@echo "Exporting gold layer to CSV..."
	@mkdir -p data/intermediate
	python export_csv.py --layers gold

# Run complete pipeline with CSV export
pipeline:
	@echo "Running complete pipeline with CSV export..."
	python run_dbt.py --export-csv

# Clean dbt artifacts
clean:
	@echo "Cleaning dbt artifacts..."
	rm -rf dbt/target/
	rm -rf dbt/dbt_packages/
	rm -rf dbt/logs/
	@echo "Clean complete!"