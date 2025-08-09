.PHONY: init dbt-build-bronze docs clean help

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
    @echo "  docs              - Generate dbt documentation"
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
    @echo "Created warehouse directory"
    @echo "Initialization complete!"

# Build Bronze layer
dbt-build-bronze:
    @echo "Building Bronze layer..."
    @if [ ! -f .env ]; then \
        echo "Error: .env file not found. Run 'make init' first."; \
        exit 1; \
    fi
    cd dbt && dbt build --select path:models/bronze

# Generate documentation
docs:
    @echo "Generating dbt documentation..."
    cd dbt && dbt docs generate

# Clean dbt artifacts
clean:
    @echo "Cleaning dbt artifacts..."
    rm -rf dbt/target/
    rm -rf dbt/dbt_packages/
    rm -rf dbt/logs/
    @echo "Clean complete!"