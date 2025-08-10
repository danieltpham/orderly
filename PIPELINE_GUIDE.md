# Orderly Pipeline Execution Guide

This guide explains how to use the Makefile and automated scripts to run the Orderly data pipeline.

## 📋 Quick Start

### Option 1: Full Pipeline with Human-in-the-Loop (HITL)
```bash
# Initialize the project
make init

# Start the pipeline (will pause for manual SKU curation)
make pipeline

# After manual curation, continue with:
make generate-sku-seed CURATION_FILE=data/intermediate/curation_exports/your_curated_file.csv
make pipeline-continue
```

### Option 2: Automated Pipeline (No Manual Steps)
```bash
# Prerequisites: Ensure source files and curated seed file are ready
make pipeline-auto
```

### Option 3: Python Script (Automated)
```bash
# Run the streamlined Python script
python demo_pipeline_local.py
```

## 🎯 Pipeline Overview

### HITL Pipeline Steps (`make pipeline`)
1. **Initialize** - Set up directories and environment
2. **Bronze Layer** - Process raw source files
3. **Initial Staging** - Create `stg_orders` table
4. **Export SKU Candidates** - Generate curation candidates (⏸️ **MANUAL STEP**)
5. **Manual SKU Curation** - Human reviews and curates SKU mappings (⏸️ **MANUAL STEP**)
6. **Generate SKU Seed** - Create seed file from curated data (⏸️ **MANUAL STEP**)
7. **Load Seeds** - Update dbt seed tables
8. **Complete Staging** - Build remaining staging models
9. **Silver Layer** - Create cleaned, business-ready data
10. **Gold Layer** - Build final dimensional model

### Automated Pipeline Steps (`make pipeline-auto`)
Same as above but skips steps 4-6, assuming the SKU seed file is already prepared.

## 🔧 Individual Commands

### Setup
```bash
make init                    # Initialize project
make status                  # Check pipeline status
```

### Pipeline Stages
```bash
make stage-bronze           # Generate bronze models
make stage-stg-orders       # Generate stg_orders staging table
make export-sku-candidates  # Export SKU candidates for curation (HITL)
make generate-sku-seed CURATION_FILE=path/to/file.csv  # Generate seed (HITL)
make seed-update            # Update seed tables
make stage-remaining        # Build remaining staging models
make silver                 # Generate silver models
make gold                   # Generate gold models
```

### Testing & Documentation
```bash
make dbt-test               # Run all dbt tests
make docs                   # Generate and serve dbt documentation
```

### Data Export
```bash
make export-csv             # Export all layers to CSV
make export-bronze          # Export bronze layer only
make export-silver          # Export silver layer only
make export-gold            # Export gold layer only
```

### Maintenance
```bash
make clean                  # Clean dbt artifacts
make full-rebuild           # Complete rebuild from scratch
```

## 📁 Directory Structure

```
orderly/
├── data/
│   ├── source/                          # Raw source files (JSON, CSV)
│   ├── intermediate/
│   │   └── curation_exports/           # SKU curation files (HITL)
│   └── exports/                        # Exported CSV files
├── dbt/
│   ├── models/
│   │   ├── bronze/                     # Raw data models
│   │   ├── staging/                    # Staged models
│   │   ├── silver/                     # Cleaned business data
│   │   └── gold/                       # Dimensional models
│   └── seeds/
│       └── ref_sku_names.csv          # Curated SKU mappings
├── hitl/                               # Human-in-the-Loop tools
│   ├── README.md                       # HITL process documentation
│   ├── sku_curation_cli.py            # SKU curation CLI tool
│   ├── generate_sku_seed.py           # Generate seed from curated data
│   └── utils/
│       └── alias_cleaning.py          # Text cleaning utilities
├── warehouse/
│   └── orderly.duckdb                  # Final data warehouse
├── notebooks/
│   └── orderly_business_analytics.ipynb  # Analytics notebook
├── Makefile                            # Build automation
└── demo_pipeline_local.py              # Automated demo pipeline
```

## 🔄 Human-in-the-Loop (HITL) Process

The HITL steps are crucial for data quality and involve manual review of SKU mappings:

### Step 1: Export SKU Candidates
```bash
make export-sku-candidates
```
This creates a file in `data/intermediate/curation_exports/` with potential SKU mappings.

### Step 2: Manual Curation
1. Open the exported CSV file
2. Review and correct SKU name mappings
3. Save the curated file with a descriptive name (e.g., `sku_name_curation_20250810_approved.csv`)

### Step 3: Generate Seed File
```bash
make generate-sku-seed CURATION_FILE=data/intermediate/curation_exports/sku_name_curation_20250810_approved.csv
```

### Step 4: Continue Pipeline
```bash
make pipeline-continue
```

## ⚡ Prerequisites for Automated Pipeline

Before running `make pipeline-auto` or `python run_pipeline_auto.py`, ensure:

1. **Source files** are in `data/source/`
2. **Curated SKU seed file** exists at `dbt/seeds/ref_sku_names.csv`
3. **Environment** is set up (`.env` file exists)

## 🛠️ Troubleshooting

### Common Issues

**Error: "No source files found"**
- Ensure source data files are in `data/source/`
- Check file permissions

**Error: "SKU seed file not found"**
- Run the HITL pipeline first, or
- Manually place a curated `ref_sku_names.csv` in `dbt/seeds/`

**Error: "dbt command failed"**
- Check `dbt/logs/` for detailed error messages
- Ensure database connection is working
- Verify model dependencies

### Getting Help
```bash
make help                   # Show all available commands
make status                 # Check current pipeline status
```

## 📊 Analytics

After the pipeline completes successfully, you can:

1. **Run Analytics Notebook**:
   ```bash
   cd notebooks
   jupyter notebook orderly_business_analytics.ipynb
   ```

2. **Export Data for External Tools**:
   ```bash
   make export-csv
   ```

3. **View dbt Documentation**:
   ```bash
   make docs
   ```

The warehouse file `warehouse/orderly.duckdb` contains all your processed data and can be queried directly with DuckDB or connected to BI tools.
