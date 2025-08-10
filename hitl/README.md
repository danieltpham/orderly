# Human-in-the-Loop (HITL) Tools

This directory contains tools and utilities for the Human-in-the-Loop processes in the Orderly data pipeline. These tools facilitate manual data curation and quality control steps that require human judgment.

## 🎯 Purpose

In a medallion architecture, certain data quality processes require human expertise:
- **SKU Name Standardization**: Product names from different vendors need manual review
- **Vendor Mapping**: Identifying and correcting vendor name variations
- **Data Quality Review**: Manual inspection of exception cases

## 📁 Directory Structure

```
hitl/
├── README.md                 # This file - HITL process documentation
├── sku_curation_cli.py      # CLI tool for SKU curation workflow
├── generate_sku_seed.py     # Generate dbt seed files from curated data
└── utils/
    └── alias_cleaning.py    # Text cleaning and normalization utilities
```

## 🔄 HITL Workflow

### Step 1: Extract Candidates
```bash
python hitl/sku_curation_cli.py export
```
- Exports SKU candidates from `stg_orders` table
- Creates CSV file in `data/intermediate/curation_exports/`
- File contains potential SKU name mappings for review

### Step 2: Manual Curation
1. Open the exported CSV file
2. Review and correct SKU name mappings
3. Validate vendor assignments
4. Save with descriptive filename (e.g., `sku_name_curation_20250810_approved.csv`)

### Step 3: Generate Seed File
```bash
python hitl/generate_sku_seed.py data/intermediate/curation_exports/your_curated_file.csv
```
- Processes curated data
- Generates `dbt/seeds/ref_sku_names.csv`
- Ready for dbt seed loading

## 🛠️ Tool Details

### `sku_curation_cli.py`
**Purpose**: Command-line interface for SKU curation workflow
- Extracts unique SKU candidates from staging data
- Applies initial text cleaning and normalization
- Exports data in human-readable format for review
- Supports various export formats (CSV, Excel)

### `generate_sku_seed.py`
**Purpose**: Convert curated data into dbt seed format
- Takes manually curated CSV as input
- Validates data structure and completeness
- Generates properly formatted dbt seed file
- Handles data type conversions and null values

### `utils/alias_cleaning.py`
**Purpose**: Text cleaning and normalization utilities
- Standardizes product names and vendor names
- Removes special characters and formatting inconsistencies
- Applies business rules for name matching
- Fuzzy matching algorithms for similar names

## 📋 Integration with Main Pipeline

### Full Pipeline (with HITL)
```bash
make pipeline                    # Runs to HITL pause point
# Manual curation step here
make generate-sku-seed CURATION_FILE=path/to/curated.csv
make pipeline-continue           # Completes pipeline
```

### Automated Pipeline (skip HITL)
```bash
make pipeline-auto              # Assumes seed file already exists
```

## 🔍 Data Quality Checks

The HITL process includes several quality validation steps:

1. **Completeness**: All required fields populated
2. **Consistency**: Standardized naming conventions
3. **Accuracy**: Manual verification of mappings
4. **Uniqueness**: No duplicate SKU mappings

## 🎯 Best Practices

### Manual Curation Guidelines
- **SKU Names**: Use descriptive, standardized names
- **Vendor Names**: Match to official vendor registry
- **Categories**: Apply consistent product categorization
- **Validation**: Cross-check with external sources when possible

### File Naming Convention
```
sku_name_curation_YYYYMMDD_status.csv
```
Examples:
- `sku_name_curation_20250810_draft.csv`
- `sku_name_curation_20250810_approved.csv`
- `sku_name_curation_20250810_final.csv`

## 🚨 Common Issues & Solutions

### Issue: Missing SKU candidates
**Solution**: Ensure `stg_orders` table is populated before export

### Issue: Invalid curated data format
**Solution**: Check column headers match expected schema

### Issue: Seed generation fails
**Solution**: Validate curated CSV has no missing required fields

## 📊 Metrics & Monitoring

Track HITL process effectiveness:
- **Curation Coverage**: % of SKUs manually reviewed
- **Error Rate**: Issues found in curated data
- **Processing Time**: Time spent on manual review
- **Quality Score**: Post-curation data quality metrics

---

*This HITL process ensures high data quality while maintaining the automated benefits of the medallion architecture.*
