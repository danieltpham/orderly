# Transform Layer - Data Processing Pipeline

This document outlines the transform layer processing for SKU name curation and standardization.

## Overview

The transform layer handles post-bronze data processing, focusing on SKU (Stock Keeping Unit) name normalization and curation. All processing is done programmatically without creating intermediate database tables.

## SKU Name Curation Pipeline

### Location: `src/transform/sku_curation_cli.py`

**Purpose**: Generate curated SKU names directly from staging data using intelligent text cleaning algorithms.

**Process Flow**:

#### 1. Direct Data Generation
- **Source**: Queries `staging.stg_orders` directly (no intermediate tables)
- **Aggregation**: Groups by `sku_id` and `item_description_cleaned` 
- **Counting**: Counts occurrences of each description variant per SKU
- **Filtering**: Excludes records where `sku_id` or `item_description_cleaned` is null
- **Environment-Aware**: Automatically detects dev/prod schema prefixes

**Generated Candidates Schema**:
- `sku_id`: SKU identifier
- `item_description`: Cleaned item description variant
- `line_order_count`: Number of times this description appears for the SKU

#### 2. Intelligent Alias Cleaning

**Integration**: Uses `alias_cleaning.py` module for sophisticated text normalization.

**Pipeline Stages**:

##### 2.1 Text Normalization
- Convert to lowercase
- Remove punctuation using regex pattern `[^\w\s]+`
- Filter out English stop words
- Normalize whitespace

##### 2.2 Typo Collapse (Edit Distance)
- **Method**: Use Levenshtein edit distance to map similar tokens
- **Parameters**:
  - `max_edit_distance=2`: Maximum character differences allowed
  - `min_rep_len=4`: Minimum length for representative tokens
- **Effect**: Maps typos to their most frequent neighbors
  - Example: `keybord`, `kayboard` → `keyboard`

##### 2.3 Canonical Token Selection
- **Method**: Count token presence across all aliases (set-based)
- **Parameters**: `top_m=2` most frequent tokens
- **Output**: List of standardized tokens representing the product

##### 2.4 Similarity Ranking
- **Method**: Use fuzzy string matching (`fuzz.token_sort_ratio`)
- **Features**: Order-insensitive comparison with frequency tie-breaking
- **Output**: Top-3 most representative original aliases

##### 2.5 Canonicalization
- Apply representative mappings to create final standardized versions
- Return both original and canonicalized forms with confidence scores

#### 3. Automated Decision Logic
- **Auto-Approval Criteria**:
  - Top candidate score > runner-up score
  - Top candidate score > 80%
- **Decision Types**:
  - `AUTO`: High-confidence automatic approval
  - `NEED_APPROVAL`: Requires manual review

#### 4. Output Generation
- **Format**: CSV with date-tagged filename
- **Location**: `data/intermediate/curation_exports/sku_name_curation_YYYYMMDD.csv`
- **Schema**:
  ```csv
  sku_id,raw_names,match_name,match_score,final_sku_name,decision
  ```

### Example Output:
```csv
sku_id,raw_names,match_name,match_score,final_sku_name,decision
SKU001,"wireless keyboard black,techflow keybord...",wireless keyboard,100.0,wireless keyboard,AUTO
SKU002,"24"" led monitor viewmaster,wired mechanical...",wired mechanical keyboard,66.67,,NEED_APPROVAL
```

## Benefits of Integrated Approach

### No Intermediate Tables
- **Cleaner Database**: No `ref` schema or temporary tables cluttering the database
- **Direct Processing**: Queries staging data directly when needed
- **Reduced Complexity**: Fewer moving parts in the data pipeline

### Transform Layer Ownership
- **Single Responsibility**: Transform logic contained in Python modules
- **Version Control**: All processing logic tracked in source code
- **Testability**: Easier to unit test individual functions

### Flexibility
- **On-Demand Processing**: Generate candidates when needed
- **Parameterizable**: Easy to adjust algorithms and thresholds
- **Environment Agnostic**: Works across dev/prod without schema changes

## Usage

### CLI Command
```bash
python src/transform/sku_curation_cli.py export [--db-path warehouse/orderly.duckdb] [--output-dir data/intermediate/curation_exports]
```

### Parameters
- `--db-path`: Path to DuckDB database (default: ./warehouse/orderly.duckdb)
- `--output-dir`: Output directory for CSV files (default: data/intermediate/curation_exports)

## Quality Assurance

### Processing Statistics
- **Coverage**: Processes all unique SKU-description combinations from staging
- **Decision Distribution**: Tracks auto-approved vs manual review cases
- **Confidence Scoring**: Provides quantitative quality metrics (0-100 scale)

### Data Validation
- **Source Validation**: Ensures required columns exist in staging data
- **Output Validation**: Validates generated CSV structure
- **Error Handling**: Graceful handling of database connection issues

## Next Steps

The generated curation table serves as input for:
1. **Manual Review Process**: Human validation of `NEED_APPROVAL` cases
2. **Seed Table Creation**: Convert approved names to dbt seed files
3. **Gold Layer Integration**: Use standardized names in final dimensional models

This pipeline ensures data quality and consistency while minimizing manual curation effort through intelligent automation.
