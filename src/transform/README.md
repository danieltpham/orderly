# Post-Bronze Data Processing Pipeline

This document outlines the steps taken after exporting bronze tables to create curated, clean data ready for gold layer transformation.

## Overview

After bronze tables are exported, the pipeline focuses on data curation and standardization, particularly for SKU (Stock Keeping Unit) name normalization. The process involves aggregating raw order data into candidate records and applying intelligent text cleaning algorithms to standardize product names.

## Step 1: SKU Name Candidate Aggregation

### Location: `dbt/models/ref/cur_sku_name_candidates.sql`

**Purpose**: Aggregate staged order data to create a reference table of SKU name candidates for manual curation.

**Process**:
1. **Source Data**: Reads from `silver.stg_orders` (staged orders table)
2. **Aggregation**: Groups by `sku_id` and `item_description_cleaned` 
3. **Counting**: Counts occurrences of each description variant per SKU
4. **Filtering**: Excludes records where `sku_id` or `item_description_cleaned` is null
5. **Ordering**: Orders by SKU ID and frequency (most common descriptions first)

**Output Schema**:
- `sku_id`: SKU identifier
- `item_description`: Cleaned item description variant
- `line_order_count`: Number of times this description appears for the SKU

**Auto-Export**: In development environment, automatically exports results to CSV:
```
data/intermediate/cur_sku_name_candidates.csv
```

### Example Output:
```csv
sku_id,item_description,line_order_count
SKU001,wireless keyboard black,2
SKU001,techflow keybord wireles black,2
SKU001,brand new wireless keyboard black,2
SKU001,techflow wireless keyboard,1
```

## Step 2: Intelligent Alias Cleaning

### Location: `tools/alias_cleaning.py`

**Purpose**: Apply sophisticated text normalization and variant collapse algorithms to standardize product name aliases.

**Pipeline Stages**:

#### 2.1 Text Normalization
- Convert to lowercase
- Remove punctuation using regex pattern `[^\w\s]+`
- Filter out English stop words
- Normalize whitespace

#### 2.2 Typo Collapse (Edit Distance)
- **Function**: `build_rep_map_typo_collapse()`
- **Method**: Use Levenshtein edit distance to map similar tokens
- **Parameters**:
  - `max_edit_distance=2`: Maximum character differences allowed
  - `min_rep_len=4`: Minimum length for representative tokens
- **Effect**: Maps typos to their most frequent neighbors
  - Example: `keybord`, `kayboard` → `keyboard`

#### 2.3 Canonical Token Selection
- **Function**: `most_frequent_canonical_tokens()`
- **Method**: Count token presence across all aliases (set-based)
- **Parameters**: `top_m=5` most frequent tokens
- **Output**: List of standardized tokens representing the product

#### 2.4 Similarity Ranking
- **Function**: `rank_by_tokens()`
- **Method**: Use fuzzy string matching (`fuzz.token_sort_ratio`)
- **Features**: Order-insensitive comparison with frequency tie-breaking
- **Output**: Top-K most representative original aliases

#### 2.5 Canonicalization
- Apply representative mappings to create final standardized versions
- Return both original and canonicalized forms with confidence scores

## Step 3: Automated SKU Curation

### Location: `tools/sku_seed_cli.py`

**Purpose**: Generate pre-approved curation table for SKU names using the alias cleaning pipeline.

**Process Flow**:

#### 3.1 Data Loading
- **Source**: DuckDB ref schema (`cur_sku_name_candidates`)
- **Environment-Aware**: Automatically detects dev/prod schema prefixes
- **Query**: Loads all SKU candidates with their occurrence counts

#### 3.2 SKU Group Processing
- **Function**: `process_sku_group()`
- **Input**: SKU ID + list of description variants
- **Algorithm**: Apply `transform_aliases_with_canonical_tokens()` with tuned parameters:
  - `max_edit_distance=2`
  - `top_m_canonical_tokens=2`
  - `top_k_original=3`

#### 3.3 Decision Logic
- **Auto-Approval Criteria**:
  - Top candidate score > runner-up score
  - Top candidate score > 80%
- **Decision Types**:
  - `AUTO`: High-confidence automatic approval
  - `NEED_APPROVAL`: Requires manual review

#### 3.4 Output Generation
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

## Step 4: Quality Assurance

### Data Validation
- **Schema Tests**: Defined in `dbt/models/ref/schema.yml`
- **Not-Null Constraints**: All key fields (sku_id, item_description, line_order_count)
- **Referential Integrity**: Links back to staged orders

### Processing Statistics
- **Coverage**: Processes all unique SKU-description combinations
- **Decision Distribution**: Tracks auto-approved vs manual review cases
- **Confidence Scoring**: Provides quantitative quality metrics (0-100 scale)

## Usage

### CLI Command
```bash
python tools/sku_seed_cli.py export [--db-path warehouse/orderly.duckdb] [--output-dir data/intermediate/curation_exports]
```

## Next Steps

The generated curation table serves as input for:
1. **Manual Review Process**: Human validation of `NEED_APPROVAL` cases
2. **Seed Table Creation**: Convert approved names to dbt seed files
3. **Gold Layer Integration**: Use standardized names in final dimensional models

This pipeline ensures data quality and consistency while minimizing manual curation effort through intelligent automation.
