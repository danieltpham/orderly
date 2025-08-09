# Silver Layer Data Cleaning Pipeline Suggestions

## Overview
Based on the exported bronze CSV data and the original data generation plan, this document outlines comprehensive data cleaning strategies for the Silver layer using Python/pandas. The goal is to transform the raw, messy bronze data into clean, standardized, and analysis-ready datasets.

## Data Quality Assessment from Bronze Layer

### Current Data Issues Identified

From analyzing the bronze exports, we've identified these data quality patterns:

#### 1. **Order Data Issues (`bronze_raw_orders.csv`)**
- **HTML Encoding**: `&quot;`, `&amp;`, `&lt;br&gt;`, `&gt;` characters
- **Brand Inconsistencies**: "DataVault" vs "DATAVAULT" vs "Dvlt"
- **Missing Values**: Empty brand fields, missing size information
- **Inconsistent Casing**: "lcd 27 inch monitor V-Master" vs "ViewMaster 22 inch LCD Monitor"
- **Fuzzy Descriptions**: "Wired Keyboard of Mechanical" vs clean product names
- **Non-Product Entries**: Tax entries, fees with null `sku_id`
- **Unit Inconsistencies**: "each" vs "pack" for similar items

#### 2. **Reference Data Quality**
- **Cost Centres**: Clean, structured hierarchy data
- **Exchange Rates**: Clean financial data with proper timestamps
- **Vendor Master**: Clean master data with standardized formats

## Recommended Silver Layer Cleaning Pipeline

1. **Phase 1**: HTML cleaning and basic text standardization using built-in parsers
2. **Phase 2**: Fuzzy brand standardization against canonical reference data
3. **Phase 3**: TF-IDF based product description matching and normalization
4. **Phase 4**: Advanced SKU resolution using multi-criteria fuzzy matching
5. **Phase 5**: Adaptive currency conversion with confidence scoring
6. **Phase 6**: Dynamic quality scoring and statistical anomaly detection
7. **Phase 7**: Intelligent reference data enrichment and lineage tracking

## Key Benefits of Fuzzy Matching Approach

- **Adaptive Learning**: No hard-coded rules; learns patterns from clean data
- **Robust Matching**: Handles typos, abbreviations, and variations automatically
- **Confidence Scoring**: Provides transparency on matching quality
- **Scalable Framework**: Automatically adapts to new brands, products, and patterns
- **Statistical Validation**: Uses data-driven approaches for anomaly detection
- **Quality Transparency**: Tracks transformation confidence and data lineage
- **Reduced Maintenance**: Minimal manual rule updates required
- **Better Coverage**: Finds matches that rule-based systems might miss

## Advanced Features

- **Multi-criteria Matching**: Combines description, brand, and context for better accuracy
- **Adaptive Thresholds**: Dynamically adjusts matching confidence based on data quality
- **Learning from Clean Data**: Uses high-quality entries as training examples
- **Statistical Anomaly Detection**: Identifies pricing and data inconsistencies
- **Transformation Tracking**: Complete audit trail of all data changes
- **Confidence Propagation**: Tracks uncertainty through the entire pipeline

## Monitoring and Maintenance

- **Match Quality Monitoring**: Track fuzzy matching effectiveness over time
- **Threshold Optimization**: Periodically review and adjust similarity thresholds
- **New Pattern Detection**: Identify emerging data quality issues automatically
- **Performance Monitoring**: Track processing time and resource usage
- **False Positive Analysis**: Review high-confidence matches that may be incorrect
- **Canonical Data Quality**: Ensure clean reference data remains high quality

This fuzzy matching approach creates a more intelligent, adaptive, and maintainable data cleaning pipeline that learns from your data patterns rather than relying on brittle rule-based systems.


## Implementation update

Silver Pipeline Implementation Summary
🎯 Core Architecture
Python + SQL Hybrid: Used Python for fuzzy matching (RapidFuzz) and SQL for the rest
Quality-First Approach: Implemented comprehensive data quality flags and filtering
Monitoring & Observability: Created separate valid/exception tables for monitoring
📊 Results
Total Order Lines: 423 (from stg_orders)
Valid Orders: 243 lines passed quality checks
Exceptions: 180 lines flagged with issues
30 lines: SKU not found in seed (SKU_NOT_IN_SEED)
150 lines: Name mismatch with fuzzy score < 0.8 (NAME_MISMATCH)
🏗️ Models Created
Simple Passthrough Models
silver_cost_centres - Clean cost centre reference data
silver_exchange_rates - Clean exchange rate data
silver_vendor_master - Clean vendor reference data
Advanced Order Processing Models
silver_orders_fuzz_scores (Python)

Computes RapidFuzz partial_token_ratio scores
Compares order descriptions vs seed canonical names
Only processes SKUs that exist in both tables
silver_orders_with_flags (SQL)

Flags missing SKUs (not in seed)
Flags name mismatches (fuzzy score < 0.8)
Includes debugging columns for analysis
silver_orders_valid (SQL)

Clean, high-quality orders ready for gold layer
Filters out all flagged records
silver_orders_exceptions (SQL)

Quality issues for monitoring/analysis
Categorized by exception type
🔧 Technical Implementation
Python Model: Successfully integrates RapidFuzz with dbt-duckdb
Robust References: Correctly handles dev schema prefixes (dev_seeds.ref_sku_names)
Quality Metrics: Provides quantitative confidence scoring (0-1 scale)
Schema Tests: Comprehensive validation with dbt tests