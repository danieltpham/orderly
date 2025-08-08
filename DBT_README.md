# Orderly: dbt + DuckDB Implementation Guide

This guide covers the next phase of the orderly project: implementing dbt transformations with DuckDB for fuzzy matching and data quality analytics.

## 🏗️ Architecture Overview

```
orderly/
├── data/
│   ├── raw/                  # SAP ARIBA CSV exports  
│   ├── staged/               # Cleaned pandas outputs
│   ├── final/                # dbt model outputs
│   └── simulated/            # AI-generated test data
├── dbt/
│   ├── models/
│   │   ├── staging/          # Raw data staging
│   │   ├── intermediate/     # Data cleaning & enrichment
│   │   └── marts/            # Business-ready tables
│   ├── seeds/                # Reference data
│   └── dbt_project.yml       # dbt configuration
├── src/
│   ├── etl_core/             # Pandas ETL scripts
│   ├── matching/             # Fuzzy matching logic
│   ├── validation/           # Data quality checks
│   └── config.py             # Project configuration
├── notebooks/
│   └── exploratory.ipynb    # EDA and analysis
└── tests/                    # Unit tests
```

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate Sample Data
```bash
# Generate AI-powered test data
python data_generation/generate_data.py

# Create SAP ARIBA simulation data lake
python data_generation/1_generate_data_lake.py
```

### 3. Run ETL Pipeline
```bash
# Clean line items with pandas
python src/etl_core/clean_lineitems.py

# Run fuzzy matching
python src/matching/fuzzy_match_items.py

# Validate data quality
python src/validation/expectations_suite.py
```

### 4. Execute dbt Transformations
```bash
cd dbt

# Install dbt dependencies (if any)
dbt deps

# Run staging models
dbt run --select staging

# Run all models
dbt run

# Generate documentation
dbt docs generate
dbt docs serve
```

## 📊 Data Flow

### Stage 1: Raw Data Sources
- **Line Items** (JSONL): Messy user inputs from procurement requests
- **Product SKUs** (JSONL): Clean catalog of available products
- **Vendor Master** (CSV): SAP ARIBA vendor records
- **Cost Centers** (CSV): Organization structure
- **Exchange Rates** (CSV): Currency conversion data

### Stage 2: Staging Layer (dbt)
- `stg_line_items`: Type-cast and validate line items
- `stg_product_skus`: Clean product catalog
- `stg_vendor_master`: Standardize vendor data
- `stg_cost_centers`: Validate cost center data
- `stg_exchange_rates`: Currency rate staging

### Stage 3: Intermediate Layer (dbt)
- `int_line_items_cleaned`: Advanced text cleaning and categorization
- `int_vendors_enriched`: Vendor searchability enhancement

### Stage 4: Marts Layer (dbt)
- `mart_procurement_matches`: Final fuzzy matching results with confidence scores
- `mart_data_quality_metrics`: KPIs and performance tracking

## 🎯 Fuzzy Matching Strategy

### Matching Hierarchy
1. **Direct SKU Match** (Confidence: 0.95)
   - Extract SKU patterns: `[A-Z]{2,4}-[0-9]{3,6}`
   - Exact match against product catalog

2. **Brand + Text Similarity** (Confidence: 0.60-0.90)
   - Identify brand mentions in user input
   - Fuzzy match product names within brand

3. **General Text Similarity** (Confidence: 0.40-0.70)
   - Broad fuzzy matching across all products
   - Minimum similarity threshold: 0.5

4. **Manual Review** (Confidence: < 0.40)
   - Very poor quality inputs
   - No reasonable matches found

### Confidence Scoring Factors
- **SKU Pattern Present**: +0.30
- **Brand Match**: +0.20  
- **Text Similarity**: Base score
- **Data Quality Flag**: Multiplier (0.8-1.2)

## 📈 Analytics & Monitoring

### Key Metrics Tracked
- **Match Success Rate**: % of items successfully matched
- **Confidence Distribution**: Quality of matches
- **Processing Categories**: Volume by difficulty level
- **Vendor Performance**: Matching success by vendor
- **Daily Trends**: Processing volume and quality over time

### Data Quality Checks
- Required field validation
- Business rule validation (price ranges, quantities)
- Referential integrity (vendor IDs, cost centers)
- Format validation (emails, currencies, country codes)

## 🔧 Configuration

### Database Connection
- **Type**: DuckDB (embedded analytics database)
- **Location**: `data/orderly.duckdb`
- **Schema**: main (staging, intermediate, marts)

### Environment Variables (.env)
```bash
# Database
DUCKDB_PATH=./data/orderly.duckdb
DUCKDB_THREADS=4

# Processing
MIN_SIMILARITY_THRESHOLD=0.3
HIGH_CONFIDENCE_THRESHOLD=0.8

# Environment
ENVIRONMENT=development
```

## 🧪 Testing

### Unit Tests
```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_cleaning.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

### dbt Tests
```bash
cd dbt

# Run all dbt tests
dbt test

# Test specific models
dbt test --select staging
dbt test --select marts
```

## 📝 Development Workflow

### 1. Data Generation
```bash
python data_generation/generate_data.py
python data_generation/1_generate_data_lake.py
```

### 2. Exploratory Analysis
```bash
jupyter notebook notebooks/exploratory.ipynb
```

### 3. ETL Development
```bash
python src/etl_core/clean_lineitems.py
python src/matching/fuzzy_match_items.py
```

### 4. dbt Development
```bash
cd dbt
dbt run --select +model_name  # Run model and dependencies
dbt test --select model_name  # Test specific model
dbt docs generate && dbt docs serve  # Documentation
```

### 5. Validation
```bash
python src/validation/expectations_suite.py
pytest tests/
```

## 🎨 Visualization

### Jupyter Analysis
- Data quality distribution
- Matching effectiveness
- Vendor patterns
- Cost center analysis
- Text analysis and word frequency

### dbt Docs
- Model lineage
- Column documentation
- Test results
- Source freshness

## 🚀 Production Deployment

### Scheduling with dbt Cloud/Airflow
1. **Daily ETL**: Run pandas cleaning scripts
2. **dbt Refresh**: Execute full model refresh
3. **Quality Checks**: Validate data quality metrics
4. **Alerts**: Notify on quality degradation

### Performance Optimization
- **Incremental Models**: Process only new/changed data
- **Partitioning**: Organize by date/cost center
- **Indexing**: Optimize DuckDB queries
- **Caching**: Store intermediate results

## 🔍 Troubleshooting

### Common Issues
1. **DuckDB Lock**: Ensure no other processes are using the database
2. **Memory Issues**: Reduce batch sizes or increase system memory
3. **Path Issues**: Use absolute paths in configuration
4. **dbt Compilation**: Check SQL syntax and model references

### Debug Commands
```bash
# Check dbt compilation
dbt compile

# Debug dbt connection
dbt debug

# Run with verbose logging
dbt run --log-level debug

# Check data lineage
dbt ls --select +model_name
```

## 📚 Next Steps

1. **Implement Advanced Matching**: Machine learning models for similarity
2. **Real-time Processing**: Stream processing for live data
3. **API Integration**: Connect to actual SAP ARIBA system
4. **Advanced Analytics**: Spend analysis and supplier performance
5. **UI Dashboard**: Business user interface for match review

---

**Ready for the next phase of fuzzy matching and analytics!** 🎯
