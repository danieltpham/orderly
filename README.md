# 🧾 Orderly – PO Line Item ETL Pipeline

_⚠️ Disclaimer: This project uses **synthetic data only**. No real or proprietary data is used._

## 🎯 Problem Context
In large orgs or systems ingesting manual purchase entries:
- Item names are unstructured and vary across orders
- Similar products are priced differently or misclassified
- Vendor names come in many alias forms
- Orders are missing key fields or include malformed text

**Orderly** solves this by:
- Normalising item names, sizes, and units
- Matching line items to canonical SKUs via fuzzy logic
- Cleaning vendor aliases and cost centres
- Flagging duplicates, outliers, and incomplete entries

## 🛠️ Key Skills Demonstrated

- AI Prompt Engineering (structured generation of messy PO records)
- ETL Design with Pydantic, pandas, DuckDB
- SKU + Vendor Fuzzy Matching (fuzzywuzzy, token sort, regex)
- Data Quality Validation (Great Expectations)
- Analytics Layer with dbt
- Modular pipeline with match logs, issue flags, config-based logic

```
orderly/
├── README.md
├── requirements.txt
├── orderly.duckdb                # Embedded DB file (auto-generated)
│
├── data/
│   ├── raw/                      # Raw AI-generated or scraped input files
│   │   ├── line_items.jsonl
│   │   ├── vendor_master.csv
│   │   ├── sku_catalog.csv
│   │   └── cost_centres.csv
│   │
│   └── prompts/
│       └── product_prompt.txt    # AI prompt used to generate fake line items
│
├── src/
│   ├── config.py                 # Paths, DuckDB schema names, constants
│   ├── bronze_layer.py          # Reads raw files → bronze DuckDB tables
│   ├── clean_and_match.py       # Cleans + fuzzy matches → silver tables
│   └── utils/
│       └── matching_utils.py     # Fuzzy logic, token helpers, etc.
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml             # DuckDB dbt profile
│   ├── models/
│   │   ├── silver/
│   │   │   ├── line_items_clean.sql
│   │   │   ├── vendor_dim.sql
│   │   │   └── sku_dim.sql
│   │   │
│   │   └── gold/
│   │       ├── fact_orders.sql
│   │       ├── spend_cube.sql
│   │       └── anomaly_log.sql
│   │
│   └── seeds/
│       └── unit_standardisation.csv  # Optional helper maps for dbt
│
├── notebooks/
│   └── explore_sample_data.ipynb     # For quick inspection / demo
│
└── tests/
    └── great_expectations/          # Optional data quality checks
```

## Layer Summary

| Layer      | Tools Used              | Location                                  |
| ---------- | ----------------------- | ----------------------------------------- |
| **Raw**    | Files (JSONL, CSV)      | `data/raw/`                               |
| **Bronze** | Python → DuckDB         | `src/bronze_layer.py` → `bronze.*` tables |
| **Silver** | Pandas + Fuzzy Matching | `src/clean_and_match.py` → `silver.*`     |
| **Gold**   | dbt + DuckDB            | `dbt/models/gold/`                        |
