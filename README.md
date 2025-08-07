# 🧾 Orderly – PO Line Item ETL Pipeline

**Orderly** is a modular ETL project focused on cleaning and structuring messy **purchase order (PO) line items**. It simulates real-world order data where item descriptions are inconsistent, vendors vary, and price anomalies go undetected. Built for showcasing **realistic, production-grade data wrangling** in a data science portfolio.

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
