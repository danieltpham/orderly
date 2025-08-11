{% docs __overview__ %}

# **Orderly – PO Line Item ETL dbt Pipeline**  
⚠️ *Disclaimer: This project uses synthetic data only. No real or proprietary data is used.*

## **Executive Summary**  
Orderly solves the challenge of transforming free-form, manually entered purchase order (PO) data into clean, analytics-ready outputs.  

With no master SKU list available, the process relies on **human-in-the-loop (HITL) curation** for product naming, complemented by fuzzy matching for vendor identification. SKUs are standardised from scratch, while vendors are reconciled against a reference list.  

The pipeline follows a **medallion architecture** — *Bronze* (raw capture), *Silver* (standardised and validated), and *Gold* (analytics-ready) — using curated SKU/vendor seed files from an external HITL process. This ensures outputs are consistent, accurate, and fully traceable for reporting and analysis.

---

## **Problem Context**  
Manually keyed purchase orders introduce multiple complexities:  

- **No standard SKU catalogue** — products must be curated and approved before standardisation.  
- **Highly inconsistent item names** — varied wording, abbreviations, and incomplete descriptions.  
- **Vendor aliases** — multiple naming variations need reconciliation to a reference dataset.  
- **Inconsistent data attributes** — prices, units, and categories may vary or be incorrect.  

Automated fuzzy matching alone cannot fully resolve these issues — **targeted human review is essential** to build a reliable, canonical SKU list.

---

## **Workflow Overview & Impact**

1. **Bronze – Raw Capture**  
   - Ingests PO line items exactly as entered in source systems.  
   - Retains all original text and fields for audit and traceability.  

2. **External HITL Curation (outside dbt)**  
   - Curates and approves canonical SKU names from raw descriptions.  
   - Reconciles vendor aliases with a reference dataset.  
   - Produces SKU and vendor seed files for the dbt pipeline.  

3. **Silver – Standardised & Validated Data**  
   - Applies SKU and vendor mappings from seed files.  
   - Normalises units, sizes, and naming conventions.  
   - Flags anomalies, duplicates, and structural issues for review.  

4. **Gold – Analytics-Ready Models**  
   - Structures curated data into fact and dimension models.  
   - Enables robust spend analysis, vendor performance tracking, and SKU-level insights.  

---

## **Model Architecture**

### **Bronze Layer – Raw Data Ingestion**
- **`raw_orders`** – Unprocessed PO line items from JSON files with original formatting
- **`raw_vendor_master`** – Reference vendor list for fuzzy matching and reconciliation  
- **`raw_cost_centres`** – Organizational cost centre hierarchy with country mappings
- **`raw_exchange_rates`** – Historical USD/AUD exchange rate data for currency conversion

### **Staging Layer – Data Preparation**  
- **`stg_orders`** – Cleaned and validated order data with quality flags and non-product detection
- **`stg_orders_fuzz_scores`** – Fuzzy matching scores for SKU names and vendor reconciliation
- **`stg_orders_with_flags`** – Quality validation flags for missing SKUs, name mismatches, and vendor issues
- **`stg_exchange_rates`** – Gap-filled daily exchange rates with linear interpolation for missing dates

### **Silver Layer – Standardised & Validated**
- **`silver_orders_valid`** – High-quality orders with successful SKU/vendor matching (>80% confidence)
- **`silver_orders_exceptions`** – Orders with data quality issues requiring review or manual intervention
- **`silver_orders_nonproduct`** – Non-product items (services, fees, taxes) separated from inventory analysis
- **`silver_cost_centres`**, **`silver_vendor_master`**, **`silver_exchange_rates`** – Clean reference data

### **Gold Layer – Analytics-Ready Models**
**Dimensions:**
- **`dim_product`** – SKU master with categorization, brand classification, and approval status
- **`dim_vendor`** – Vendor profiles with tier classification and category groupings  
- **`dim_cost_centre`** – Organizational hierarchy with geographic attributes
- **`dim_date`** – Comprehensive date dimension with fiscal periods and integrated exchange rates

**Facts:**
- **`fct_order_line`** – Core transactional fact for spend analysis with multi-currency support
- **`fct_data_quality`** – Exception tracking and data quality monitoring for continuous improvement
- **`fct_price_variance`** – Price anomaly detection across vendors, SKUs, and time periods

---

## **Key Design Principles**  
Separating **raw capture**, **human curation**, and **transformation** enables:  

- Accuracy where automated matching would fail.  
- A trustworthy reference for SKUs and vendors from messy manual inputs.  
- A complete audit trail from raw PO lines to analytical outputs.  
- Scalable procurement analytics without embedding human review directly in transformation logic.

---

## **Getting Started**
1. **Explore the Data Quality Models** – Start with `fct_data_quality` to understand exception patterns
2. **Review Key Fact Tables** – `fct_order_line` provides the core transactional data for analysis
3. **Understand the Dimensions** – Use `dim_product`, `dim_vendor`, and `dim_cost_centre` for meaningful categorization
4. **Monitor Data Health** – Track `silver_orders_exceptions` for ongoing data quality improvements

{% enddocs %}
