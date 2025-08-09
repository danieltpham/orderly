# Gold Layer Recommendations for Orderly Data Platform

Based on analysis of your silver layer tables, here are my recommendations for gold layer dimensional modeling to support analytics and reporting needs.

## Current Silver Layer Analysis

Your silver layer provides clean, validated data with excellent data quality management:

### Reference Tables (Dimension Candidates)
- **`silver_cost_centres`**: Clean cost center master data (3 columns)
- **`silver_vendor_master`**: Clean vendor master data (2 columns)  
- **`silver_exchange_rates`**: Daily USD/AUD exchange rates with interpolation

### Transaction Tables (Fact Candidates)
- **`silver_orders_valid`**: High-quality order line items (~20 columns)
- **`silver_orders_exceptions`**: Orders with data quality issues for monitoring
- **`silver_orders_nonproduct`**: Non-product items (services, fees, taxes)

## Recommended Gold Layer Design

### Core Dimensions

#### 1. `dim_date` ⭐ **HIGH PRIORITY**
```yaml
Purpose: Time dimension for temporal analysis
Grain: One row per day
Source: Generated dimension table
Key Attributes:
  - date_key (YYYYMMDD)
  - date_actual
  - year, quarter, month, week
  - day_of_week, day_of_month
  - is_weekend, is_holiday
  - fiscal_year, fiscal_quarter (if applicable)
```

#### 2. `dim_cost_centre` ⭐ **HIGH PRIORITY**
```yaml
Purpose: Organizational hierarchy and geographic analysis
Grain: One row per cost centre
Source: silver_cost_centres
Key Attributes:
  - cost_centre_key (surrogate key)
  - cost_centre_id (business key)
  - cost_centre_name
  - country_code
  - region (derived from country_code)
  - is_active
```

#### 3. `dim_vendor` ⭐ **HIGH PRIORITY** 
```yaml
Purpose: Vendor analysis and supplier management
Grain: One row per vendor
Source: silver_vendor_master + enrichment
Key Attributes:
  - vendor_key (surrogate key)
  - vendor_id (business key)
  - vendor_name
  - vendor_category (derived/enriched)
  - country_code (if available)
  - payment_terms (if available)
  - vendor_tier (strategic/preferred/standard)
```

#### 4. `dim_product` ⭐ **HIGH PRIORITY**
```yaml
Purpose: Product catalog and SKU management
Grain: One row per SKU
Source: ref_sku_names seed + silver_orders_valid
Key Attributes:
  - product_key (surrogate key)
  - sku_id (business key)
  - canonical_name
  - product_category (derived from name)
  - product_subcategory
  - unit_of_measure (if standardized)
  - source_type (approved_seed/auto_ref)
  - is_active
```

#### 5. `dim_currency` 🔶 **MEDIUM PRIORITY**
```yaml
Purpose: Multi-currency support and exchange rate management
Grain: One row per currency
Source: Static/reference data
Key Attributes:
  - currency_key
  - currency_code
  - currency_name
  - symbol
  - decimal_places
```

### Core Facts

#### 1. `fct_order_line` ⭐ **HIGH PRIORITY**
```yaml
Purpose: Core transactional fact for order analysis
Grain: One row per order line item
Source: silver_orders_valid
Key Dimensions:
  - date_key (order_date)
  - delivery_date_key  
  - cost_centre_key
  - vendor_key
  - product_key
  - currency_key

Measures:
  - quantity
  - unit_price
  - line_total_amount
  - line_total_usd (converted)
  - line_total_aud (converted)
  
Attributes:
  - order_id
  - line_number
  - requisitioner
  - approval_status
  - source_filename
```

#### 2. `fct_exchange_rate` 🔶 **MEDIUM PRIORITY**
```yaml
Purpose: Daily exchange rates for currency conversion
Grain: One row per currency pair per day
Source: silver_exchange_rates
Key Dimensions:
  - date_key
  - from_currency_key
  - to_currency_key

Measures:
  - exchange_rate
  
Attributes:
  - rate_source
  - confidence_level (derived from rate_source)
```

### Analytical & Monitoring Facts

#### 3. `fct_data_quality` 🔶 **MEDIUM PRIORITY**
```yaml
Purpose: Data quality monitoring and exception tracking
Grain: One row per exception record
Source: silver_orders_exceptions
Key Dimensions:
  - date_key (order_date)
  - cost_centre_key
  - product_key (if available)

Measures:
  - exception_count (always 1)
  - fuzz_score
  - vendor_fuzz_score

Attributes:
  - exception_type
  - flag_missing_in_seed
  - flag_name_mismatch  
  - flag_vendor_mismatch
  - order_id
  - line_number
```

#### 4. `fct_price_variance` 🔵 **LOW PRIORITY / FUTURE**
```yaml
Purpose: Price anomaly detection and variance analysis
Grain: One row per SKU per time period with price variance
Source: silver_orders_valid (with analytical calculations)
Key Dimensions:
  - date_key
  - product_key
  - vendor_key
  - cost_centre_key

Measures:
  - avg_unit_price
  - min_unit_price
  - max_unit_price
  - price_variance
  - order_count
  - total_quantity
```

## Implementation Priority

### Phase 1: Core Analytics Foundation
1. `dim_date` - Generate comprehensive date dimension
2. `dim_cost_centre` - Direct mapping from silver layer
3. `dim_vendor` - Direct mapping with basic enrichment
4. `dim_product` - SKU master with categorization
5. `fct_order_line` - Core transactional fact

### Phase 2: Enhanced Analytics
6. `dim_currency` - Multi-currency support
7. `fct_exchange_rate` - Exchange rate fact
8. `fct_data_quality` - Quality monitoring

### Phase 3: Advanced Analytics
9. `fct_price_variance` - Price anomaly detection
10. Additional derived dimensions (product categories, vendor tiers)

## Business Value by Table

| Table | Business Value | Use Cases |
|-------|----------------|-----------|
| `fct_order_line` | 🔥 Critical | Spend analysis, vendor performance, procurement reporting |
| `dim_cost_centre` | 🔥 Critical | Geographic spend analysis, org hierarchy reporting |
| `dim_vendor` | 🔥 Critical | Supplier management, vendor consolidation analysis |
| `dim_product` | 🔥 Critical | Category spend, SKU rationalization, catalog management |
| `dim_date` | 🔥 Critical | Time-series analysis, trend identification, forecasting |
| `fct_data_quality` | 🔥 Important | Data governance, quality monitoring, process improvement |
| `fct_exchange_rate` | 💡 Useful | Multi-currency reporting, financial consolidation |
| `fct_price_variance` | 💡 Useful | Anomaly detection, pricing optimization |

## Key Design Principles Applied

1. **Surrogate Keys**: All dimensions use surrogate keys for SCD handling
2. **Business Keys Preserved**: Original business keys maintained for traceability  
3. **Grain Clarity**: Each table has clearly defined grain
4. **Conformity**: Shared dimensions across facts (date, cost_centre, vendor, product)
5. **Quality Integration**: Data quality metrics embedded in design
6. **Currency Handling**: Multi-currency support with conversion capabilities

## Recommended Next Steps

1. **Start with Phase 1 tables** - they provide 80% of analytical value
2. **Implement SCD Type 2** for vendor and product dimensions if change tracking needed
3. **Create standard currency conversion logic** using exchange rate facts
4. **Build data quality dashboards** using the quality fact table
5. **Consider partitioning** large fact tables by date for performance

This design leverages your excellent silver layer data quality work and provides a solid foundation for analytics, reporting, and data governance needs.
