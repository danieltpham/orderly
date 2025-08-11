# Hidden Canonical Data - Ground Truth Reference

> **⚠️ IMPORTANT**: This folder contains the "ground truth" canonical data that represents the ideal, standardized SKU and brand names, generated from the data simulation script using advanced fuzzy matching algorithms. This data is **NOT** used as direct input to the DBT pipeline but serves as the reference standard for evaluating and improving the Silver layer NLP matching algorithms.

## Purpose & Context

This hidden canonical data represents what we **aspire to achieve** through our Silver layer data processing pipeline. In practice, this level of standardization is not available as a direct data source and must be inferred through:

1. **Human manual entry** and curation
2. **Data denoising and wrangling** algorithms  
3. **Human-in-the-loop (HITL) review** processes
4. **Iterative refinement** based on business domain expertise
5. **Advanced fuzzy matching** with RapidFuzz algorithms

The goal of our Silver layer NLP fuzzy matching pipeline is to get as close as possible to this canonical representation without having direct access to it.

## Generation Logic & Algorithms

### Brand Canonicalization Process
Our canonical brand generation uses a sophisticated multi-tier matching approach:

1. **Source of Truth**: Global `BRANDS` list defines canonical brand names
   - `["TechFlow", "ViewMaster", "ConnectPro", "DataVault", "OfficeLink", "ProConnect"]`

2. **Fuzzy Matching Pipeline**:
   ```
   Raw Brand → Abbreviation Check → Exact Match → RapidFuzz (75% threshold) → Canonical Brand
   ```

3. **Explicit Abbreviation Handling**:
   - `TF` → `TechFlow`
   - `DV` → `DataVault` 
   - `VM` → `ViewMaster`
   - `CP` → `ConnectPro`
   - `OL` → `OfficeLink`
   - `PC` → `ProConnect`

4. **Alias Collection**: All variations found in source data are preserved as `brand_aliases`

### SKU Versioning & Lifecycle Management
Our SKU catalog implements sophisticated duplicate handling and versioning:

1. **Duplicate Detection**: Multiple instances of same SKU ID are preserved
2. **Data Conflict Warnings**: Immediate alerts for inconsistent data across duplicates
3. **Version Control System**:
   - **Previous Versions**: `is_active = False`, superseded date tracking
   - **Current Version**: `is_active = True`, latest update timestamp
   - **Timeline Simulation**: 30-day intervals between version releases

4. **Smart Date Management**:
   ```
   Version 1: created=2025-01-01, updated=2025-01-31 (inactive)
   Version 2: created=2025-01-31, updated=2025-03-02 (inactive)  
   Version 3: created=2025-03-02, updated=2025-07-01 (active)
   ```

## Files Overview

### `canonical_brands.csv` 
**The definitive brand master reference with intelligent alias mapping**

| Column | Description | Example |
|--------|-------------|---------|
| `brand_id` | Unique brand identifier | `BRD001` |
| `canonical_brand_name` | Standardized brand name from global BRANDS | `ConnectPro` |
| `brand_aliases` | Auto-discovered variations via fuzzy matching | `["Connect Pro", "CP", "connectpro"]` |
| `is_active` | Whether brand is currently active | `True` |
| `created_date` | Initial record creation | `2025-01-01` |
| `updated_date` | Last modification date | `2025-07-01` |

**Generation Algorithm:**
- **Canonical Source**: Uses predefined `BRANDS` list as authoritative source
- **Fuzzy Matching**: RapidFuzz with 75% similarity threshold to find variations
- **Abbreviation Priority**: Explicit mappings override fuzzy matches
- **Alias Preservation**: All discovered variations stored for reference

**Key Insights:**
- **6 canonical brands** from global BRANDS definition
- Brands include: `ConnectPro`, `DataVault`, `OfficeLink`, `ProConnect`, `TechFlow`, `ViewMaster`
- **Auto-discovered aliases** from source data (e.g., `DV`, `DatVault`, `DatVlt` → `DataVault`)
- **Case-insensitive matching** with proper canonical casing

### `canonical_sku_catalog.csv`
**The definitive product catalog with version control and brand standardization**

| Column | Description | Example |
|--------|-------------|---------|
| `sku_id` | Business SKU identifier | `SKU001` |
| `canonical_description` | Standardized product name | `TechFlow Wireless Keyboard Black` |
| `canonical_brand` | **Mapped to canonical BRANDS** | `TechFlow` |
| `canonical_unit` | Standard unit of measure | `each` |
| `canonical_size` | Standardized size/dimension | `24 inch`, `6 ft` |
| `category` | Auto-inferred product category | `Keyboards`, `Monitors`, `Cables` |
| `is_active` | **Version control flag** | `True`/`False` |
| `created_date` | **Version-aware creation date** | `2025-01-01` |
| `updated_date` | **Lifecycle management timestamp** | `2025-07-01` |

**Generation Algorithm:**
- **Brand Mapping**: All brands fuzzy-matched to canonical BRANDS list
- **Duplicate Preservation**: Multiple instances of same SKU preserved as versions
- **Version Control**: Previous versions marked inactive, latest remains active
- **Data Conflict Detection**: Warnings for inconsistent duplicate data
- **Category Inference**: Keyword-based categorization from descriptions
- **Chronological Sorting**: Output sorted by SKU ID for consistency

**Advanced Features:**
- **Complete Instance Preservation**: All duplicates included (not just unique SKUs)
- **Brand Consistency**: Variations like `techflow`, `TF` → `TechFlow`
- **Version Timeline**: 30-day intervals between SKU versions
- **Quality Monitoring**: Immediate feedback on data conflicts

## Usage in Data Pipeline

### ❌ What This Data Is NOT Used For:
- Direct DBT source tables
- Automated seed files 
- Production lookup tables
- Real-time data matching

### ✅ What This Data IS Used For:
- **Algorithm evaluation**: Measuring fuzzy match accuracy against ground truth
- **Quality assessment**: Identifying gaps in current matching logic
- **Training data**: Human reviewers can reference for difficult cases
- **Validation**: Testing new matching algorithms before deployment
- **Documentation**: Understanding business requirements for standardization

## Silver Layer Matching Strategy

Our Silver layer aims to replicate this canonical standardization through:

### 1. **Advanced Brand Matching Pipeline**
```
Raw Vendor Names → Abbreviation Check → Fuzzy Matching (RapidFuzz) → Canonical Brand Assignment
├── "officelink" → exact match → "OfficeLink"
├── "DatVlt" → fuzzy match (85%) → "DataVault" 
├── "TF" → abbreviation map → "TechFlow"
└── "ViewMaster Corp" → fuzzy match (90%) → "ViewMaster"
```

**Matching Hierarchy:**
1. **Exact Abbreviation Match** (highest priority)
2. **Exact String Match** (case-insensitive)  
3. **RapidFuzz Similarity** (75% threshold)
4. **Manual Review Queue** (below threshold)

### 2. **SKU Description Standardization with Versioning**
```
Raw Descriptions → Brand Mapping → Version Control → Canonical Description
├── "wireless keyboard black" + "techflow" → "TechFlow Wireless Keyboard Black" (v1)
├── "TechFlow Wireless Keyboard - Black" → "TechFlow Wireless Keyboard Black" (v2, active)
└── Previous versions marked inactive with supersession dates
```

### 3. **Enhanced Quality Thresholds**
- **High Confidence**: Fuzzy score ≥ 75% → Automatic assignment
- **Medium Confidence**: Fuzzy score 60-74% → HITL review queue
- **Low Confidence**: Fuzzy score < 60% → Manual research required
- **Abbreviation Matches**: 100% confidence → Direct mapping

### 4. **Data Quality Monitoring**
- **Duplicate Detection**: Real-time alerts for conflicting SKU data
- **Brand Consistency**: Automatic mapping to canonical brands
- **Version Tracking**: Complete audit trail of SKU evolution
- **Conflict Resolution**: Systematic handling of data inconsistencies

## Data Quality Standards

### Brand Standardization Rules:
- **Canonical Source**: All brands must map to predefined `BRANDS` list
- **Fuzzy Matching**: RapidFuzz with 75% similarity threshold
- **Abbreviation Priority**: Explicit mappings (`TF` → `TechFlow`) override fuzzy matches
- **Consistent casing**: "OfficeLink" not "officelink" 
- **Alias Preservation**: All discovered variations stored for reference
- **No orphaned brands**: Poor matches flagged for manual review

### SKU Description Rules:
- **Brand consistency**: Mapped to canonical brands via fuzzy matching
- **Version control**: Multiple instances preserved with lifecycle management
- **Standard terminology**: "LED Monitor" not "led monitor"
- **Complete specifications**: Include relevant technical details
- **Category inference**: Automatic categorization from description keywords
- **Audit trail**: Complete history of SKU changes with timestamps

### Advanced Quality Features:
- **Duplicate preservation**: All SKU instances included in canonical data
- **Conflict detection**: Immediate warnings for inconsistent duplicate data
- **Version lifecycle**: Previous versions marked inactive with supersession dates
- **Chronological ordering**: 30-day intervals between SKU version releases
- **Data completeness**: All fields populated with defaults where missing

## Future Evolution

This canonical data will evolve through:

1. **Algorithm Enhancement**: Continuous improvement of fuzzy matching thresholds
2. **Abbreviation Expansion**: Adding new brand abbreviations to explicit mapping
3. **Version Management**: Refined SKU lifecycle and versioning strategies  
4. **Quality Monitoring**: Enhanced conflict detection and resolution algorithms
5. **Brand Consolidation**: Systematic handling of vendor mergers and acquisitions
6. **Category Evolution**: Dynamic expansion of product categorization rules
7. **Performance Optimization**: Faster matching algorithms and batch processing
8. **HITL Integration**: Seamless human-in-the-loop feedback incorporation