# Training Dataset Generator

This module generates training datasets for fuzzy matching and data cleaning models using **Pydantic AI** with clean product SKUs and messy user-input line items.

## Purpose
Create realistic datasets for training models to:
- Match messy user inputs to clean product records
- Clean and standardize product descriptions
- Handle real-world data quality issues in procurement

## Built with Pydantic AI
- **Type-safe**: Structured responses guaranteed by Pydantic validation
- **Model-agnostic**: Supports OpenAI, Anthropic, Gemini, and more
- **Validated output**: Automatic retry if validation fails
- **Clean architecture**: Leverages Python's familiar patterns

## Data Model

### ProductSKU (Source of Truth)
Clean, standardized product records:
- `sku_id`: Unique identifier
- `brand`: Official brand name (standardized)
- `product_name`: Clean product name
- `variant`: Product variant details
- `category`: Product category
- `unit`: Standard unit of measure
- `size`: Standardized size/dimensions

### LineItem (Messy User Input)
Real-world messy entries that should match to ProductSKUs:
- `sku_id`: Reference to the clean SKU (ground truth)
- `item_description`: Raw description as entered by user
- `brand`: Brand as entered (inconsistent/missing)
- `quantity`: Quantity ordered
- `unit`: Unit as entered (may be wrong/missing)
- `size`: Size as entered (inconsistent/missing)
- `data_quality_flag`: Quality classification

### ProcurementDataset
Structured output containing both SKUs and line items with automatic validation.

## Data Quality Types
- **clean**: Exact match to canonical description
- **fuzzy**: Abbreviations, typos, missing words, case issues
- **html**: HTML artifacts and encoding issues
- **non_product**: Fees, services, taxes (not actual products)

## How to Run

1. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

2. Set your API key (supports multiple providers):
   ```powershell
   # OpenAI (default)
   $env:OPENAI_API_KEY="sk-your-api-key-here"
   
   # Or Anthropic
   $env:ANTHROPIC_API_KEY="your-api-key-here"
   
   # Or other supported models
   ```

3. Generate dataset:
   ```powershell
   python generate/generate_data.py
   ```

4. Output files:
   ```
   data/product_skus.jsonl    # Clean SKUs (source of truth)
   data/line_items.jsonl      # Messy line items with SKU references
   ```

## Pydantic AI Agent

The generator uses a Pydantic AI agent with:
- **Structured output**: `ProcurementDataset` with automatic validation
- **Intelligent prompting**: System prompt optimized for data generation
- **Error handling**: Automatic retry on validation failures
- **Type safety**: Full type checking throughout

```python
from pydantic_ai import Agent
from models.schema import ProcurementDataset

procurement_agent = Agent(
    'openai:gpt-4o',  # or 'anthropic:claude-3-sonnet'
    output_type=ProcurementDataset,
    system_prompt=load_prompt(),
)

result = await procurement_agent.run(user_query)
dataset = result.output  # Guaranteed to be valid ProcurementDataset
```

## Training Data Structure

Each messy line item is linked to its clean SKU for supervised learning:

**Clean SKU:**
```json
{
  "sku_id": "SKU001",
  "brand": "TechFlow",
  "product_name": "Wireless Keyboard",
  "variant": "Compact Black",
  "category": "Keyboards",
  "unit": "each",
  "size": "Full Size"
}
```

**Messy Line Items (all reference SKU001):**
```json
{
  "sku_id": "SKU001",
  "item_description": "techflow wireless kb compact blk",
  "brand": "techflow",
  "quantity": 5,
  "unit": "each", 
  "size": null,
  "data_quality_flag": "fuzzy"
}
```

## Realistic Data Issues Included
- **Brand variations**: "TechFlow" → "TF", "techflow"
- **Abbreviations**: "keyboard" → "kb", "monitor" → "mon"
- **Missing information**: Incomplete descriptions, missing units
- **Typos**: "keybord", "monitr", "wireles"
- **Case inconsistencies**: lowercase, UPPERCASE, Mixed
- **Size variations**: "24in" vs "24 inch" vs "24\""
- **HTML artifacts**: "&quot;", "&#x2013;", "&lt;br&gt;"
- **Wrong units**: "pack" instead of "each"

## Use Cases
- Training fuzzy matching algorithms
- Building product name standardization models
- Testing data cleaning pipelines
- Benchmarking entity resolution systems
- Creating synthetic procurement datasets

## Dataset Statistics
- ~50 clean ProductSKUs
- ~500 messy LineItems (3-7 per SKU)
- ~90% actual products, ~10% non-product entries
- Multiple quality levels per SKU for robust training
- Automatic validation and consistency checking

## License
MIT
