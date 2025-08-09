# Seeds

This directory contains CSV files that will be loaded as seed data into the DuckDB database.

Seeds are typically used for:
- Reference data (e.g., country codes, status mappings)
- Small lookup tables
- Test data for development

## Usage

To load seeds:
```bash
dbt seed
```

To load specific seeds:

```bash
dbt seed --select seed_name
```

## Files

Currently empty - seeds will be added as needed.