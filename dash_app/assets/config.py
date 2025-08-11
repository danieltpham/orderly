# --- Step definitions ---
STEPS = [
    ("bronze", "Bronze", "Raw data ingestion"),
    ("hitl-auto", "HITL – Auto Curate", "Automated SKU matching"),
    ("hitl-review", "HITL – Review", "Manual review of seeds"),
    ("silver", "Silver", "Wrangled tables with QC"),
    ("gold", "Gold", "Dimensional modeling"),
]

# --- Step narrative content ---
STEP_CONTENT = {
    "bronze": {
        "Purpose": [
            "Ingest raw files into Bronze layer.",
            "Enable schema inspection before transformations."
        ],
        "Key Challenges": [
            "Schema drift across sources.",
            "Mixed units/currencies; vendor aliasing."
        ],
        "What We Did": [
            "Unified ingestion to DuckDB.",
            "Basic normalization and type inference.",
            "Lineage tracking for audits."
        ]
    },
    "hitl-auto": {
        "Purpose": ["Auto-approve clear matches to reduce manual load."],
        "Key Challenges": ["Avoid false positives near threshold."],
        "What We Did": ["Fuzzy matching with set thresholds.", "Token normalization before matching."]
    },
    "hitl-review": {
        "Purpose": ["Human validation for ambiguous matches."],
        "Key Challenges": ["Provide clear review signals.", "Ensure version control on seed updates."],
        "What We Did": ["Review queue with status changes.", "Audit trail for every decision."]
    },
    "silver": {
        "Purpose": ["Produce analytics-grade records."],
        "Key Challenges": ["Unit and currency normalization.", "Referential integrity checks."],
        "What We Did": ["Deterministic conversion rules.", "QC exception reporting."]
    },
    "gold": {
        "Purpose": ["Expose a clean star schema for BI."],
        "Key Challenges": ["Choose correct grain and keys."],
        "What We Did": ["Dimensional modelling.", "Surrogate keys for SCD-ready dims."]
    },
}