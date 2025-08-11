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
        "Aim": [
            "Ingest raw files into Bronze layer.",
            "Enable schema inspection before transformations."
        ],
        "Key Challenges": [
            "Schema drift across sources.",
            "Mixed units/currencies; vendor aliasing."
        ],
        "Achievements": [
            "Unified ingestion to DuckDB.",
            "Basic normalization and type inference.",
            "Lineage tracking for audits."
        ]
    },
    "hitl-auto": {
        "Aim": ["Auto-approve clear matches to reduce manual load."],
        "Key Challenges": ["Avoid false positives near threshold."],
        "Achievements": ["Fuzzy matching with set thresholds.", "Token normalization before matching."]
    },
    "hitl-review": {
        "Aim": ["Human validation for ambiguous matches."],
        "Key Challenges": ["Provide clear review signals.", "Ensure version control on seed updates."],
        "Achievements": ["Review queue with status changes.", "Audit trail for every decision."]
    },
    "silver": {
        "Aim": ["Produce analytics-grade records."],
        "Key Challenges": ["Unit and currency normalization.", "Referential integrity checks."],
        "Achievements": ["Deterministic conversion rules.", "QC exception reporting."]
    },
    "gold": {
        "Aim": ["Expose a clean star schema for BI."],
        "Key Challenges": ["Choose correct grain and keys."],
        "Achievements": ["Dimensional modelling.", "Surrogate keys for SCD-ready dims."]
    },
}