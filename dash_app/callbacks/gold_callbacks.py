from typing import Callable, Dict, List, OrderedDict
from dash.dependencies import Input, Output, State
from dash import callback, html
import pandas as pd
import duckdb
from pathlib import Path
from components.steps.silver import get_silver_data, create_table_component
import dash_mantine_components as dmc

from dash_app.components.steps.analytics_func import *


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

# Hard-code business questions here (titles and matching SQL files).
# The SQL files must exist in your repo's sql/ folder.
# Example structure:
#   project_root/
#     sql/
#       top_vendors_by_spend.sql
#       category_spend_last_90d.sql
#       sku_velocity_by_store.sql
#
# Adjust file names to match your existing sql folder.
from collections import OrderedDict

QUESTIONS: "OrderedDict[str, Dict[str, str]]" = OrderedDict([
    ("q1_procurement_spend", {
        "label": "What is our overall procurement spend by category, order volume, and quantity?",
        "sql_file": "1_procurement_spend_analysis.sql",
        "summary": "summary_procurement_spend"
    }),
    ("q2_data_quality", {
        "label": "Where are the key data quality issues and how many exceptions are we seeing?",
        "sql_file": "2_data_quality_monitoring.sql",
        "summary": "summary_data_quality"
    }),
    ("q3_price_variance", {
        "label": "Which SKUs show the highest price variance and potential savings opportunities?",
        "sql_file": "3_price_variance_analysis.sql",
        "summary": "summary_price_variance"
    }),
    ("q4_multi_region", {
        "label": "How is procurement performance varying across regions or cost centres?",
        "sql_file": "4_multi_region_performance.sql",
        "summary": "summary_multi_region_performance"
    }),
    ("q5_seasonal_trends", {
        "label": "What seasonal trends are visible in procurement spend over time?",
        "sql_file": "5_seasonal_trends.sql",
        "summary": "summary_seasonal_trends"
    }),
    ("q6_vendor_performance", {
        "label": "Which vendors are performing best in terms of spend, pricing, and relationship metrics?",
        "sql_file": "6_vendor_performance.sql",
        "summary": "summary_vendor_performance"
    }),
    ("q7_executive_dashboard", {
        "label": "What are the key executive KPIs for the current month, previous month, and year-to-date?",
        "sql_file": "7_executive_dashboard.sql",
        "summary": "summary_executive_dashboard"
    }),
])


# Paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = PROJECT_ROOT / "notebooks" / "sql"
DB_PATH = PROJECT_ROOT / "warehouse" / "orderly.duckdb"

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def _run_sql(sql_text: str) -> pd.DataFrame:
    if not DB_PATH.exists():
        # Return empty DF with a helpful note if DB is missing
        return pd.DataFrame({"_error": [f"Database not found: {DB_PATH}"]})
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as con:
            return con.execute(sql_text).df()
    except Exception as e:
        return pd.DataFrame({"_error": [f"SQL execution error: {e}"]})

def _fmt_num(x) -> str:
    try:
        # human-ish formatting
        return f"{x:,.2f}"
    except Exception:
        return str(x)

# -------------------------------------------------------------------
# Summary generators (no plots; text only)
# Each returns a list[str] that we’ll render on the right panel.
# These names mirror the notebook’s “print after plots” sections:
# procurement spend, data quality, price variance, multi‑region,
# seasonal trends, vendor performance, and executive KPIs.
# -------------------------------------------------------------------

# Registry so QUESTION metadata can reference summaries by name
SUMMARY_FUNCS: Dict[str, Callable[[pd.DataFrame], List[str]]] = {
    "summary_procurement_spend": summary_procurement_spend,
    "summary_data_quality": summary_data_quality,
    "summary_price_variance": summary_price_variance,
    "summary_multi_region_performance": summary_multi_region_performance,
    "summary_seasonal_trends": summary_seasonal_trends,
    "summary_vendor_performance": summary_vendor_performance,
    "summary_executive_dashboard": summary_executive_dashboard,
}

def _load_sql(sql_filename: str) -> str:
    sql_path = SQL_DIR / sql_filename
    if not sql_path.exists():
        return f"-- ERROR: SQL file not found: {sql_path}"
    try:
        return sql_path.read_text(encoding="utf-8")
    except Exception as e:
        return f"-- ERROR reading SQL file {sql_path.name}: {e}"

def register_gold_callbacks(app):
    @app.callback(
        Output("gold-sql-code", "children"),
        Output("gold-summary-list", "children"),
        Input("gold-question-dropdown", "value"),
        prevent_initial_call=True,
    )
    def _update_panels(question_key: str):
        if not question_key:
            return "-- Select a question to view SQL", [dmc.ListItem("No question selected.")]

        meta = QUESTIONS.get(question_key)
        if not meta:
            return f"-- ERROR: Unknown question: {question_key}", [dmc.ListItem("Unknown question key.")]

        # Load SQL from file (left panel)
        sql_text = _load_sql(meta["sql_file"])

        # If SQL is an error comment, show that and skip execution
        if sql_text.strip().startswith("-- ERROR"):
            return sql_text, [dmc.ListItem(sql_text)]

        # Run SQL -> DF
        df = _run_sql(sql_text)

        # Build summary (right panel)
        summary_fn_name = meta.get("summary", "")
        summary_fn = SUMMARY_FUNCS.get(summary_fn_name, None)

        if summary_fn is None:
            # Fallback generic summary
            items = [
                f"Rows returned: {len(df)}",
            ]
            if "_error" in df.columns:
                items = [df["_error"].iloc[0]]
            return sql_text, [dmc.ListItem(t) for t in items]

        lines = summary_fn(df)
        return sql_text, [dmc.ListItem(line) for line in lines]