from typing import List
import pandas as pd
import numpy as np

def _fmt(x):
    try:
        if isinstance(x, (int, np.integer)):
            return f"{x:,}"
        if isinstance(x, float):
            return f"{x:,.2f}"
        return str(x)
    except Exception:
        return str(x)

# 1) Procurement Spend Analysis
def summary_procurement_spend(df: pd.DataFrame) -> List[str]:
    if df is None or len(df) == 0:
        return [">> Results: 0 records found"]

    lines = [f">> Results: {len(df)} records found"]
    if "total_spend_aud" in df.columns:
        lines.append(f">> Total spend (AUD): {_fmt(df['total_spend_aud'].sum())}")
    if "total_orders" in df.columns:
        lines.append(f">> Total orders: {_fmt(df['total_orders'].sum())}")
    if "qty" in df.columns:
        lines.append(f">> Total quantity: {_fmt(df['qty'].sum())}")

    # Top categories by spend
    cat_col = "category" if "category" in df.columns else None
    if cat_col and "total_spend_aud" in df.columns:
        top = (
            df[[cat_col, "total_spend_aud"]]
            .groupby(cat_col, as_index=False).sum()
            .sort_values("total_spend_aud", ascending=False)
            .head(10)
        )
        lines.append("\n>> Top 10 Spending Categories:")
        for i, r in top.reset_index(drop=True).iterrows():
            lines.append(f"  {i+1}. {r[cat_col]} — {_fmt(r['total_spend_aud'])} AUD")
    return lines

# 2) Data Quality Monitoring
def summary_data_quality(df: pd.DataFrame) -> List[str]:
    if df is None or len(df) == 0:
        return [">> Results: 0 exception records found"]

    lines = [f">> Results: {len(df)} exception records found"]
    # Totals / score if available
    if "exception_count" in df.columns:
        lines.append(f">> Total exceptions: {_fmt(df['exception_count'].sum())}")
    if "avg_data_quality_score" in df.columns:
        # Sometimes a single-row metric table
        score = df["avg_data_quality_score"].mean()
        lines.append(f">> Data quality score: {score:.1f}/100")

    # Top exception areas
    key = "exception_type" if "exception_type" in df.columns else ("table_name" if "table_name" in df.columns else None)
    if key and "exception_count" in df.columns:
        top = (
            df[[key, "exception_count"]]
            .groupby(key, as_index=False).sum()
            .sort_values("exception_count", ascending=False)
            .head(10)
        )
        lines.append("\n>> Top 10 Exception Areas:")
        for i, r in top.reset_index(drop=True).iterrows():
            lines.append(f"  {i+1}. {r[key]} — {_fmt(r['exception_count'])}")
    return lines

# 3) Price Variance and Anomaly Detection
def summary_price_variance(df: pd.DataFrame) -> List[str]:
    if df is None or len(df) == 0:
        return [">> Results: 0 products with significant price variance"]

    lines = [f">> Results: {len(df)} products with significant price variance"]

    # Totals and averages
    if "potential_savings_aud" in df.columns:
        lines.append(f">> Total potential savings (AUD): {_fmt(df['potential_savings_aud'].sum())}")
    if "price_variance_pct" in df.columns:
        lines.append(f">> Avg variance (%): {df['price_variance_pct'].mean():.2f}")

    # Top savings opportunities
    name_col = "sku_name" if "sku_name" in df.columns else ("sku_id" if "sku_id" in df.columns else None)
    if name_col and "potential_savings_aud" in df.columns:
        top = (
            df[[name_col, "potential_savings_aud"]]
            .groupby(name_col, as_index=False).sum()
            .sort_values("potential_savings_aud", ascending=False)
            .head(10)
        )
        lines.append("\n>> Top 10 Savings Opportunities:")
        for i, r in top.reset_index(drop=True).iterrows():
            lines.append(f"  {i+1}. {r[name_col]} — {_fmt(r['potential_savings_aud'])} AUD")
    return lines

# 4) Multi‑Region Cost Centre Performance
def summary_multi_region_performance(df: pd.DataFrame) -> List[str]:
    if df is None or len(df) == 0:
        return [">> Results: 0 regional performance records"]

    lines = [f">> Results: {len(df)} regional performance records"]
    region_col = None
    for c in ["region", "location_state", "state", "cost_centre_region"]:
        if c in df.columns:
            region_col = c
            break

    # Overall aggregates
    if "total_spend_aud" in df.columns:
        lines.append(f">> Total spend (AUD): {_fmt(df['total_spend_aud'].sum())}")
    if "total_orders" in df.columns:
        lines.append(f">> Total orders: {_fmt(df['total_orders'].sum())}")
    if "qty" in df.columns:
        lines.append(f">> Total quantity: {_fmt(df['qty'].sum())}")

    # Regional overview (top by spend if available)
    if region_col:
        metric = "total_spend_aud" if "total_spend_aud" in df.columns else ( "total_orders" if "total_orders" in df.columns else None )
        if metric:
            top = (
                df[[region_col, metric]]
                .groupby(region_col, as_index=False).sum()
                .sort_values(metric, ascending=False)
                .head(15)
            )
            lines.append("\n>> Regional Performance Overview:")
            for i, r in top.reset_index(drop=True).iterrows():
                suffix = "AUD" if metric == "total_spend_aud" else ""
                lines.append(f"  {i+1}. {r[region_col]} — {_fmt(r[metric])} {suffix}".rstrip())
    return lines

# 5) Seasonal Procurement Trends
def summary_seasonal_trends(df: pd.DataFrame) -> List[str]:
    if df is None or len(df) == 0:
        return [">> Results: 0 monthly trend records"]

    lines = [f">> Results: {len(df)} monthly trend records"]

    time_col = None
    for c in ["month", "period", "month_name", "period_name", "year_month"]:
        if c in df.columns:
            time_col = c
            break

    if "total_spend_aud" in df.columns:
        lines.append(f">> Total spend (AUD): {_fmt(df['total_spend_aud'].sum())}")
        # Top months by spend
        if time_col:
            top = (
                df[[time_col, "total_spend_aud"]]
                .groupby(time_col, as_index=False).sum()
                .sort_values("total_spend_aud", ascending=False)
                .head(12)
            )
            lines.append("\n>> Sample Seasonal Trends (top by spend):")
            for i, r in top.reset_index(drop=True).iterrows():
                lines.append(f"  {i+1}. {r[time_col]} — {_fmt(r['total_spend_aud'])} AUD")
    return lines

# 6) Vendor Performance and Relationship Analysis
def summary_vendor_performance(df: pd.DataFrame) -> List[str]:
    if df is None or len(df) == 0:
        return [">> Results: 0 vendor performance records"]

    lines = [f">> Results: {len(df)} vendor performance records"]
    if "total_spend_aud" in df.columns:
        lines.append(f">> Total spend (AUD): {_fmt(df['total_spend_aud'].sum())}")
    if "avg_unit_price" in df.columns:
        lines.append(f">> Avg unit price: {_fmt(df['avg_unit_price'].mean())}")
    if "avg_line_value" in df.columns:
        lines.append(f">> Avg line value: {_fmt(df['avg_line_value'].mean())}")

    name_col = "vendor_name" if "vendor_name" in df.columns else ("vendor_id" if "vendor_id" in df.columns else None)
    if name_col and "total_spend_aud" in df.columns:
        top = (
            df[[name_col, "total_spend_aud"]]
            .groupby(name_col, as_index=False).sum()
            .sort_values("total_spend_aud", ascending=False)
            .head(10)
        )
        lines.append("\n>> Top 10 Vendors by Spend:")
        for i, r in top.reset_index(drop=True).iterrows():
            lines.append(f"  {i+1}. {r[name_col]} — {_fmt(r['total_spend_aud'])} AUD")
    return lines

# 7) Executive Summary Dashboard KPIs
def summary_executive_dashboard(df: pd.DataFrame) -> List[str]:
    """
    Expects a wide table with rows for periods like:
      - 'Current Month', 'Previous Month', 'Year to Date'
    And metrics such as: total_spend_aud, total_orders, active_vendors,
    active_cost_centres, avg_data_quality_score, total_exceptions.
    Works gracefully if only a subset is present.
    """
    if df is None or len(df) == 0:
        return [">> Executive KPI Results: (no rows)"]

    lines = [">> Executive KPI Results:"]
    period_col = "period_name" if "period_name" in df.columns else None
    if not period_col:
        # Single-row summary fallback
        row = df.iloc[0]
        for col in ["total_spend_aud", "total_orders", "active_vendors", "active_cost_centres",
                    "avg_data_quality_score", "total_exceptions"]:
            if col in df.columns:
                label = col.replace("_", " ").title()
                val = row[col]
                if "spend" in col:
                    val = f"{_fmt(val)} AUD"
                elif "score" in col:
                    val = f"{val:.1f}/100"
                else:
                    val = _fmt(val)
                lines.append(f"  • {label}: {val}")
        return lines

    # Structured, with deltas where possible
    # Normalise period names a bit
    dfp = df.copy()
    dfp[period_col] = dfp[period_col].astype(str)

    def pick(name: str):
        s = dfp[dfp[period_col].str.lower().eq(name.lower())]
        return s.iloc[0] if len(s) else None

    current = pick("Current Month")
    if current is None:
        current = pick("Current")

    previous = pick("Previous Month")
    if previous is None:
        previous = pick("Previous")

    ytd = pick("Year to Date")
    if ytd is None:
        ytd = pick("YTD")


    def _line(metric, label, suffix=""):
        vals = []
        cur = current[metric] if (current is not None and metric in current) else None
        prev = previous[metric] if (previous is not None and metric in previous) else None
        if cur is not None:
            v = f"{_fmt(cur)}{suffix}"
            if prev is not None:
                delta = cur - prev
                sign = "▲" if delta > 0 else ("▼" if delta < 0 else "•")
                v += f" ({sign} {_fmt(delta)}{suffix})"
            vals.append(f"   • {label}: {v}")
        if ytd is not None and metric in ytd:
            vals.append(f"   • YTD {label}: {_fmt(ytd[metric])}{suffix}")
        return vals

    # Spend
    if any(col in df.columns for col in ["total_spend_aud"]):
        lines.append("\n>> SPEND")
        lines += _line("total_spend_aud", "Spend", " AUD")

    # Orders
    if "total_orders" in df.columns:
        lines.append("\n>> ORDERS")
        lines += _line("total_orders", "Orders")

    # Vendors
    if "active_vendors" in df.columns:
        lines.append("\n>> VENDORS")
        lines += _line("active_vendors", "Active Vendors")

    # Cost centres
    if "active_cost_centres" in df.columns:
        lines.append("\n>> COST CENTRES")
        lines += _line("active_cost_centres", "Active Cost Centres")

    # Data quality
    if any(col in df.columns for col in ["avg_data_quality_score", "total_exceptions"]):
        lines.append("\n>> DATA QUALITY METRICS")
        if "avg_data_quality_score" in df.columns:
            cur = current["avg_data_quality_score"] if (current is not None and "avg_data_quality_score" in current) else None
            if cur is not None:
                s = f"{cur:.1f}/100"
                if previous is not None and "avg_data_quality_score" in previous:
                    delta = cur - previous["avg_data_quality_score"]
                    sign = "▲" if delta > 0 else ("▼" if delta < 0 else "•")
                    s += f" ({sign} {delta:+.1f})"
                lines.append(f"   • Quality Score: {s}")
            if ytd is not None and "avg_data_quality_score" in ytd:
                lines.append(f"   • YTD Quality Score: {ytd['avg_data_quality_score']:.1f}/100")
        if "total_exceptions" in df.columns:
            cur = current["total_exceptions"] if (current is not None and "total_exceptions" in current) else None
            if cur is not None:
                s = _fmt(cur)
                if previous is not None and "total_exceptions" in previous:
                    delta = cur - previous["total_exceptions"]
                    sign = "▲" if delta > 0 else ("▼" if delta < 0 else "•")
                    s += f" ({sign} {_fmt(delta)})"
                lines.append(f"   • Total Exceptions: {s}")
            if ytd is not None and "total_exceptions" in ytd:
                lines.append(f"   • YTD Exceptions: {_fmt(ytd['total_exceptions'])}")

    lines.append("\n" + "="*80)
    return lines
