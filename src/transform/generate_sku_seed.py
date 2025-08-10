#!/usr/bin/env python3
"""
generate_sku_seed.py — Convert a reviewed curation export into a minimal dbt seed.

Input (approved CSV): columns (case-insensitive) expected:
  - sku_id
  - final_sku_name
  - decision  (APPROVED | AUTO | NEED_APPROVAL)

Output (seed CSV):
  - sku_id, canonical_name, source, effective_from, version

Usage:
  python src/transform/generate_sku_seed.py data/intermediate/curation_exports/sku_name_curation_approved.csv \
      --seed-path dbt/seeds/ref_sku_names.csv \
      [--version v0.3]  # or let it auto-bump
      [--effective-from 2025-08-09]
"""

import argparse
import sys
import re
from datetime import date
from pathlib import Path
import pandas as pd


APPROVED = "APPROVED"
AUTO = "AUTO"
NEED = "NEED_APPROVAL"

SRC_APPROVED = "approved_seed"
SRC_AUTO = "auto_ref"

SEED_COLS = ["sku_id", "canonical_name", "source", "effective_from", "version"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert approved curation export to dbt seed CSV.")
    p.add_argument("approved_csv", help="Path to reviewed/approved CSV (curation export).")
    p.add_argument("--seed-path", default="dbt/seeds/ref_sku_names.csv",
                   help="Output seed CSV path (default: dbt/seeds/ref_sku_names.csv)")
    p.add_argument("--version", default=None,
                   help="Explicit version tag to write (e.g., v0.3). If omitted, auto-bump.")
    p.add_argument("--effective-from", default=None,
                   help="Effective-from date (YYYY-MM-DD). Defaults to today.")
    return p.parse_args()


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Make column access tolerant to case/spacing
    mapper = {c: re.sub(r"\s+", "_", c.strip().lower()) for c in df.columns}
    df = df.rename(columns=mapper)
    return df


def read_existing_seed(seed_path: Path) -> pd.DataFrame:
    if seed_path.exists():
        df = pd.read_csv(seed_path, dtype=str).fillna("")
        missing = [c for c in SEED_COLS if c not in df.columns]
        if missing:
            sys.exit(f"[ERROR] Existing seed missing columns: {missing}")
        return df
    # Empty frame with proper columns
    return pd.DataFrame(columns=SEED_COLS)


def semver_tuple(v: str) -> tuple:
    """
    Parse 'vX' or 'vX.Y' into a sortable tuple (X, Y).
    Unknown -> (0, 0)
    """
    if not v:
        return (0, 0)
    m = re.fullmatch(r"v(\d+)(?:\.(\d+))?", v.strip())
    if not m:
        return (0, 0)
    major = int(m.group(1))
    minor = int(m.group(2) or 0)
    return (major, minor)


def bump_version_auto(existing_seed: pd.DataFrame) -> str:
    if existing_seed.empty:
        return "v0.1"
    # Take the max version present in current seed and bump minor
    versions = sorted({semver_tuple(v) for v in existing_seed["version"].fillna("")}, reverse=True)
    major, minor = versions[0]
    return f"v{major}.{minor + 1}"


def build_new_rows(approved_df: pd.DataFrame, eff_from: str, version: str) -> pd.DataFrame:
    df = normalize_cols(approved_df).copy()

    required = ["sku_id", "final_sku_name", "decision"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        sys.exit(f"[ERROR] Approved CSV missing columns: {missing}")

    # Normalize decisions
    df["decision"] = df["decision"].astype(str).str.strip().str.upper()
    # Keep only APPROVED or AUTO
    df = df[df["decision"].isin([APPROVED, AUTO])].copy()

    if df.empty:
        # No rows to add/update; return empty with correct cols
        return pd.DataFrame(columns=SEED_COLS)

    # Map to minimal seed cols
    df["canonical_name"] = df["final_sku_name"].astype(str).str.strip()
    df["source"] = df["decision"].map({APPROVED: SRC_APPROVED, AUTO: SRC_AUTO})
    df["effective_from"] = eff_from
    df["version"] = version

    # Validate
    if df["sku_id"].isna().any() or (df["sku_id"].astype(str).str.strip() == "").any():
        sys.exit("[ERROR] Found null/empty sku_id in approved CSV.")
    if (df["canonical_name"] == "").any():
        bad = df[df["canonical_name"] == ""]
        sys.exit(f"[ERROR] Empty canonical_name for {len(bad)} rows; fix sku_name in approved CSV.")

    return df[SEED_COLS].copy()


def merge_seed(existing: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Prefer APPROVED over AUTO; if both new and existing present:
      - If existing is APPROVED and new is AUTO: keep existing.
      - Else: take new (updates name/source/effective_from/version).
    """
    if existing.empty:
        return new_rows[SEED_COLS].sort_values("sku_id").reset_index(drop=True)

    # Add a priority flag
    def src_priority(s: pd.Series) -> pd.Series:
        return s.map({SRC_APPROVED: 2, SRC_AUTO: 1}).fillna(0).astype(int)

    existing = existing.copy()
    new_rows = new_rows.copy()

    existing["_prio"] = src_priority(existing["source"])
    new_rows["_prio"] = src_priority(new_rows["source"])

    # Start from existing, update/insert from new where it wins priority
    ex_by_sku = existing.set_index("sku_id")
    for sku, r in new_rows.set_index("sku_id").iterrows():
        if sku not in ex_by_sku.index:
            ex_by_sku.loc[sku] = r
        else:
            r_old = ex_by_sku.loc[sku]
            if int(r["_prio"]) >= int(r_old["_prio"]):
                ex_by_sku.loc[sku] = r
            # else keep existing (approved beats auto)

    merged = ex_by_sku.reset_index()
    merged = merged.drop(columns=[c for c in merged.columns if c.startswith("_")], errors="ignore")
    # Keep exact column order
    return merged[SEED_COLS].sort_values("sku_id").reset_index(drop=True)


def main():
    args = parse_args()
    approved_csv = Path(args.approved_csv)
    seed_path = Path(args.seed_path)
    seed_path.parent.mkdir(parents=True, exist_ok=True)

    eff_from = args.effective_from or date.today().isoformat()

    # Load existing seed (if any)
    existing_seed = read_existing_seed(seed_path)

    # Decide version
    version = args.version or bump_version_auto(existing_seed)

    # Read approved/auto export
    try:
        approved_df = pd.read_csv(approved_csv, dtype=str).fillna("")
    except Exception as e:
        sys.exit(f"[ERROR] Failed to read approved CSV: {e}")

    new_rows = build_new_rows(approved_df, eff_from, version)

    # Merge with existing seed
    final_seed = merge_seed(existing_seed, new_rows)

    # Final sanity: uniqueness & non-nulls
    if final_seed["sku_id"].duplicated().any():
        dups = final_seed[final_seed["sku_id"].duplicated(keep=False)]["sku_id"].tolist()
        sys.exit(f"[ERROR] Duplicate sku_id in final seed: {sorted(set(dups))}")
    if (final_seed["canonical_name"].astype(str).str.strip() == "").any():
        sys.exit("[ERROR] Empty canonical_name in final seed after merge.")

    final_seed.to_csv(seed_path, index=False)
    print(f"[OK] Wrote seed: {seed_path}  rows={len(final_seed)}  version={version}")
    print("Next steps:")
    print(f"  dbt seed")
    print(f"  dbt run --select models/silver")

if __name__ == "__main__":
    main()
